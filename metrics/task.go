package metrics

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

var _ server.Taskable = &metricsTask{}

type metricsTask struct {
	Subsystem *MetricsSubsystem
}

// Name - name of the task
func (m *metricsTask) Name() string {
	return "Metrics"
}

// Execute - runs the task to collect metrics
func (m *metricsTask) Execute() error {
	workingCount := m.Subsystem.Server.Store().Working().Size()
	if err := m.Subsystem.StatsDClient().Count(m.Subsystem.PrefixMetricName("working.count"), int64(workingCount), m.Subsystem.Options.Tags, 1); err != nil {
		util.Warnf("unable to submit metric: %v", err)
	}

	scheduledCount := m.Subsystem.Server.Store().Scheduled().Size()
	if err := m.Subsystem.StatsDClient().Count(m.Subsystem.PrefixMetricName("scheduled.count"), int64(scheduledCount), m.Subsystem.Options.Tags, 1); err != nil {
		util.Warnf("unable to submit metric: %v", err)
	}

	retriesCount := m.Subsystem.Server.Store().Retries().Size()
	if err := m.Subsystem.StatsDClient().Count(m.Subsystem.PrefixMetricName("retries.count"), int64(retriesCount), m.Subsystem.Options.Tags, 1); err != nil {
		util.Warnf("unable to submit metric: %v", err)
	}

	deadCount := m.Subsystem.Server.Store().Dead().Size()
	if err := m.Subsystem.StatsDClient().Count(m.Subsystem.PrefixMetricName("dead.count"), int64(deadCount), m.Subsystem.Options.Tags, 1); err != nil {
		util.Warnf("unable to submit metric: %v", err)
	}

	var totalEnqueued uint64 = 0

	m.Subsystem.Server.Store().EachQueue(func(queue storage.Queue) {
		count := queue.Size()
		totalEnqueued += count
		metricName := m.Subsystem.PrefixMetricName(fmt.Sprintf("enqueued.%s.count", queue.Name()))
		if err := m.Subsystem.StatsDClient().Count(metricName, int64(count), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}
		// count=0 will pull exactly 1 job
		queue.Page(0, 0, func(index int, e []byte) error {
			var job client.Job
			if err := json.Unmarshal(e, &job); err != nil {
				util.Warnf("metrics task unable to unmarshal job data: %v", err)
				return nil
			}
			t, err := util.ParseTime(job.EnqueuedAt)
			if err != nil {
				util.Warnf("metrics task unable to parse EnqueuedAt: %v", err)
				return nil
			}
			metricName := m.Subsystem.PrefixMetricName(fmt.Sprintf("enqueued.%s.time", job.Queue))
			timeElapsed := time.Duration(time.Now().Sub(t)).Milliseconds()
			if err := m.Subsystem.StatsDClient().Gauge(metricName, float64(timeElapsed), m.Subsystem.Options.Tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
			return nil
		})
		util.Debugf("metrics: enqueued.%s.count: %d", queue.Name(), count)
	})

	if err := m.Subsystem.StatsDClient().Count(m.Subsystem.PrefixMetricName("enqueued.count"), int64(totalEnqueued), m.Subsystem.Options.Tags, 1); err != nil {
		util.Warnf("unable to submit metric: %v", err)
	}
	util.Debugf("metrics: enqueued.count: %d", totalEnqueued)

	return nil
}

// Stats - this is required but there are no useful stats to record for metrics
// so return an empty map
func (s *metricsTask) Stats() map[string]interface{} {
	// no stats
	return map[string]interface{}{}
}
