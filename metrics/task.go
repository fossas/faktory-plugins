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
	go func() {
		connectionCount := m.Subsystem.Server.Stats.Connections
		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("connections.count"), float64(connectionCount), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		workingCount := m.Subsystem.Server.Store().Working().Size()
		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("working.count"), float64(workingCount), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		scheduledCount := m.Subsystem.Server.Store().Scheduled().Size()
		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("scheduled.count"), float64(scheduledCount), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		retriesCount := m.Subsystem.Server.Store().Retries().Size()
		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("retries.count"), float64(retriesCount), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		deadCount := m.Subsystem.Server.Store().Dead().Size()
		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("dead.count"), float64(deadCount), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		var totalEnqueued uint64 = 0

		m.Subsystem.Server.Store().EachQueue(func(queue storage.Queue) {
			count := queue.Size()
			totalEnqueued += count
			queueCountMetricName := m.Subsystem.PrefixMetricName(fmt.Sprintf("enqueued.%s.count", queue.Name()))
			if err := m.Subsystem.StatsDClient().Gauge(queueCountMetricName, float64(count), m.Subsystem.Options.Tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}

			queueLatencyMetricName := m.Subsystem.PrefixMetricName(fmt.Sprintf("enqueued.%s.time", queue.Name()))
			var timeElapsed time.Duration = 0
			// This does an LRANGE on the queue
			// start is the offset from the left of the queue
			// count is not the number of items to fetch, but rather the offset to the last item to return
			// Jobs are LPUSH'd into the queue and RPOP'd out, so to get the last job we want -1, -1
			queue.Page(-1, 0, func(_ int, data []byte) error {
				var job client.Job
				if err := json.Unmarshal(data, &job); err != nil {
					util.Warnf("metrics task unable to unmarshal job data: %v", err)
					return nil
				}

				t, err := util.ParseTime(job.EnqueuedAt)
				if err != nil {
					util.Warnf("metrics task unable to parse EnqueuedAt: %v", err)
					return nil
				}
				timeElapsed = time.Duration(time.Now().Sub(t))

				return nil
			})
			if err := m.Subsystem.StatsDClient().Timing(queueLatencyMetricName, timeElapsed, m.Subsystem.Options.Tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}

			util.Debugf("metrics: %s: %d", queueCountMetricName, count)
			util.Debugf("metrics: %s: %d", queueLatencyMetricName, timeElapsed)
		})

		if err := m.Subsystem.StatsDClient().Gauge(m.Subsystem.PrefixMetricName("enqueued.count"), float64(totalEnqueued), m.Subsystem.Options.Tags, 1); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}
		util.Debugf("metrics: enqueued.count: %d", totalEnqueued)
	}()
	return nil
}

// Stats - this is required but there are no useful stats to record for metrics
// so return an empty map
func (s *metricsTask) Stats() map[string]interface{} {
	// no stats
	return map[string]interface{}{}
}
