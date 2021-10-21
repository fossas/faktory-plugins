package metrics

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type MetricsSubsystem struct {
	statsdClient statsd.ClientInterface
	Server       *server.Server
	Options      *options
}

type options struct {
	statsdServer string
	tags         []string
}

// starts the subsystem
// connects to statsd server
func (m *MetricsSubsystem) Start(s *server.Server) error {
	m.Server = s
	m.Options = m.getOptions(s)
	if err := m.connectStatsd(); err != nil {
		util.Warnf("Unable to connect to statsd: %v", err)
		return nil
	}
	m.Server.AddTask(10, &metrics{m.Server.Store(), m.Client, 1, []string{}})

	m.addMiddleware()
	return nil
}

// returns the name of the subsystem or plugin
func (m *MetricsSubsystem) Name() string {
	return "metrics"
}

// reload, config may of changed so re-create statsd client
func (m *MetricsSubsystem) Reload(s *server.Server) error {
	m.Server = s
	opts := m.getOptions(s)

	if m.Options.statsdServer != opts.statsdServer {
		util.Warnf("Statsd configuration changed")
		m.Options.statsdServer = opts.statsdServer
		if m.statsdClient != nil {
			m.statsdClient.Close()
		}
		return m.connectStatsd()
	}
	return nil
}

// shutdown - nothing needs to be done but the function must exist for subsystems
func (m *MetricsSubsystem) Shutdown(s *server.Server) error {
	m.statsdClient.Close()
	return nil
}

// returns the statsd client
func (m *MetricsSubsystem) Client() statsd.ClientInterface {
	return m.statsdClient
}

func (m *MetricsSubsystem) getOptions(s *server.Server) *options {
	return &options{
		statsdServer: s.Options.String("metrics", "statsd_server", ""),
	}

}
func (m *MetricsSubsystem) connectStatsd() error {
	if m.Options.statsdServer == "" {
		return fmt.Errorf("statsd server not configured")

	}
	client, err := statsd.New(m.Options.statsdServer)
	if err != nil {
		return fmt.Errorf("unable to create statsd client: %v", err)
	}
	m.statsdClient = client
	return nil
}

func (m *MetricsSubsystem) getTagsFromJob(ctx manager.Context) []string {
	jid := fmt.Sprintf("jid:%s", ctx.Job().Jid)
	jobType := fmt.Sprintf("jobtype:%s", ctx.Job().Type)
	return []string{
		jid,
		jobType,
	}
}

func (m *MetricsSubsystem) addMiddleware() {
	m.Server.Manager().AddMiddleware("ack", func(next func() error, ctx manager.Context) error {
		tags := m.getTagsFromJob(ctx)

		if ctx.Reservation() != nil {
			m.statsdClient.Timing("jobs.succeeded.time", time.Duration(time.Until(ctx.Reservation().ReservedAt())), tags, 1)
		}

		m.statsdClient.Incr("jobs.succeeded.count", tags, 1)

		return next()
	})
	m.Server.Manager().AddMiddleware("fail", func(next func() error, ctx manager.Context) error {
		tags := m.getTagsFromJob(ctx)

		if ctx.Reservation() != nil {
			m.statsdClient.Timing("jobs.failed.time", time.Duration(time.Until(ctx.Reservation().ReservedAt())), tags, 1)
		}

		if ctx.Job().Failure.RetryCount >= ctx.Job().Retry {
			// only count a job as failed on the last retry
			m.statsdClient.Incr("jobs.failed.count", tags, 1)
		} else if ctx.Job().Retry > 0 && ctx.Job().Failure.RetryCount == 0 {
			// only count first retry
			m.statsdClient.Incr("jobs.retried_at_least_once.count", tags, 1)
		}
		return next()
	})
}

type metricsTask func(time.Time) (int64, error)

type metrics struct {
	store  storage.Store
	Client func() statsd.ClientInterface
	rate   float64
	tags   []string
}

func (m *metrics) Name() string {
	return "Metrics"
}

func (m *metrics) Execute() error {
	// track count for each queue
	m.store.EachQueue(func(queue storage.Queue) {
		count := queue.Size()
		metricName := fmt.Sprintf("jobs.%s.queued.count", queue.Name())
		m.Client().Count(metricName, int64(count), m.tags, m.rate)

		queue.Page(0, 1, func(index int, e []byte) error {
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
			metricName := fmt.Sprintf("jobs.%s.queued.time", job.Queue)
			timeElapsed := time.Duration(time.Until(t)).Seconds()
			m.Client().Gauge(metricName, timeElapsed, m.tags, m.rate)
			return nil
		})

	})

	return nil
}

func (s *metrics) Stats() map[string]interface{} {
	// no stats
	return map[string]interface{}{}
}
