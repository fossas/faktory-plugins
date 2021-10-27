package metrics

import (
	"fmt"
	"os"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Subsystem = &MetricsSubsystem{}

// Metrics Subsystem uses dogstatsd to submit metrics for queues
// middleware is used to send metrics for suceeded/failed job count and time to complete
// a task is used to track the count of each queue and how long the next job has been waiting
type MetricsSubsystem struct {
	statsDClient statsd.ClientInterface
	Server       *server.Server
	Options      *Options
}

// Options for the plugin
type Options struct {
	// Enabled controls whether or not the plugin will function
	Enabled bool
	// Tags sent to datdog
	Tags []string
	// Prefix for metrics
	Namespace string
}

// Start starts the subsystem
// get options from global config "[metrics]"
//
func (m *MetricsSubsystem) Start(s *server.Server) error {
	if _, ok := s.Options.GlobalConfig["metrics"]; !ok {
		return fmt.Errorf("No [metrics] configuration found, plugin cannot start")
	}

	m.Server = s
	m.Options = m.getOptions(s)

	if !m.Options.Enabled {
		return nil
	}

	if err := m.createStatsDClient(); err != nil {
		util.Warnf("Unable to create statsD client: %v", err)
		return fmt.Errorf("Unable to create statsD client")
	}
	m.Server.AddTask(10, &metricsTask{m})

	m.addMiddleware()
	util.Info("Started statsd metrics")
	return nil
}

// Name - returns the name of the subsystem or plugin
func (m *MetricsSubsystem) Name() string {
	return "metrics"
}

// Reload - the config is reloaded by faktory
// update the plugin and re-create dogstatsd client if needed
func (m *MetricsSubsystem) Reload(s *server.Server) error {
	return nil
}

// Shutdown - nothing needs to be done but the function must exist for subsystems
func (m *MetricsSubsystem) Shutdown(s *server.Server) error {
	if err := m.StatsDClient().Close(); err != nil {
		return fmt.Errorf("Unable to close statsd client: %v", err)
	}

	return nil
}

// StatsDClient returns the statsd client
func (m *MetricsSubsystem) StatsDClient() statsd.ClientInterface {
	return m.statsDClient
}

// PrefixMetricName - adds configured namespace to the metric

func (m *MetricsSubsystem) PrefixMetricName(metricName string) string {
	return fmt.Sprintf("%s.%s", m.Options.Namespace, metricName)
}

func (m *MetricsSubsystem) getOptions(s *server.Server) *Options {
	enabledValue := s.Options.Config("metrics", "enabled", false)
	enabled, ok := enabledValue.(bool)
	if !ok {
		enabled = false
	}

	tags := []string{}
	tagsValue := s.Options.Config("metrics", "tags", []string{})
	if tagsInterface, ok := tagsValue.([]interface{}); ok {
		for _, tag := range tagsInterface {
			if tagValue, ok := tag.(string); ok {
				tags = append(tags, tagValue)
			}

		}
	}

	namespace := s.Options.String("metrics", "namespace", "jobs")
	return &Options{
		Enabled:   enabled,
		Tags:      tags,
		Namespace: namespace,
	}
}

func (m *MetricsSubsystem) createStatsDClient() error {
	tags := m.Options.Tags

	// dogstatsd doesn't parse DD_TAGS so do it here
	if value := os.Getenv("DD_TAGS"); value != "" {
		for _, tag := range strings.Split(value, "") {
			tags = append(tags, strings.TrimSpace(tag))
		}
	}

	// passing "" uses env
	statsDClient, err := statsd.New("", statsd.WithTags(tags))
	if err != nil {
		return fmt.Errorf("unable to create statsd client: %v", err)
	}
	m.statsDClient = statsDClient
	return nil
}

func (m *MetricsSubsystem) getTagsFromJob(ctx manager.Context) []string {
	jobType := fmt.Sprintf("jobtype:%s", ctx.Job().Type)
	queueName := fmt.Sprintf("queue:%s", ctx.Job().Queue)
	return []string{
		jobType,
		queueName,
	}
}
