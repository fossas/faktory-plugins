package deadjobs

import (
	"fmt"
	"sync/atomic"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Subsystem = &DeadJobCleanupSubsystem{}

// DeadJobCleanupSubsystem periodically removes old dead jobs from the dead set.
// Faktory's built-in dead job TTL is hardcoded at 180 days which can cause the
// dead set to grow unboundedly. This plugin allows configuring a shorter retention
// period and only runs cleanup when the dead set exceeds a configurable threshold.
type DeadJobCleanupSubsystem struct {
	Server  *server.Server
	options atomic.Pointer[Options]
}

// loadOptions safely reads the current options.
func (d *DeadJobCleanupSubsystem) loadOptions() *Options {
	return d.options.Load()
}

// storeOptions safely writes new options.
func (d *DeadJobCleanupSubsystem) storeOptions(opts *Options) {
	d.options.Store(opts)
}

// Options for the dead job cleanup plugin.
type Options struct {
	// Enabled controls whether the plugin will function.
	Enabled bool
	// RetentionDays is how many days to keep dead jobs (default: 7).
	RetentionDays int64
	// Threshold is the minimum dead job count before cleanup runs (default: 10000).
	Threshold int64
	// BatchSize is the max number of jobs to remove per sweep iteration (default: 1000).
	BatchSize int64
	// IntervalSeconds is how often the cleanup task runs (default: 3600 = 1 hour).
	IntervalSeconds int64
}

// Start initializes the subsystem and registers the periodic cleanup task.
func (d *DeadJobCleanupSubsystem) Start(s *server.Server) error {
	d.Server = s
	opts := d.parseOptions()
	d.storeOptions(opts)

	if !opts.Enabled {
		return nil
	}

	s.AddTask(opts.IntervalSeconds, &deadJobCleanupTask{subsystem: d})
	util.Infof("Started dead job cleanup plugin (retention=%dd, threshold=%d, batch_size=%d, interval=%ds)",
		opts.RetentionDays, opts.Threshold, opts.BatchSize, opts.IntervalSeconds)
	return nil
}

// Name returns the name of the subsystem.
func (d *DeadJobCleanupSubsystem) Name() string {
	return "dead_job_cleanup"
}

// Reload reloads configuration. Hot reload updates options atomically.
func (d *DeadJobCleanupSubsystem) Reload(s *server.Server) error {
	d.storeOptions(d.parseOptions())
	return nil
}

// Shutdown gracefully shuts down the subsystem.
func (d *DeadJobCleanupSubsystem) Shutdown(s *server.Server) error {
	return nil
}

func (d *DeadJobCleanupSubsystem) parseOptions() *Options {
	opts := &Options{
		RetentionDays:   7,
		Threshold:       10000,
		BatchSize:       1000,
		IntervalSeconds: 3600,
	}

	enabledValue := d.Server.Options.Config("dead_job_cleanup", "enabled", false)
	if enabled, ok := enabledValue.(bool); ok {
		opts.Enabled = enabled
	}

	if v := d.Server.Options.Config("dead_job_cleanup", "retention_days", int64(7)); v != nil {
		if val, ok := v.(int64); ok && val > 0 {
			opts.RetentionDays = val
		}
	}

	if v := d.Server.Options.Config("dead_job_cleanup", "threshold", int64(10000)); v != nil {
		if val, ok := v.(int64); ok && val >= 0 {
			opts.Threshold = val
		}
	}

	if v := d.Server.Options.Config("dead_job_cleanup", "batch_size", int64(1000)); v != nil {
		if val, ok := v.(int64); ok && val > 0 {
			opts.BatchSize = val
		}
	}

	if v := d.Server.Options.Config("dead_job_cleanup", "interval_seconds", int64(3600)); v != nil {
		if val, ok := v.(int64); ok && val > 0 {
			opts.IntervalSeconds = val
		}
	}

	return opts
}

// formatCount returns a human-readable count string.
func formatCount(n uint64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
