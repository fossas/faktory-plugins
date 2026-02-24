package deadjobs

import (
	"context"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Taskable = &deadJobCleanupTask{}

// deadJobCleanupTask is a periodic task that removes dead jobs older than
// the configured retention period when the dead set exceeds the threshold.
type deadJobCleanupTask struct {
	subsystem    *DeadJobCleanupSubsystem
	sweeps       int64
	totalRemoved int64
}

// Name returns the task name.
func (t *deadJobCleanupTask) Name() string {
	return "Dead job cleanup"
}

// Execute runs the cleanup task.
func (t *deadJobCleanupTask) Execute(ctx context.Context) error {
	t.sweeps++
	opts := t.subsystem.Options

	deadSet := t.subsystem.Server.Store().Dead()
	currentSize := deadSet.Size(ctx)

	if currentSize <= uint64(opts.Threshold) {
		util.Debugf("Dead job cleanup: %s dead jobs, below threshold of %d. Skipping.",
			formatCount(currentSize), opts.Threshold)
		return nil
	}

	cutoff := time.Now().Add(-time.Duration(opts.RetentionDays) * 24 * time.Hour)
	cutoffStr := util.Thens(cutoff)

	removed, err := deadSet.RemoveBefore(ctx, cutoffStr, int64(opts.BatchSize), func(data []byte) error {
		return nil
	})
	if err != nil {
		util.Warnf("Dead job cleanup: error removing jobs: %v", err)
		return nil
	}

	t.totalRemoved += removed
	newSize := deadSet.Size(ctx)
	util.Infof("Dead job cleanup: removed %d dead jobs older than %d days (%s remaining)",
		removed, opts.RetentionDays, formatCount(newSize))

	return nil
}

// Stats returns statistics about the cleanup task.
func (t *deadJobCleanupTask) Stats(ctx context.Context) map[string]any {
	return map[string]any{
		"sweeps":        t.sweeps,
		"total_removed": t.totalRemoved,
	}
}
