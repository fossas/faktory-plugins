package batch

import (
	"context"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Taskable = &batchSweepTask{}

// batchSweepTask is a background task that periodically re-checks committed
// batches and fires any callbacks that should have fired but didn't (e.g. due
// to a transient Redis error or crash during processing).
type batchSweepTask struct {
	subsystem      *BatchSubsystem
	sweeps         int64
	batchesChecked int64
}

// Name returns the name of the task
func (t *batchSweepTask) Name() string {
	return "Batch callback sweep"
}

// Execute runs the sweep task
func (t *batchSweepTask) Execute(ctx context.Context) error {
	t.sweeps++

	s := t.subsystem.Server
	rds := s.Manager().Redis()

	iter := rds.SScan(ctx, batchCommittedSetKey(), 0, "", 100).Iterator()
	for iter.Next(ctx) {
		bid := iter.Val()
		t.batchesChecked++
		t.subsystem.checkAndFireCallbacks(ctx, bid)
	}
	if err := iter.Err(); err != nil {
		util.Warnf("Batch sweep: failed to scan committed set: %v", err)
	}

	return nil
}

// Stats returns statistics about the task
func (t *batchSweepTask) Stats(ctx context.Context) map[string]any {
	return map[string]any{
		"sweeps":          t.sweeps,
		"batches_checked": t.batchesChecked,
	}
}
