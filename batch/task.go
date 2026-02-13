package batch

import (
	"context"

	"github.com/contribsys/faktory/util"
)

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
	redis := s.Manager().Redis()

	bids, err := redis.SMembers(ctx, batchCommittedSetKey()).Result()
	if err != nil {
		util.Warnf("Batch sweep: failed to read committed set: %v", err)
		return nil
	}

	for _, bid := range bids {
		t.batchesChecked++
		t.subsystem.checkAndFireCallbacks(ctx, s, bid)
	}

	return nil
}

// Stats returns statistics about the task
func (t *batchSweepTask) Stats(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"sweeps":          t.sweeps,
		"batches_checked": t.batchesChecked,
	}
}
