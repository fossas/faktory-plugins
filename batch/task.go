package batch

import (
	"context"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Taskable = &batchSweepTask{}

// batchSweepTask is a background task that periodically re-checks committed
// batches and fires any callbacks that should have fired but didn't (e.g. due
// to a transient Redis error or crash during processing). It also reaps
// batches that have been committed for longer than CommittedBatchTTL.
type batchSweepTask struct {
	subsystem      *BatchSubsystem
	sweeps         int64
	batchesChecked int64
	batchesReaped  int64
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

	now := time.Now()
	iter := rds.SScan(ctx, batchCommittedSetKey(), 0, "", 100).Iterator()
	for iter.Next(ctx) {
		bid := iter.Val()
		t.batchesChecked++
		if t.reapIfStale(ctx, bid, now) {
			t.batchesReaped++
			continue
		}
		t.subsystem.checkAndFireCallbacks(ctx, bid)
	}
	if err := iter.Err(); err != nil {
		util.Warnf("Batch sweep: failed to scan committed set: %v", err)
	}

	return nil
}

// reapIfStale deletes a batch, and its children, if it was committed more than
// CommittedBatchTTL ago. Batches committed before committed_at was recorded
// fall back to created_at. A child is left for its parent's reap, which
// deletes the whole tree. Returns true if the batch was reaped.
func (t *batchSweepTask) reapIfStale(ctx context.Context, bid string, now time.Time) bool {
	rds := t.subsystem.Server.Manager().Redis()
	fields, err := rds.HMGet(ctx, batchMetaKey(bid), "committed_at", "created_at", "parent_bid").Result()
	if err != nil {
		util.Warnf("Batch sweep: failed to read batch %s: %v", bid, err)
		return false
	}
	committedAt, _ := fields[0].(string)
	if committedAt == "" {
		committedAt, _ = fields[1].(string)
	}
	if committedAt == "" {
		return false
	}
	committed, err := util.ParseTime(committedAt)
	if err != nil {
		util.Warnf("Batch sweep: invalid commit time %q for batch %s: %v", committedAt, bid, err)
		return false
	}
	if now.Sub(committed) < CommittedBatchTTL {
		return false
	}

	if parentBid, _ := fields[2].(string); parentBid != "" {
		parentExists, err := batchExists(ctx, t.subsystem.Server, parentBid)
		if err != nil || parentExists {
			return false
		}
	}

	status, err := getBatchStatus(ctx, t.subsystem.Server, bid)
	if err == nil {
		util.Warnf("Batch sweep: reaping batch %s committed at %s (total=%d pending=%d failed=%d complete_st=%q success_st=%q)",
			bid, committedAt, status.Total, status.Pending, status.Failed, status.CompleteState, status.SuccessState)
	}
	t.subsystem.deleteBatchTree(ctx, bid)
	return true
}

// Stats returns statistics about the task
func (t *batchSweepTask) Stats(ctx context.Context) map[string]any {
	return map[string]any{
		"sweeps":          t.sweeps,
		"batches_checked": t.batchesChecked,
		"batches_reaped":  t.batchesReaped,
	}
}
