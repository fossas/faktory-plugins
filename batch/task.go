package batch

import (
	"context"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
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

// staleBatch is a batch the sweep found stale, with the committed_at it read.
type staleBatch struct {
	bid         string
	committedAt string
}

// reapIfStale deletes a batch, and its children, once every batch in the tree
// was committed more than CommittedBatchTTL ago. Batches committed before
// committed_at was recorded fall back to created_at. A child is left for its
// parent's reap. Each batch is deleted only if it is still committed with the
// commit time read here, so a batch reopened or recommitted in the meantime
// stays. Returns true if the batch was reaped.
func (t *batchSweepTask) reapIfStale(ctx context.Context, bid string, now time.Time) bool {
	s := t.subsystem.Server
	parentBid, err := s.Manager().Redis().HGet(ctx, batchMetaKey(bid), "parent_bid").Result()
	if err != nil && err != redis.Nil {
		util.Warnf("Batch sweep: failed to read batch %s: %v", bid, err)
		return false
	}
	if parentBid != "" {
		parentExists, err := batchExists(ctx, s, parentBid)
		if err != nil || parentExists {
			return false
		}
	}

	tree, ok := t.staleTree(ctx, bid, now)
	if !ok {
		return false
	}

	status, err := getBatchStatus(ctx, s, bid)
	if err == nil {
		util.Warnf("Batch sweep: reaping batch %s committed at %s (total=%d pending=%d failed=%d complete_st=%q success_st=%q)",
			bid, tree[0].committedAt, status.Total, status.Pending, status.Failed, status.CompleteState, status.SuccessState)
	}

	// The root goes first: if it changed since it was read, the tree stays.
	// A child that changed stays too, and is reaped on its own once stale,
	// since its parent is gone.
	for i, batch := range tree {
		reaped, err := reapBatch(ctx, s, batch.bid, batch.committedAt)
		if err != nil {
			util.Warnf("Batch sweep: %v", err)
		}
		if i == 0 && !reaped {
			return false
		}
	}
	return true
}

// staleTree returns bid and its descendants, root first, if every one of them
// is committed and stale. A child whose keys are gone is skipped. It returns
// false if any batch in the tree is uncommitted or fresh, or cannot be read.
func (t *batchSweepTask) staleTree(ctx context.Context, bid string, now time.Time) ([]staleBatch, bool) {
	s := t.subsystem.Server
	rds := s.Manager().Redis()

	committed, err := rds.SIsMember(ctx, batchCommittedSetKey(), bid).Result()
	if err != nil {
		util.Warnf("Batch sweep: failed to check whether batch %s is committed: %v", bid, err)
		return nil, false
	}
	if !committed {
		return nil, false
	}

	fields, err := rds.HMGet(ctx, batchMetaKey(bid), "committed_at", "created_at").Result()
	if err != nil {
		util.Warnf("Batch sweep: failed to read batch %s: %v", bid, err)
		return nil, false
	}
	committedAt, _ := fields[0].(string)
	commitTime := committedAt
	if commitTime == "" {
		commitTime, _ = fields[1].(string)
	}
	if commitTime == "" {
		return nil, false
	}
	parsed, err := util.ParseTime(commitTime)
	if err != nil {
		util.Warnf("Batch sweep: invalid commit time %q for batch %s: %v", commitTime, bid, err)
		return nil, false
	}
	if now.Sub(parsed) < CommittedBatchTTL {
		return nil, false
	}

	tree := []staleBatch{{bid: bid, committedAt: committedAt}}
	children, err := getChildBatches(ctx, s, bid)
	if err != nil {
		util.Warnf("Batch sweep: failed to read children of batch %s: %v", bid, err)
		return nil, false
	}
	for _, child := range children {
		exists, err := batchExists(ctx, s, child)
		if err != nil {
			return nil, false
		}
		if !exists {
			continue
		}
		subtree, ok := t.staleTree(ctx, child, now)
		if !ok {
			return nil, false
		}
		tree = append(tree, subtree...)
	}
	return tree, true
}

// Stats returns statistics about the task
func (t *batchSweepTask) Stats(ctx context.Context) map[string]any {
	return map[string]any{
		"sweeps":          t.sweeps,
		"batches_checked": t.batchesChecked,
		"batches_reaped":  t.batchesReaped,
	}
}
