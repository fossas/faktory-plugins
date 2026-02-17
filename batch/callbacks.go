package batch

import (
	"context"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// checkAndFireCallbacks evaluates whether callbacks should fire for a batch
// and enqueues them if conditions are met
func (b *BatchSubsystem) checkAndFireCallbacks(ctx context.Context, s *server.Server, bid string) {
	status, err := getBatchStatus(ctx, s, bid)
	if err != nil {
		util.Warnf("batch callbacks: error getting status for %s: %v", bid, err)
		return
	}

	// Batch must be committed
	committed, err := isCommitted(ctx, s, bid)
	if err != nil {
		util.Warnf("batch callbacks: error checking committed state for %s: %v", bid, err)
		return
	}
	if !committed {
		return
	}

	// Check complete callback
	// Fires when: pending == 0 AND all children's complete callbacks finished
	if status.Pending == 0 && status.CompleteState == CallbackPending {
		childrenOk := allChildrenCallbackFinished(ctx, s, bid, "complete")
		if childrenOk {
			b.fireCallback(ctx, s, bid, "complete")
			// Refresh status after firing complete
			status, err = getBatchStatus(ctx, s, bid)
			if err != nil {
				util.Warnf("batch callbacks: error refreshing status for %s: %v", bid, err)
				return
			}
		}
	}

	// Check success callback
	// Fires when: pending == 0 AND failed == 0 AND no child failures AND complete callback enqueued (or not defined) AND all children's success callbacks finished
	if status.Pending == 0 && status.Failed == 0 && status.SuccessState == CallbackPending {
		// Complete must be enqueued (or not defined)
		completeOk := status.CompleteState == CallbackEnqueued || status.CompleteState == CallbackFinished || !hasCompleteCallback(ctx, s, bid)
		childrenOk := allChildrenCallbackFinished(ctx, s, bid, "success")
		childFailures, childErr := anyChildHasFailures(ctx, s, bid)
		noChildFailures := childErr == nil && !childFailures
		if completeOk && childrenOk && noChildFailures {
			b.fireCallback(ctx, s, bid, "success")
		}
	}
}

// fireCallback enqueues a callback job for the given batch
func (b *BatchSubsystem) fireCallback(ctx context.Context, s *server.Server, bid string, callbackType string) {
	rds := s.Manager().Redis()

	// Determine which state key to use
	var stateKey string
	if callbackType == "complete" {
		stateKey = batchCompleteStateKey(bid)
	} else {
		stateKey = batchSuccessStateKey(bid)
	}

	// Use a lock key to prevent double-firing
	lockKey := stateKey + ":lock"

	// Try to acquire lock atomically using SetNX with TTL to prevent permanent stuck
	// state if the process crashes while holding the lock. The state-check at line 85-88
	// provides idempotency, so a retry after lock expiry is safe.
	acquired, err := rds.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil {
		util.Warnf("batch callbacks: failed to acquire lock for %s callback on batch %s: %v", callbackType, bid, err)
		return
	}
	if !acquired {
		// Another goroutine is handling this callback
		return
	}

	// Check current state
	currentState, err := rds.Get(ctx, stateKey).Result()
	if err != nil && err != redis.Nil {
		util.Warnf("batch callbacks: failed to read %s callback state for batch %s: %v", callbackType, bid, err)
		rds.Del(ctx, lockKey)
		return
	}
	if currentState != CallbackPending {
		// Already processed, release lock
		rds.Del(ctx, lockKey)
		return
	}

	// Get the callback job definition
	batch, err := getBatch(ctx, s, bid)
	if err != nil {
		util.Warnf("batch callbacks: failed to get batch %s for %s callback: %v", bid, callbackType, err)
		// Reset state and release lock to allow retry
		rds.Del(ctx, lockKey)
		return
	}

	var callbackJob *client.Job
	if callbackType == "complete" {
		callbackJob = batch.Complete
	} else {
		callbackJob = batch.Success
	}

	if callbackJob == nil {
		// No callback defined, mark as finished
		util.Debugf("batch %s has no %s callback defined, marking as finished", bid, callbackType)
		rds.Set(ctx, stateKey, CallbackFinished, 0)
		// Check if this triggers the next callback or cleanup
		b.checkPostCallback(ctx, s, bid, callbackType)
		return
	}

	// Create a copy of the callback job with required fields
	job := &client.Job{
		Jid:       uuid.NewString(),
		Type:      callbackJob.Type,
		Args:      callbackJob.Args,
		Queue:     callbackJob.Queue,
		Retry:     callbackJob.Retry,
		CreatedAt: util.Nows(),
	}

	// Ensure Args is set (Faktory requires it)
	if job.Args == nil {
		job.Args = []interface{}{}
	}

	// Copy custom fields and add batch callback metadata
	if callbackJob.Custom != nil {
		job.Custom = make(map[string]interface{})
		for k, v := range callbackJob.Custom {
			job.Custom[k] = v
		}
	} else {
		job.Custom = make(map[string]interface{})
	}
	job.Custom["_bid"] = bid
	job.Custom["_cb"] = callbackType

	// Set default queue if not specified
	if job.Queue == "" {
		job.Queue = "default"
	}

	// Push the callback job first, then mark as enqueued.
	// This ordering prevents a stuck state if the process crashes: if we set state
	// to Enqueued first and crash before Push, the callback would never be delivered.
	// The lock prevents double-push during the window where state is still Pending.
	err = s.Manager().Push(ctx, job)
	if err != nil {
		util.Warnf("batch callbacks: failed to enqueue %s callback for batch %s: %v", callbackType, bid, err)
		// Release lock to allow retry
		rds.Del(ctx, lockKey)
		return
	}

	// Mark as enqueued after successful push
	rds.Set(ctx, stateKey, CallbackEnqueued, 0)

	// Release lock
	rds.Del(ctx, lockKey)

	util.Infof("Enqueued %s callback for batch %s (job %s)", callbackType, bid, job.Jid)
}

// handleCallbackComplete is called when a callback job finishes (ACK'd)
func (b *BatchSubsystem) handleCallbackComplete(ctx context.Context, bid string, callbackType string) {
	rds := b.Server.Manager().Redis()

	// Update callback state to finished
	var stateKey string
	if callbackType == "complete" {
		stateKey = batchCompleteStateKey(bid)
	} else {
		stateKey = batchSuccessStateKey(bid)
	}

	err := rds.Set(ctx, stateKey, CallbackFinished, 0).Err()
	if err != nil {
		util.Warnf("batch callbacks: failed to mark %s callback as finished for batch %s: %v", callbackType, bid, err)
		return
	}

	util.Debugf("batch %s %s callback completed", bid, callbackType)

	b.checkPostCallback(ctx, b.Server, bid, callbackType)
}

// handleCallbackFailure is called when a callback job terminally fails.
// It marks the callback as finished so the batch can progress or be cleaned up,
// preventing the batch from being stuck forever when a callback goes to the morgue.
func (b *BatchSubsystem) handleCallbackFailure(ctx context.Context, job *client.Job, bid string, callbackType string) {
	// Determine if this is a terminal failure
	isTerminal := false
	if job.Retry != nil && *job.Retry <= 0 {
		isTerminal = true
	} else if job.Failure != nil && job.Failure.RetryRemaining == 0 {
		isTerminal = true
	}

	if !isTerminal {
		return
	}

	util.Warnf("batch %s %s callback job failed terminally, marking as finished", bid, callbackType)

	rds := b.Server.Manager().Redis()
	var stateKey string
	if callbackType == "complete" {
		stateKey = batchCompleteStateKey(bid)
	} else {
		stateKey = batchSuccessStateKey(bid)
	}

	err := rds.Set(ctx, stateKey, CallbackFinished, 0).Err()
	if err != nil {
		util.Warnf("batch callbacks: failed to mark %s callback as finished for batch %s: %v", callbackType, bid, err)
		return
	}

	b.checkPostCallback(ctx, b.Server, bid, callbackType)
}

// checkPostCallback handles actions after a callback completes
func (b *BatchSubsystem) checkPostCallback(ctx context.Context, s *server.Server, bid string, callbackType string) {
	// Re-check parent's callbacks (this might unblock them)
	batch, err := getBatch(ctx, s, bid)
	if err != nil {
		util.Warnf("batch callbacks: error getting batch %s: %v", bid, err)
		return
	}

	if batch.ParentBid != "" {
		util.Debugf("batch %s has parent %s, checking parent callbacks", bid, batch.ParentBid)
		go b.checkAndFireCallbacks(context.Background(), s, batch.ParentBid)
	}

	// If complete callback just finished, check if success callback can fire
	if callbackType == "complete" {
		go b.checkAndFireCallbacks(context.Background(), s, bid)
	}

	// Check if batch is fully complete for cleanup
	b.checkBatchCleanup(ctx, s, bid)
}

// checkBatchCleanup checks if a batch is fully complete and can be cleaned up
func (b *BatchSubsystem) checkBatchCleanup(ctx context.Context, s *server.Server, bid string) {
	status, err := getBatchStatus(ctx, s, bid)
	if err != nil {
		return
	}

	// Check if all callbacks are finished
	completeFinished := status.CompleteState == CallbackFinished || !hasCompleteCallback(ctx, s, bid)

	// Success is "finished" if:
	// 1. SuccessState == CallbackFinished (it ran), OR
	// 2. No success callback defined, OR
	// 3. failed > 0 (success can never fire because it requires failed == 0), OR
	// 4. Any child batch has failures (parent success can never fire)
	childFailures, childErr := anyChildHasFailures(ctx, s, bid)
	successFinished := status.SuccessState == CallbackFinished ||
		!hasSuccessCallback(ctx, s, bid) ||
		status.Failed > 0 ||
		(childErr == nil && childFailures)

	if completeFinished && successFinished {
		// If this batch has a parent, don't delete yet — the parent needs to be able
		// to observe this child's callback state. Instead, only delete when the parent
		// is also being cleaned up (the parent's cleanup will delete children).
		batch, err := getBatch(ctx, s, bid)
		if err != nil {
			util.Warnf("batch cleanup: failed to get batch %s: %v", bid, err)
			return
		}
		if batch.ParentBid != "" {
			parentExists, err := batchExists(ctx, s, batch.ParentBid)
			if err != nil {
				util.Warnf("batch cleanup: failed to check parent %s: %v", batch.ParentBid, err)
				return
			}
			if parentExists {
				util.Debugf("batch %s is fully complete but parent %s still exists, deferring cleanup", bid, batch.ParentBid)
				return
			}
		}

		util.Debugf("batch %s is fully complete, scheduling cleanup", bid)
		// Delete batch data and all child batches
		b.deleteBatchTree(ctx, s, bid)
	}
}

// deleteBatchTree deletes a batch and all of its child batches recursively
func (b *BatchSubsystem) deleteBatchTree(ctx context.Context, s *server.Server, bid string) {
	// Delete children first
	children, err := getChildBatches(ctx, s, bid)
	if err == nil {
		for _, childBid := range children {
			b.deleteBatchTree(ctx, s, childBid)
		}
	}

	// Delete this batch
	err = deleteBatch(ctx, s, bid)
	if err != nil {
		util.Warnf("batch cleanup: failed to delete batch %s: %v", bid, err)
	}
}

// hasCompleteCallback checks if a batch has a complete callback defined
func hasCompleteCallback(ctx context.Context, s *server.Server, bid string) bool {
	rds := s.Manager().Redis()
	completeJSON, err := rds.HGet(ctx, batchMetaKey(bid), "complete").Result()
	if err != nil && err != redis.Nil {
		return true // Assume callback exists on Redis error (conservative for cleanup)
	}
	return completeJSON != ""
}

// hasSuccessCallback checks if a batch has a success callback defined
func hasSuccessCallback(ctx context.Context, s *server.Server, bid string) bool {
	rds := s.Manager().Redis()
	successJSON, err := rds.HGet(ctx, batchMetaKey(bid), "success").Result()
	if err != nil && err != redis.Nil {
		return true // Assume callback exists on Redis error (conservative for cleanup)
	}
	return successJSON != ""
}
