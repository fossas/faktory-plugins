package batch

import (
	"context"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/google/uuid"
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
	// Fires when: pending == 0 AND failed == 0 AND complete callback finished (or not defined) AND all children's success callbacks finished
	if status.Pending == 0 && status.Failed == 0 && status.SuccessState == CallbackPending {
		// Complete must be finished (or not defined)
		completeOk := status.CompleteState == CallbackFinished || !hasCompleteCallback(ctx, s, bid)
		childrenOk := allChildrenCallbackFinished(ctx, s, bid, "success")
		if completeOk && childrenOk {
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

	// Try to acquire lock atomically using SetNX
	acquired, err := rds.SetNX(ctx, lockKey, "1", 0).Result()
	if err != nil {
		util.Warnf("batch callbacks: failed to acquire lock for %s callback on batch %s: %v", callbackType, bid, err)
		return
	}
	if !acquired {
		// Another goroutine is handling this callback
		return
	}

	// Check current state
	currentState, _ := rds.Get(ctx, stateKey).Result()
	if currentState != CallbackPending {
		// Already processed
		return
	}

	// Mark as enqueued
	rds.Set(ctx, stateKey, CallbackEnqueued, 0)

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

	// Push the callback job
	err = s.Manager().Push(ctx, job)
	if err != nil {
		util.Warnf("batch callbacks: failed to enqueue %s callback for batch %s: %v", callbackType, bid, err)
		// Reset state and release lock to allow retry
		rds.Set(ctx, stateKey, CallbackPending, 0)
		rds.Del(ctx, lockKey)
		return
	}

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
	// 3. failed > 0 (success can never fire because it requires failed == 0)
	successFinished := status.SuccessState == CallbackFinished ||
		!hasSuccessCallback(ctx, s, bid) ||
		status.Failed > 0

	if completeFinished && successFinished {
		util.Debugf("batch %s is fully complete, scheduling cleanup", bid)
		// Delete batch data
		err := deleteBatch(ctx, s, bid)
		if err != nil {
			util.Warnf("batch cleanup: failed to delete batch %s: %v", bid, err)
		}
	}
}

// hasCompleteCallback checks if a batch has a complete callback defined
func hasCompleteCallback(ctx context.Context, s *server.Server, bid string) bool {
	rds := s.Manager().Redis()
	completeJSON, err := rds.HGet(ctx, batchMetaKey(bid), "complete").Result()
	if err != nil || completeJSON == "" {
		return false
	}
	return true
}

// hasSuccessCallback checks if a batch has a success callback defined
func hasSuccessCallback(ctx context.Context, s *server.Server, bid string) bool {
	rds := s.Manager().Redis()
	successJSON, err := rds.HGet(ctx, batchMetaKey(bid), "success").Result()
	if err != nil || successJSON == "" {
		return false
	}
	return true
}
