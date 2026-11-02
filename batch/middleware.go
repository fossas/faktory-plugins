package batch

import (
	"context"
	"errors"
	"fmt"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
	"github.com/fossas/faktory-plugins/requeue"
	"github.com/redis/go-redis/v9"
)

// pushToMatchLua atomically checks that callback states are both pending (""),
// then increments the total, pending and unfinished counters. Returns 1 on
// success, 0 if callbacks have already started.
// KEYS[1] = complete_st, KEYS[2] = success_st, KEYS[3] = total, KEYS[4] = pending,
// KEYS[5] = unfinished
var pushToMatchLua = redis.NewScript(`
	local complete_st = redis.call("GET", KEYS[1]) or ""
	local success_st = redis.call("GET", KEYS[2]) or ""
	if complete_st ~= "" or success_st ~= "" then
		return 0
	end
	redis.call("INCR", KEYS[3])
	redis.call("INCR", KEYS[4])
	redis.call("INCR", KEYS[5])
	return 1
`)

// pushMiddleware tracks jobs being added to a batch
func (b *BatchSubsystem) pushMiddleware(ctx context.Context, next func() error) error {
	// During REQUEUE, the job is being put back on the queue — not a new addition.
	// Skip batch accounting so total/pending counters are not inflated.
	if requeue.IsRequeue(ctx) {
		return next()
	}

	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
	job := mh.Job()

	// Check if job belongs to a batch
	bidValue, ok := job.GetCustom("bid")
	if !ok {
		// Not a batch job, pass through
		return next()
	}

	bid, ok := bidValue.(string)
	if !ok || bid == "" {
		return next()
	}

	// Verify batch exists
	exists, err := batchExists(ctx, b.Server, bid)
	if err != nil {
		util.Warnf("batch push middleware: error checking batch %s: %v", bid, err)
		return manager.Halt("ERR", fmt.Sprintf("transient error checking batch %s, retry push", bid))
	}
	if !exists {
		return manager.Halt("ERR", fmt.Sprintf("batch %s does not exist", bid))
	}

	// Atomically check callback state and increment counters using a Lua script.
	// This prevents a TOCTOU race where a concurrent checkAndFireCallbacks could
	// see pending==0 and fire callbacks between our state check and counter increment.
	rds := b.Server.Manager().Redis()
	result, err := pushToMatchLua.Run(ctx, rds,
		[]string{
			batchCompleteStateKey(bid),
			batchSuccessStateKey(bid),
			batchTotalKey(bid),
			batchPendingKey(bid),
			batchUnfinishedKey(bid),
		},
	).Int64()
	if err != nil {
		util.Warnf("batch push middleware: failed to update counters for batch %s: %v", bid, err)
		return manager.Halt("ERR", "failed to update counters for batch")
	}
	if result == 0 {
		return manager.Halt("ERR", "cannot add jobs to batch after callbacks have started")
	}

	util.Debugf("Added job %s to batch %s", job.Jid, bid)

	// A later middleware (e.g. uniq's NOTUNIQUE) or the store can still reject
	// the push. The job would then never ACK or FAIL, so undo the increment.
	if err := next(); err != nil {
		pending, rbErr := adjustCounters(ctx, b.Server, bid, counterDeltas{pending: -1, total: -1, unfinished: -1})
		if rbErr != nil {
			util.Warnf("batch push middleware: failed to roll back counters for batch %s: %v", bid, rbErr)
		} else if pending <= 0 {
			go b.checkAndFireCallbacks(context.Background(), bid)
		}
		return err
	}
	return nil
}

// fetchMiddleware releases a batch job that a later fetch middleware discards
// (expire drops jobs past their expires_at). The job never runs, so it would
// never ACK or FAIL and its batch would wait on it forever. A discarded member
// counts as a terminal failure; a discarded callback counts as finished.
func (b *BatchSubsystem) fetchMiddleware(ctx context.Context, next func() error) error {
	err := next()
	var known manager.KnownError
	if err == nil || !errors.As(err, &known) || known.Code() != "DISCARD" {
		return err
	}

	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
	job := mh.Job()

	if cbType, ok := job.GetCustom("_cb"); ok {
		bid, bidOk := job.Custom["_bid"].(string)
		cbTypeStr, cbOk := cbType.(string)
		if bidOk && cbOk && bid != "" && cbTypeStr != "" {
			util.Warnf("batch %s %s callback job %s was discarded, marking as finished", bid, cbTypeStr, job.Jid)
			b.handleCallbackComplete(ctx, bid, cbTypeStr)
		}
		return err
	}

	bid, ok := job.Custom["bid"].(string)
	if !ok || bid == "" {
		return err
	}

	// pending was already decremented if the job failed before.
	var pendingDelta int64
	if job.Failure == nil {
		pendingDelta = -1
	}
	if _, adjErr := adjustCounters(ctx, b.Server, bid, counterDeltas{pending: pendingDelta, failed: 1, unfinished: -1}); adjErr != nil {
		util.Warnf("batch fetch middleware: failed to release discarded job %s from batch %s: %v", job.Jid, bid, adjErr)
		return err
	}

	util.Debugf("Job %s in batch %s was discarded, counted as failed", job.Jid, bid)
	go b.checkAndFireCallbacks(context.Background(), bid)
	return err
}

// ackMiddleware handles job completion (success)
func (b *BatchSubsystem) ackMiddleware(ctx context.Context, next func() error) error {
	// During REQUEUE, the job is being put back on the queue — not completed.
	// Skip batch accounting so pending is not decremented prematurely.
	if requeue.IsRequeue(ctx) {
		return next()
	}

	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
	job := mh.Job()

	// Execute the rest of the middleware chain first
	err := next()
	if err != nil {
		return err
	}

	// Handle callback job completion
	if cbType, ok := job.GetCustom("_cb"); ok {
		if bidValue, ok := job.GetCustom("_bid"); ok {
			bid, bidOk := bidValue.(string)
			cbTypeStr, cbOk := cbType.(string)

			if bidOk && cbOk && bid != "" && cbTypeStr != "" {
				b.handleCallbackComplete(ctx, bid, cbTypeStr)
			} else {
				util.Warnf("batch ack middleware: invalid _bid or _cb type for job %s", job.Jid)
			}
		} else {
			util.Warnf("batch ack middleware: job %s has _cb but missing _bid", job.Jid)
		}
		return nil
	}

	// Handle regular batch job completion
	bidValue, ok := job.GetCustom("bid")
	if !ok {
		return nil
	}

	bid, ok := bidValue.(string)
	if !ok || bid == "" {
		return nil
	}

	// Only decrement pending on first execution — if the job previously failed,
	// pending was already decremented by failMiddleware on first failure.
	// An ACK on any attempt finishes the job.
	var pendingDelta int64
	if job.Failure == nil {
		pendingDelta = -1
	}
	pending, err := adjustCounters(ctx, b.Server, bid, counterDeltas{pending: pendingDelta, unfinished: -1})
	if errors.Is(err, ErrBatchNotFound) {
		util.Debugf("Job %s ACKed after its batch %s was deleted", job.Jid, bid)
		return nil
	}
	if err != nil {
		util.Warnf("batch ack middleware: %v", err)
	}

	util.Debugf("Job %s in batch %s completed (ACK)", job.Jid, bid)

	// Check if callbacks should fire
	if pending <= 0 {
		go b.checkAndFireCallbacks(context.Background(), bid)
	}

	return nil
}

// failMiddleware handles job failure
func (b *BatchSubsystem) failMiddleware(ctx context.Context, next func() error) error {
	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
	job := mh.Job()

	// Execute the rest of the middleware chain first
	err := next()
	if err != nil {
		return err
	}

	// Handle callback job failure
	if cbType, ok := job.GetCustom("_cb"); ok {
		if bidValue, ok := job.GetCustom("_bid"); ok {
			bid, bidOk := bidValue.(string)
			cbTypeStr, cbOk := cbType.(string)

			if bidOk && cbOk && bid != "" && cbTypeStr != "" {
				b.handleCallbackFailure(ctx, job, bid, cbTypeStr)
			} else {
				util.Warnf("batch fail middleware: invalid _bid or _cb type for job %s", job.Jid)
			}
		} else {
			util.Warnf("batch fail middleware: job %s has _cb but missing _bid", job.Jid)
		}
		return nil
	}

	// Only count batch jobs
	bidValue, ok := job.GetCustom("bid")
	if !ok {
		return nil
	}

	bid, ok := bidValue.(string)
	if !ok || bid == "" {
		return nil
	}

	// Determine if this is the first execution or a retry
	// job.Failure.RetryCount is 0 on first failure, incremented for each retry
	isFirstExecution := job.Failure != nil && job.Failure.RetryCount == 0

	// Determine if this is a terminal failure (no more retries)
	// A job is terminally failed when:
	// 1. Retry is explicitly 0 (no retries configured)
	// 2. Failure.RetryRemaining == 0 (exhausted all retries)
	// Note: Retry == nil means use server default (25 retries), NOT terminal
	isTerminalFailure := false
	if job.Retry != nil && *job.Retry <= 0 {
		// Explicitly configured with no retries (0) or direct-to-morgue (-1)
		isTerminalFailure = true
	} else if job.Failure != nil && job.Failure.RetryRemaining == 0 {
		// Exhausted all retries
		isTerminalFailure = true
	}

	// Non-first, non-terminal failure: nothing to update, no need to check callbacks
	if !isFirstExecution && !isTerminalFailure {
		util.Debugf("Job %s in batch %s failed (retry, will try again)", job.Jid, bid)
		return nil
	}

	var pendingDelta, failedDelta, unfinishedDelta int64

	// Decrement pending only on first execution
	// (complete callback fires when all jobs have executed at least once)
	if isFirstExecution {
		pendingDelta = -1
		util.Debugf("Job %s in batch %s failed on first execution", job.Jid, bid)
	}

	// Increment failed, and finish the job, only on terminal failure
	if isTerminalFailure {
		failedDelta = 1
		unfinishedDelta = -1
		util.Debugf("Job %s in batch %s failed terminally", job.Jid, bid)
	}

	_, err = adjustCounters(ctx, b.Server, bid, counterDeltas{pending: pendingDelta, failed: failedDelta, unfinished: unfinishedDelta})
	if errors.Is(err, ErrBatchNotFound) {
		util.Debugf("Job %s FAILed after its batch %s was deleted", job.Jid, bid)
		return nil
	}
	if err != nil {
		util.Warnf("batch fail middleware: %v", err)
	}

	// Check if callbacks should fire
	go b.checkAndFireCallbacks(context.Background(), bid)

	return nil
}
