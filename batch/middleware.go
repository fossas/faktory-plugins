package batch

import (
	"context"
	"fmt"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

// pushToMatchLua atomically checks that callback states are both pending (""),
// then increments total and pending counters. Returns 1 on success, 0 if
// callbacks have already started.
// KEYS[1] = complete_st, KEYS[2] = success_st, KEYS[3] = total, KEYS[4] = pending
var pushToMatchLua = redis.NewScript(`
	local complete_st = redis.call("GET", KEYS[1]) or ""
	local success_st = redis.call("GET", KEYS[2]) or ""
	if complete_st ~= "" or success_st ~= "" then
		return 0
	end
	redis.call("INCR", KEYS[3])
	redis.call("INCR", KEYS[4])
	return 1
`)

// pushMiddleware tracks jobs being added to a batch
func (b *BatchSubsystem) pushMiddleware(ctx context.Context, next func() error) error {
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
	return next()
}

// ackMiddleware handles job completion (success)
func (b *BatchSubsystem) ackMiddleware(ctx context.Context, next func() error) error {
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
			bid, _ := bidValue.(string)
			cbTypeStr, _ := cbType.(string)
			b.handleCallbackComplete(ctx, bid, cbTypeStr)
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

	// Decrement pending counter
	redis := b.Server.Manager().Redis()
	pending, err := redis.Decr(ctx, batchPendingKey(bid)).Result()
	if err != nil {
		util.Warnf("batch ack middleware: failed to decrement pending for batch %s: %v", bid, err)
	}

	util.Debugf("Job %s in batch %s completed (ACK)", job.Jid, bid)

	// Check if callbacks should fire
	if pending <= 0 {
		go b.checkAndFireCallbacks(context.Background(), b.Server, bid)
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
			bid, _ := bidValue.(string)
			cbTypeStr, _ := cbType.(string)
			b.handleCallbackFailure(ctx, job, bid, cbTypeStr)
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
	isFirstExecution := job.Failure == nil || job.Failure.RetryCount == 0

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

	redis := b.Server.Manager().Redis()
	pipe := redis.TxPipeline()

	// Decrement pending only on first execution
	// (complete callback fires when all jobs have executed at least once)
	if isFirstExecution {
		pipe.Decr(ctx, batchPendingKey(bid))
		util.Debugf("Job %s in batch %s failed on first execution", job.Jid, bid)
	}

	// Increment failed only on terminal failure
	if isTerminalFailure {
		pipe.Incr(ctx, batchFailedKey(bid))
		util.Debugf("Job %s in batch %s failed terminally", job.Jid, bid)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		util.Warnf("batch fail middleware: failed to update counters for batch %s: %v", bid, err)
	}

	// Check if callbacks should fire
	go b.checkAndFireCallbacks(context.Background(), b.Server, bid)

	return nil
}
