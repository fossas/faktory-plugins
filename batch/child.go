package batch

import (
	"context"
	"fmt"
	"strconv"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

// addChildLua atomically checks that callback states are both pending (""),
// then adds the child to the parent's children set. Returns 1 on success,
// 0 if callbacks have already started.
// KEYS[1] = complete_st, KEYS[2] = success_st, KEYS[3] = children set
// ARGV[1] = child bid
var addChildLua = redis.NewScript(`
	local cs = redis.call("GET", KEYS[1]) or ""
	local ss = redis.call("GET", KEYS[2]) or ""
	if cs ~= "" or ss ~= "" then return 0 end
	redis.call("SADD", KEYS[3], ARGV[1])
	return 1
`)

// addChildBatch adds a child batch to a parent's children set
func addChildBatch(ctx context.Context, s *server.Server, parentBid, childBid string) error {
	// Verify parent exists
	exists, err := batchExists(ctx, s, parentBid)
	if err != nil {
		return fmt.Errorf("failed to check parent batch: %w", err)
	}
	if !exists {
		return fmt.Errorf("parent batch %s not found", parentBid)
	}

	// Atomically check callback state and add child to parent's children set.
	// This prevents a TOCTOU race where callbacks could fire between our state
	// check and the SADD.
	rds := s.Manager().Redis()
	result, err := addChildLua.Run(ctx, rds,
		[]string{
			batchCompleteStateKey(parentBid),
			batchSuccessStateKey(parentBid),
			batchChildrenKey(parentBid),
		},
		childBid,
	).Int64()
	if err != nil {
		return fmt.Errorf("failed to add child to parent: %w", err)
	}
	if result == 0 {
		return fmt.Errorf("cannot add child batch after parent callbacks have started")
	}

	// Only set TTL on children key if parent is not yet committed.
	// Committed parents have persistent keys; re-applying TTL would cause
	// the children key to expire prematurely.
	committed, _ := isCommitted(ctx, s, parentBid)
	if !committed {
		rds.Expire(ctx, batchChildrenKey(parentBid), BatchTTL)
	}

	util.Debugf("Added child batch %s to parent %s", childBid, parentBid)
	return nil
}

// getChildBatches returns all child batch IDs for a parent
func getChildBatches(ctx context.Context, s *server.Server, parentBid string) ([]string, error) {
	rds := s.Manager().Redis()
	children, err := rds.SMembers(ctx, batchChildrenKey(parentBid)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get children: %w", err)
	}
	return children, nil
}

// checkChildrenStatus checks all child batches' complete and success callback states
// plus their failure counters in a single pipeline (3N GETs, 1 round trip).
//
// Returns:
//   - completeAllFinished: true if every child's complete callback is done (or not defined)
//   - successAllFinished: true if every child's success callback is done (or not defined, or child has failures)
//   - anyFailures: true if any child has failed > 0
//   - err: non-nil on Redis errors
func checkChildrenStatus(ctx context.Context, s *server.Server, parentBid string) (bool, bool, bool, error) {
	children, err := getChildBatches(ctx, s, parentBid)
	if err != nil {
		util.Warnf("batch children: failed to get children for %s: %v", parentBid, err)
		return false, false, false, err
	}

	if len(children) == 0 {
		return true, true, false, nil
	}

	rds := s.Manager().Redis()

	// Pipeline: for each child, GET complete_st + GET success_st + GET failed
	pipe := rds.TxPipeline()
	type childCmds struct {
		bid           string
		completeStCmd *redis.StringCmd
		successStCmd  *redis.StringCmd
		failedCmd     *redis.StringCmd
	}
	cmds := make([]childCmds, len(children))
	for i, childBid := range children {
		cmds[i] = childCmds{
			bid:           childBid,
			completeStCmd: pipe.Get(ctx, batchCompleteStateKey(childBid)),
			successStCmd:  pipe.Get(ctx, batchSuccessStateKey(childBid)),
			failedCmd:     pipe.Get(ctx, batchFailedKey(childBid)),
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		util.Warnf("batch children: pipeline error for %s: %v", parentBid, err)
		return false, false, false, err
	}

	completeAllFinished := true
	successAllFinished := true
	anyFailures := false

	for _, c := range cmds {
		// Parse failed counter
		failed, err := strconv.ParseInt(c.failedCmd.Val(), 10, 64)
		if err != nil && c.failedCmd.Val() != "" {
			util.Warnf("batch children: failed to parse failed counter for child %s: %v", c.bid, err)
			return false, false, anyFailures, err
		}
		if failed > 0 {
			anyFailures = true
		}

		// Check complete callback state
		completeSt, completeErr := c.completeStCmd.Result()
		if completeErr == redis.Nil {
			// Child batch cleaned up, treat as finished
			util.Debugf("batch children: child %s complete state not found, assuming finished", c.bid)
		} else if completeErr != nil {
			util.Warnf("batch children: error reading complete state for child %s: %v", c.bid, completeErr)
			return false, false, anyFailures, completeErr
		} else if completeSt != CallbackFinished {
			if hasCompleteCallback(ctx, s, c.bid) {
				util.Debugf("batch children: child %s complete callback not finished (state=%s)", c.bid, completeSt)
				completeAllFinished = false
			}
		}

		// Check success callback state
		successSt, successErr := c.successStCmd.Result()
		if successErr == redis.Nil {
			// Child batch cleaned up, treat as finished
			util.Debugf("batch children: child %s success state not found, assuming finished", c.bid)
		} else if successErr != nil {
			util.Warnf("batch children: error reading success state for child %s: %v", c.bid, successErr)
			return false, false, anyFailures, successErr
		} else if successSt == CallbackFinished {
			// Already finished, nothing to do
		} else if failed > 0 {
			// Child has failures — its success callback can never fire.
			// Treat as "done" so we don't block the parent forever.
			util.Debugf("batch children: child %s has failures, success callback will never fire", c.bid)
		} else if hasSuccessCallback(ctx, s, c.bid) {
			util.Debugf("batch children: child %s success callback not finished (state=%s)", c.bid, successSt)
			successAllFinished = false
		}
	}

	if completeAllFinished {
		util.Debugf("batch children: all children of %s have finished complete callbacks", parentBid)
	}
	if successAllFinished {
		util.Debugf("batch children: all children of %s have finished success callbacks", parentBid)
	}
	return completeAllFinished, successAllFinished, anyFailures, nil
}

// anyChildHasFailures checks if any child batch has failed > 0.
// It pipelines GET failed for all children in a single round trip.
func anyChildHasFailures(ctx context.Context, s *server.Server, parentBid string) (bool, error) {
	children, err := getChildBatches(ctx, s, parentBid)
	if err != nil {
		util.Warnf("batch children: failed to get children for %s: %v", parentBid, err)
		return false, err
	}

	if len(children) == 0 {
		return false, nil
	}

	rds := s.Manager().Redis()
	pipe := rds.TxPipeline()
	failedCmds := make([]*redis.StringCmd, len(children))
	for i, childBid := range children {
		failedCmds[i] = pipe.Get(ctx, batchFailedKey(childBid))
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return false, err
	}

	for i, cmd := range failedCmds {
		failed, err := strconv.ParseInt(cmd.Val(), 10, 64)
		if err != nil && cmd.Val() != "" {
			util.Warnf("batch children: failed to parse failed counter for child %s: %v", children[i], err)
			return false, err
		}
		if failed > 0 {
			return true, nil
		}
	}
	return false, nil
}
