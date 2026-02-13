package batch

import (
	"context"
	"fmt"

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

// allChildrenCallbackFinished checks if all child batches have finished the specified callback type.
// For "success" callbacks, a child with failed > 0 is treated as "done" because its success
// callback can never fire. Use anyChildHasFailures separately to decide if the parent's
// own success callback should fire.
func allChildrenCallbackFinished(ctx context.Context, s *server.Server, parentBid string, callbackType string) bool {
	children, err := getChildBatches(ctx, s, parentBid)
	if err != nil {
		util.Warnf("batch children: failed to get children for %s: %v", parentBid, err)
		return false // Return false on error to prevent premature callback firing; will be retried on next job ACK/FAIL
	}

	if len(children) == 0 {
		return true
	}

	rds := s.Manager().Redis()

	for _, childBid := range children {
		// Get child's callback state
		var stateKey string
		if callbackType == "complete" {
			stateKey = batchCompleteStateKey(childBid)
		} else {
			stateKey = batchSuccessStateKey(childBid)
		}

		state, err := rds.Get(ctx, stateKey).Result()
		if err == redis.Nil {
			// Child batch cleaned up, treat as finished
			util.Debugf("batch children: child %s state not found, assuming finished", childBid)
			continue
		}
		if err != nil {
			util.Warnf("batch children: error reading state for child %s: %v", childBid, err)
			return false // Conservative: don't fire callbacks on Redis error
		}

		if state == CallbackFinished {
			continue
		}

		// For success callbacks: if child has failed > 0, the success callback
		// will never fire (it requires failed == 0). Treat as "done" so we
		// don't block the parent forever.
		if callbackType == "success" {
			childStatus, statusErr := getBatchStatus(ctx, s, childBid)
			if statusErr == nil && childStatus.Failed > 0 {
				util.Debugf("batch children: child %s has failures, success callback will never fire", childBid)
				continue
			}
		}

		// Check if child has this callback type defined
		var hasCallback bool
		if callbackType == "complete" {
			hasCallback = hasCompleteCallback(ctx, s, childBid)
		} else {
			hasCallback = hasSuccessCallback(ctx, s, childBid)
		}

		if hasCallback && state != CallbackFinished {
			util.Debugf("batch children: child %s %s callback not finished (state=%s)", childBid, callbackType, state)
			return false
		}
	}

	util.Debugf("batch children: all children of %s have finished %s callbacks", parentBid, callbackType)
	return true
}

// anyChildHasFailures checks if any child batch has failed > 0
func anyChildHasFailures(ctx context.Context, s *server.Server, parentBid string) (bool, error) {
	children, err := getChildBatches(ctx, s, parentBid)
	if err != nil {
		util.Warnf("batch children: failed to get children for %s: %v", parentBid, err)
		return false, err
	}

	var lastErr error
	for _, childBid := range children {
		childStatus, err := getBatchStatus(ctx, s, childBid)
		if err != nil {
			lastErr = err
			continue
		}
		if childStatus.Failed > 0 {
			return true, nil
		}
	}
	return false, lastErr
}
