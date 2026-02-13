package batch

import (
	"context"
	"fmt"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

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

	// Verify parent callbacks haven't started
	status, err := getBatchStatus(ctx, s, parentBid)
	if err != nil {
		return fmt.Errorf("failed to get parent batch status: %w", err)
	}
	if status.CompleteState != CallbackPending || status.SuccessState != CallbackPending {
		return fmt.Errorf("cannot add child batch after parent callbacks have started")
	}

	// Add child to parent's children set
	rds := s.Manager().Redis()
	err = rds.SAdd(ctx, batchChildrenKey(parentBid), childBid).Err()
	if err != nil {
		return fmt.Errorf("failed to add child to parent: %w", err)
	}

	// Set TTL on children key (will be persisted when parent commits)
	rds.Expire(ctx, batchChildrenKey(parentBid), BatchTTL)

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

	for _, childBid := range children {
		childStatus, err := getBatchStatus(ctx, s, childBid)
		if err != nil {
			continue
		}
		if childStatus.Failed > 0 {
			return true, nil
		}
	}
	return false, nil
}
