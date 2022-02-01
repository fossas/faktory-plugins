package batch

import (
	"fmt"
	"time"

	"github.com/contribsys/faktory/util"
)

func (m *batchManager) addChild(batch *batch, childBatch *batch) error {
	if childBatch.Id == batch.Id {
		return fmt.Errorf("addChild: child batch is the same as the parent")
	}
	for _, child := range batch.Children {
		if child.Id == childBatch.Id {
			// avoid duplicates
			return nil
		}
	}
	batch.Children = append(batch.Children, childBatch)
	if err := m.rclient.SAdd(m.getChildKey(batch.Id), childBatch.Id).Err(); err != nil {
		return fmt.Errorf("addChild: cannot save child (%s) to batch (%s) %v", childBatch.Id, batch.Id, err)
	}
	batch.Meta.ChildCount += 1
	if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "child_count", 1).Err(); err != nil {
		return fmt.Errorf("addChild: cannot increment cihldren_count to batch (%s) %v", batch.Id, err)
	}
	if len(batch.Children) == 1 {
		// only set expire when adding the first child
		if err := m.rclient.Expire(m.getChildKey(batch.Id), time.Duration(m.Subsystem.Options.CommittedTimeoutDays)*time.Hour*24).Err(); err != nil {
			util.Warnf("addChild: could not set expiration for set storing batch children: %v", err)
		}
	}
	if err := m.addParent(childBatch, batch); err != nil {
		return fmt.Errorf("addChild: erorr adding parent batch (%s) to child (%s): %v", batch.Id, childBatch.Id, err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(batch, map[string]bool{batch.Id: true})
	}
	return nil
}

func (m *batchManager) addParent(batch *batch, parentBatch *batch) error {
	if parentBatch.Id == batch.Id {
		return fmt.Errorf("addParent: parent batch is the same as the child")
	}
	for _, parent := range batch.Parents {
		if parent.Id == parentBatch.Id {
			// avoid duplicates
			return nil
		}
	}
	batch.Parents = append(batch.Parents, parentBatch)
	if err := m.rclient.SAdd(m.getParentsKey(batch.Id), parentBatch.Id).Err(); err != nil {
		return fmt.Errorf("addParent: %v", err)
	}
	if len(batch.Parents) == 1 {
		// only set expire when adding the first parent
		if err := m.rclient.Expire(m.getParentsKey(batch.Id), time.Duration(m.Subsystem.Options.CommittedTimeoutDays)*time.Hour*24).Err(); err != nil {
			util.Warnf("addChild: could not set expiration for set storing batch children: %v", err)
		}
	}
	return nil
}

func (m *batchManager) removeParent(batch *batch, parentBatch *batch) error {
	for i, p := range batch.Parents {
		if p.Id == parentBatch.Id {
			batch.Parents = append(batch.Parents[:i], batch.Parents[i+1:]...)
			break
		}
	}
	if err := m.rclient.SRem(m.getParentsKey(batch.Id), parentBatch.Id).Err(); err != nil {
		return fmt.Errorf("removeParent: could not remove parent %v", err)
	}
	return nil
}

func (m *batchManager) removeChildren(b *batch) {
	// locking must be handled outside of function
	if len(b.Children) > 0 {
		b.Children = []*batch{}
		if err := m.rclient.Del(m.getChildKey(b.Id)).Err(); err != nil {
			util.Warnf("removeChildren: unable to remove child batches from %s: %v", b.Id, err)
		}
		b.Meta.ChildCount = 0
		if err := m.rclient.HSet(m.getMetaKey(b.Id), "child_count", 0).Err(); err != nil {
			util.Warnf("removeChildren: unable to remove child batches from %s: %v", b.Id, err)
		}
	}
}

func (m *batchManager) handleChildComplete(batch *batch, childBatch *batch, areChildsChildrenFinished bool, areChildsChildrenSucceeded bool, parentsVisited map[string]bool) {
	if areChildsChildrenFinished && areChildsChildrenSucceeded {
		// batch can be removed as a parent to stop propagation
		if err := m.removeParent(childBatch, batch); err != nil {
			util.Warnf("childCompleted: unable to remove parent (%s) from (%s): %v", batch.Id, childBatch.Id, err)
		}
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(batch, parentsVisited)
	}
}

func (m *batchManager) areChildrenFinished(b *batch) (bool, bool) {
	// iterate through children up to a certain depth
	// check to see if any batch still has jobs being processed
	currentDepth := 1
	visited := map[string]bool{b.Id: true} // handle circular cases
	stack := b.Children
	var childStack []*batch
	var child *batch
	var maxSearchDepth int
	succeeded := true
	if b.Meta.ChildSearchDepth != nil {
		maxSearchDepth = *b.Meta.ChildSearchDepth
	} else {
		maxSearchDepth = m.Subsystem.Options.ChildSearchDepth
	}
	for len(stack) > 0 {
		child, stack = stack[0], stack[1:]
		if visited[child.Id] {
			goto nextDepth
		}
		visited[child.Id] = true
		if !m.areBatchJobsCompleted(child) {
			return false, false
		}
		if len(child.Children) != child.Meta.ChildCount && child.Meta.ChildCount != 0 {
			// one of the child batches timed out
			return false, false
		}
		if succeeded && !m.areBatchJobsSucceeded(child) {
			succeeded = false
		}
		if len(child.Children) > 0 {
			childStack = append(childStack, child.Children...)
		}

	nextDepth:
		if len(stack) == 0 && len(childStack) > 0 {
			if currentDepth == maxSearchDepth {
				return true, succeeded
			}
			currentDepth += 1
			stack = childStack
			childStack = []*batch{}
		}
	}
	return true, succeeded
}
