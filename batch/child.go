package batch

import (
	"fmt"
	"github.com/contribsys/faktory/util"
	"time"
)

func (b *batch) addChild(childBatch *batch) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if childBatch.Id == b.Id {
		return fmt.Errorf("addChild: child batch is the same as the parent")
	}
	for _, child := range b.Children {
		if child.Id == childBatch.Id {
			// avoid duplicates
			return nil
		}
	}
	b.Children = append(b.Children, childBatch)
	if err := b.rclient.SAdd(b.ChildKey, childBatch.Id).Err(); err != nil {
		return fmt.Errorf("addChild: cannot save child (%s) to batch (%s) %v", childBatch.Id, b.Id, err)
	}
	if len(b.Children) == 1 {
		// only set expire when adding the first child
		if err := b.rclient.Expire(b.ChildKey, time.Duration(b.Subsystem.Options.CommittedTimeoutDays)*time.Hour*24).Err(); err != nil {
			util.Warnf("addChild: could not set expiration for set storing batch children: %v", err)
		}
	}
	if err := childBatch.addParent(b); err != nil {
		return fmt.Errorf("addChild: erorr adding parent batch (%s) to child (%s): %v", b.Id, childBatch.Id, err)
	}
	if b.areBatchJobsCompleted() {
		b.handleBatchJobsCompleted()
	}
	return nil
}

func (b *batch) addParent(parentBatch *batch) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if parentBatch.Id == b.Id {
		return fmt.Errorf("addParent: parent batch is the same as the child")
	}
	for _, parent := range b.Parents {
		if parent.Id == parentBatch.Id {
			// avoid duplicates
			return nil
		}
	}
	b.Parents = append(b.Parents, parentBatch)
	if err := b.rclient.SAdd(b.ParentsKey, parentBatch.Id).Err(); err != nil {
		return fmt.Errorf("addParent: %v", err)
	}
	if len(b.Parents) == 1 {
		// only set expire when adding the first parent
		if err := b.rclient.Expire(b.ParentsKey, time.Duration(b.Subsystem.Options.CommittedTimeoutDays)*time.Hour*24).Err(); err != nil {
			util.Warnf("addChild: could not set expiration for set storing batch children: %v", err)
		}
	}
	return nil
}

func (b *batch) removeParent(parentBatch *batch) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, p := range b.Parents {
		if p.Id == parentBatch.Id {
			b.Parents = append(b.Parents[:i], b.Parents[i+1:]...)
			break
		}
	}
	if err := b.rclient.SRem(b.ParentsKey, parentBatch.Id).Err(); err != nil {
		return fmt.Errorf("removeParent: could not remove parent %v", err)
	}
	return nil
}

func (b *batch) removeChildren() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.Children) > 0 {
		b.Children = []*batch{}
		if err := b.rclient.Del(b.ChildKey).Err(); err != nil {
			util.Warnf("removeChildren: unable to remove child batches from %s: %v", b.Id, err)
		}
	}
}

func (b *batch) handleChildComplete(childBatch *batch, areChildsChildrenFinished bool, visited map[string]bool) {
	if b.areChildrenFinished(visited) {
		if areChildsChildrenFinished {
			// batch can be removed as a parent to stop propagation
			if err := childBatch.removeParent(b); err != nil {
				util.Warnf("childCompleted: unable to remove parent (%s) from (%s): %v", b.Id, childBatch.Id, err)
			}
		}
		if b.areBatchJobsCompleted() {
			b.handleBatchJobsCompleted()
		}
	}
}

func (b *batch) areChildrenFinished(visited map[string]bool) bool {
	// iterate through children up to a certain depth
	// check to see if any batch still has jobs being processed
	currentDepth := 1
	visited[b.Id] = true // handle circular cases
	stack := b.Children
	var childStack []*batch
	var child *batch
	var maxSearchDepth int
	if b.Meta.ChildSearchDepth != nil {
		maxSearchDepth = *b.Meta.ChildSearchDepth
	} else {
		maxSearchDepth = b.Subsystem.Options.ChildSearchDepth
	}
	for len(stack) > 0 {
		child, stack = stack[0], stack[1:]
		if visited[child.Id] {
			goto nextDepth
		}
		visited[child.Id] = true
		if !child.areBatchJobsCompleted() {
			return false
		}
		if len(child.Children) > 0 {
			childStack = append(childStack, child.Children...)
		}

	nextDepth:
		if len(stack) == 0 && len(childStack) > 0 {
			if currentDepth == maxSearchDepth {
				return true
			}
			currentDepth += 1
			stack = childStack
			childStack = []*batch{}
		}
	}
	return true
}
