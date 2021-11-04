package batch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

type batch struct {
	Id         string
	BatchKey   string
	ChildKey   string
	ParentsKey string
	JobsKey    string
	MetaKey    string
	Meta       *batchMeta
	Jobs       []string
	Parents    []*batch
	Children   []*batch
	Workers    map[string]string
	Subsystem  *BatchSubsystem
	rclient    *redis.Client
	mu         sync.Mutex
}

const (
	// CallbackJobPending no status
	CallbackJobPending = ""
	// CallbackJobQueued callback job has been queued
	CallbackJobQueued = "1"
	// CallbackJobSucceeded callback job has succeeded
	CallbackJobSucceeded = "2"
)

type batchMeta struct {
	Total            int
	Failed           int
	Pending          int
	Succeeded        int
	CreatedAt        string
	Description      string
	Committed        bool
	SuccessJob       string
	CompleteJob      string
	SuccessJobState  string
	CompleteJobState string
}

func (b *batch) init() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	meta, err := b.rclient.HGetAll(b.MetaKey).Result()
	if err != nil {
		return nil
	}

	if err := b.Subsystem.Server.Manager().Redis().SAdd("batches", b.Id).Err(); err != nil {
		return fmt.Errorf("init: store batch: %v", err)
	}

	if err := b.Subsystem.Server.Manager().Redis().SetNX(b.BatchKey, b.Id, time.Duration(2*time.Hour)).Err(); err != nil {
		return fmt.Errorf("init: set expiration: %v", err)
	}
	jobs, err := b.Subsystem.Server.Manager().Redis().SMembers(b.JobsKey).Result()
	if err != nil {
		return fmt.Errorf("init: get jobs: %v", err)
	}
	b.Jobs = jobs

	if len(meta) == 0 {
		// set default values
		data := map[string]interface{}{
			"total":          b.Meta.Total,
			"failed":         b.Meta.Failed,
			"succeeded":      b.Meta.Succeeded,
			"pending":        b.Meta.Pending,
			"created_at":     b.Meta.CreatedAt,
			"description":    b.Meta.Description,
			"committed":      b.Meta.Committed,
			"success_job":    b.Meta.SuccessJob,
			"complete_job":   b.Meta.CompleteJob,
			"success_st":     b.Meta.SuccessJobState,
			"complete_st":    b.Meta.CompleteJobState,
		}
		b.rclient.HMSet(b.MetaKey, data)
		return nil
	}

	b.Meta.Total, err = strconv.Atoi(meta["total"])
	if err != nil {
		return fmt.Errorf("init: failed converting string to int: %v", err)
	}
	b.Meta.Failed, err = strconv.Atoi(meta["failed"])
	if err != nil {
		return fmt.Errorf("init: failed converting string to int: %v", err)
	}
	b.Meta.Succeeded, err = strconv.Atoi(meta["succeeded"])
	if err != nil {
		return fmt.Errorf("init: failed converting string to int: %v", err)
	}
	b.Meta.Pending, err = strconv.Atoi(meta["pending"])
	if err != nil {
		return fmt.Errorf("init: failed converting string to int: %v", err)
	}

	b.Meta.Committed, err = strconv.ParseBool(meta["committed"])
	if err != nil {
		return fmt.Errorf("init: failed converting string to bool: %v", err)
	}
	b.Meta.Description = meta["description"]
	b.Meta.CreatedAt = meta["created_at"]
	b.Meta.SuccessJob = meta["success_job"]
	b.Meta.CompleteJob = meta["complete_job"]
	if b.Meta.SuccessJob == "" && b.Meta.CompleteJob == "" {
		return fmt.Errorf("init: no callback job was specified for batch %s", b.Id)
	}
	b.Meta.SuccessJobState = meta["success_st"]
	b.Meta.CompleteJobState = meta["complete_st"]

	return nil
}

func (b *batch) commit() error {
	b.mu.Lock()
	if err := b.updateCommitted(true); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) open() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isBatchDone() {
		return fmt.Errorf("open: batch job (%s) has already finished", b.Id)
	}
	if err := b.updateCommitted(false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (b *batch) jobQueued(jobId string) error {
	b.mu.Lock()
	if err := b.addJobToBatch(jobId); err != nil {
		return fmt.Errorf("jobQueued: add job to batch: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) jobFinished(jobId string, success bool, isRetry bool) error {
	b.mu.Lock()
	if err := b.removeJobFromBatch(jobId, success, isRetry); err != nil {
		return fmt.Errorf("jobFinished: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) callbackJobSucceeded(callbackType string) error {
	b.mu.Lock()
	if err := b.updateJobCallbackState(callbackType, CallbackJobSucceeded); err != nil {
		return fmt.Errorf("callbackJobSucceeded: update callback job state: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) setWorkerForJid(jid string, wid string) {
	b.mu.Lock()
	b.Workers[jid] = wid
	b.mu.Unlock()
}

func (b *batch) removeWorkerForJid(jid string) {
	b.mu.Lock()
	delete(b.Workers, jid)
	b.mu.Unlock()
}

func (b *batch) hasWorker(wid string) bool {
	for _, worker := range b.Workers {
		if worker == wid {
			return true
		}
	}
	return false
}

func (b *batch) remove() error {
	b.mu.Lock()
	if err := b.Subsystem.Server.Manager().Redis().SRem("batches", b.Id).Err(); err != nil {
		return fmt.Errorf("remove: batch (%s) %v", b.Id, err)
	}
	if err := b.rclient.Del(b.MetaKey).Err(); err != nil {
		return fmt.Errorf("remove: batch (%s) %v", b.Id, err)
	}
	if err := b.rclient.Del(b.JobsKey).Err(); err != nil {
		return fmt.Errorf("remove: batch (%s), %v", b.Id, err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) updateCommitted(committed bool) error {
	b.Meta.Committed = committed
	if err := b.rclient.HSet(b.MetaKey, "committed", committed).Err(); err != nil {
		return fmt.Errorf("updateCommitted: could not update committed: %v", err)
	}
	if committed {
		// remove expiration as batch has been committed
		if err := b.extendBatchExpiration(); err != nil {
			return fmt.Errorf("updatedCommitted: could not expire: %v", err)
		}
		b.checkBatchDone()
	} else {
		if err := b.Subsystem.Server.Manager().Redis().Expire(b.BatchKey, time.Duration(2*time.Hour)).Err(); err != nil {
			return fmt.Errorf("updatedCommitted: could not expire: %v", err)
		}
	}
	return nil
}

func (b *batch) extendBatchExpiration() error {
	return b.Subsystem.Server.Manager().Redis().Expire(b.BatchKey, time.Duration(2*time.Hour)).Err()
}

func (b *batch) updateJobCallbackState(callbackType string, state string) error {
	if callbackType == "success" {
		b.Meta.SuccessJobState = state
		if err := b.rclient.HSet(b.MetaKey, "success_st", state).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set success_st: %v", err)
		}
	} else {
		b.Meta.CompleteJobState = state
		if err := b.rclient.HSet(b.MetaKey, "completed_st", state).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set completed_st: %v", err)
		}
	}
	return nil
}

func (b *batch) addJobToBatch(jobId string) error {
	if err := b.rclient.SAdd(b.JobsKey, jobId).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: %v", err)
	}
	b.Meta.Total += 1
	if err := b.rclient.HIncrBy(b.MetaKey, "total", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify total: %v", err)
	}
	b.Meta.Pending += 1
	if err := b.rclient.HIncrBy(b.MetaKey, "pending", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify pending: %v", err)
	}
	b.Jobs = append(b.Jobs, jobId)
	return nil
}

func (b *batch) removeJobFromBatch(jobId string, success bool, isRetry bool) error {
	if !isRetry {
		// job has already been removed
		if err := b.rclient.SRem(b.JobsKey, jobId).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: could not remove job key %v", err)
		}
		for i, v := range b.Jobs {
			if v == jobId {
				b.Jobs = append(b.Jobs[:i], b.Jobs[i+1:]...)
				break
			}
		}
		b.Meta.Pending -= 1
		if err := b.rclient.HIncrBy(b.MetaKey, "pending", -1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify pending: %v", err)
		}

	}
	if success {
		b.Meta.Succeeded += 1
		if err := b.rclient.HIncrBy(b.MetaKey, "succeeded", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify succeeded: %v", err)
		}
	} else {
		b.Meta.Failed += 1
		if err := b.rclient.HIncrBy(b.MetaKey, "failed", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify failed: %v", err)
		}
	}

	b.checkBatchDone()
	return nil
}

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
	if err := childBatch.addParent(b); err != nil {
		return fmt.Errorf("addChild: erorr adding parent batch (%s) to child (%s): %v", b.Id, childBatch.Id, err)
	}
	b.checkBatchDone()
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
	return nil
}

func (b *batch) childFinished() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.areChildrenFinished() {
		b.checkBatchDone()
	}
}

func (b *batch) areChildrenFinished() bool {
	// iterate through children up to a certain depth
	// check to see if any batch still has jobs being processed
	currentDepth := 1
	visited := map[string]bool{}
	stack := b.Children
	var childStack []*batch
	var child *batch

	for len(stack) > 0 {

		child, stack = stack[0], stack[1:]
		if visited[child.Id] {
			goto nextDepth
		}
		visited[child.Id] = true
		if !child.isBatchDone() {
			return false
		}
		if len(child.Children) > 0 {
			childStack = append(childStack, child.Children...)
		}

		nextDepth:
			if len(stack) == 0 && len(childStack) > 0 {
				if currentDepth == b.Subsystem.Options.ChildDepthTracked {
					return true
				}
				currentDepth += 1
				stack = childStack
				childStack = []*batch{}
			}
	}
	return true
}

func (b *batch) isBatchDone() bool {
	return b.Meta.Committed == true && b.Meta.Pending == 0
}

func (b *batch) checkBatchDone() {
	if b.isBatchDone() {
		if b.areChildrenFinished() {
			// only create callback jobs if children are considered done
			if b.Meta.CompleteJob != "" && b.Meta.CompleteJobState == CallbackJobPending {
				b.queueBatchDoneJob(b.Meta.CompleteJob, "complete")
			}
			if b.Meta.Succeeded == b.Meta.Total && b.Meta.SuccessJob != "" && b.Meta.SuccessJobState == CallbackJobPending {
				b.queueBatchDoneJob(b.Meta.SuccessJob, "success")
			}
		}
		// notify parents child is done
		for _, parent := range b.Parents {
			parent.childFinished()
		}
	}
}

func (b *batch) queueBatchDoneJob(jobData string, callbackType string) {
	var job client.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		util.Warnf("queueBatchDoneJob: unmarshal job(%s): %v", callbackType, err)
		return
	}
	job.Jid = fmt.Sprintf("%s-%s", b.Id, callbackType)
	// these are required to update the call back job state
	job.SetCustom("_bid", b.Id)
	job.SetCustom("_cb", callbackType)
	if err := b.Subsystem.Server.Manager().Push(&job); err != nil {
		util.Warnf("queueBatchDoneJob: cannot push job (%s) %v", callbackType, err)
		return
	}
	if err := b.updateJobCallbackState(callbackType, CallbackJobQueued); err != nil {
		util.Warnf("queueBatchDoneJob: could not update job callback state: %v", err)
	}
	util.Infof("Pushed %s job (jid: %s)", callbackType, job.Jid)
}
