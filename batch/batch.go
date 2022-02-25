package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

type batch struct {
	Id       string
	Meta     *batchMeta
	Parents  []*batch
	Children []*batch
	mu       sync.Mutex
}

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
	ChildSearchDepth *int
	ChildCount       int
}

type batchManager struct {
	Batches   map[string]*batch
	Subsystem *BatchSubsystem
	rclient   *redis.Client
	mu        sync.RWMutex
}

const (
	// CallbackJobPending no status
	CallbackJobPending = ""
	// CallbackJobQueued callback job has been queued
	CallbackJobQueued = "1"
	// CallbackJobSucceeded callback job has succeeded
	CallbackJobSucceeded = "2"
)

func (m *batchManager) getBatchFromInterface(batchId interface{}) (*batch, error) {
	bid, ok := batchId.(string)
	if !ok {
		return nil, errors.New("getBatchFromInterface: invalid custom bid value")
	}
	batch, err := m.getBatch(bid)
	if err != nil {
		util.Warnf("getBatchFromInterface: Unable to retrieve batch: %v", err)
		return nil, fmt.Errorf("getBatchFromInterface: unable to get batch: %s", bid)
	}
	return batch, nil
}

func (m *batchManager) loadExistingBatches() error {
	vals, err := m.rclient.SMembers("batches").Result()
	if err != nil {
		return fmt.Errorf("loadExistingBatches: retrieve batches: %v", err)
	}
	for idx := range vals {
		batch, err := m.newBatch(vals[idx], &batchMeta{})
		if err != nil {
			util.Warnf("loadExistingBatches: error load batch (%s) %v", vals[idx], err)
			continue
		}
		m.Batches[vals[idx]] = batch
	}

	// update parent and children
	for _, b := range m.Batches {
		parentIds, err := m.rclient.SMembers(m.getParentsKey(b.Id)).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, parentId := range parentIds {
			parentBatch, err := m.getBatch(parentId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting parent batch (%s) %v", parentId, err)
				continue
			}
			b.Parents = append(b.Parents, parentBatch)
		}

		childIds, err := m.rclient.SMembers(m.getChildKey(b.Id)).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, childId := range childIds {
			childBatch, err := m.getBatch(childId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting child batch (%s) %v", childId, err)
				continue
			}
			b.Children = append(b.Children, childBatch)
		}

		if m.areBatchJobsCompleted(b) {
			m.handleBatchJobsCompleted(b, map[string]bool{b.Id: true})
		}
	}

	return nil
}

func (m *batchManager) getBatch(batchId string) (*batch, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if batchId == "" {
		return nil, fmt.Errorf("getBatch: batchId cannot be blank")
	}

	b, ok := m.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("getBatch: no batch found")
	}

	exists, err := m.rclient.Exists(m.getBatchKey(b.Id)).Result()
	if err != nil {
		util.Warnf("Cannot confirm batch exists: %v", err)
		return nil, fmt.Errorf("getBatch: unable to check if batch has timed out")
	}
	if exists == 0 {
		m.removeBatch(b)
		return nil, fmt.Errorf("getBatch: batch was not committed within 2 hours")
	}

	return b, nil
}

func (m *batchManager) removeBatch(batch *batch) {
	m.mu.Lock()
	if err := m.remove(batch); err != nil {
		util.Warnf("removeBatch: unable to remove batch: %v", err)
	}
	delete(m.Batches, batch.Id)

	batch = nil
	m.mu.Unlock()
}

func (m *batchManager) removeStaleBatches() {
	util.Debugf("Checking for stale batches")
	for _, b := range m.Batches {
		createdAt, err := time.Parse(time.RFC3339Nano, b.Meta.CreatedAt)
		if err != nil {
			continue
		}
		remove := false
		uncommittedTimeout := time.Now().Add(-time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute).UTC()
		committedTimeout := time.Now().AddDate(0, 0, -m.Subsystem.Options.CommittedTimeoutDays).UTC()
		if !b.Meta.Committed && createdAt.Before(uncommittedTimeout) {
			remove = true
		} else if b.Meta.Committed && createdAt.Before(committedTimeout) {
			remove = true
		}

		if remove {
			util.Debugf("Removing stale batch %s", b.Id)
			b.mu.Lock()
			m.removeBatch(b)
			b.mu.Unlock()
		}
	}
}

func (m *batchManager) newBatchMeta(description string, success string, complete string, childSearchDepth *int) *batchMeta {
	return &batchMeta{
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Total:            0,
		Succeeded:        0,
		Failed:           0,
		Pending:          0,
		SuccessJob:       success,
		CompleteJob:      complete,
		Description:      description,
		SuccessJobState:  CallbackJobPending,
		CompleteJobState: CallbackJobPending,
		ChildSearchDepth: childSearchDepth,
		ChildCount:       0,
	}
}

func (m *batchManager) newBatch(batchId string, meta *batchMeta) (*batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := &batch{
		Id:       batchId,
		Parents:  make([]*batch, 0),
		Children: make([]*batch, 0),
		Meta:     meta,
		mu:       sync.Mutex{},
	}
	if err := m.init(b); err != nil {
		return nil, fmt.Errorf("newBatch: %v", err)
	}

	m.Batches[batchId] = b

	return b, nil
}

func (m *batchManager) getBatchKey(batchId string) string {
	return fmt.Sprintf("batch-%s", batchId)
}

func (m *batchManager) getMetaKey(batchId string) string {
	return fmt.Sprintf("meta-%s", batchId)
}

func (m *batchManager) getParentsKey(batchId string) string {
	return fmt.Sprintf("parent-ids-%s", batchId)
}

func (m *batchManager) getChildKey(batchId string) string {
	return fmt.Sprintf("child-ids--%s", batchId)
}

func (m *batchManager) getSuccessJobStateKey(batchId string) string {
	return fmt.Sprintf("success-st-%s", batchId)
}

func (m *batchManager) getCompleteJobStateKey(batchId string) string {
	return fmt.Sprintf("complete-st-%s", batchId)
}

func (m *batchManager) init(batch *batch) error {
	meta, err := m.rclient.HGetAll(m.getMetaKey(batch.Id)).Result()
	if err != nil {
		return fmt.Errorf("init: unable to retrieve meta: %v", err)
	}

	if err := m.rclient.SAdd("batches", batch.Id).Err(); err != nil {
		return fmt.Errorf("init: store batch: %v", err)
	}

	expiration := time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute
	if err := m.rclient.SetNX(m.getBatchKey(batch.Id), batch.Id, expiration).Err(); err != nil {
		return fmt.Errorf("init: set expiration: %v", err)
	}

	if len(meta) == 0 {
		// set default values
		data := map[string]interface{}{
			"total":        batch.Meta.Total,
			"failed":       batch.Meta.Failed,
			"succeeded":    batch.Meta.Succeeded,
			"pending":      batch.Meta.Pending,
			"created_at":   batch.Meta.CreatedAt,
			"description":  batch.Meta.Description,
			"committed":    batch.Meta.Committed,
			"success_job":  batch.Meta.SuccessJob,
			"complete_job": batch.Meta.CompleteJob,
			"child_count":  batch.Meta.ChildCount,
		}
		if batch.Meta.ChildSearchDepth != nil {
			data["child_search_depth"] = *batch.Meta.ChildSearchDepth
		}
		if err := m.rclient.HMSet(m.getMetaKey(batch.Id), data).Err(); err != nil {
			return fmt.Errorf("init: could not load meta for batch: %s: %v", batch.Id, err)
		}
		if err := m.rclient.Expire(m.getMetaKey(batch.Id), expiration).Err(); err != nil {
			return fmt.Errorf("init: could set expiration for batch meta: %v", err)
		}

		timeout := time.Duration(m.Subsystem.Options.CommittedTimeoutDays) * 24 * time.Hour
		if err := m.rclient.SetNX(m.getSuccessJobStateKey(batch.Id), CallbackJobPending, timeout).Err(); err != nil {
			return fmt.Errorf("init: could not set success_st: %v", err)
		}
		if err := m.rclient.SetNX(m.getCompleteJobStateKey(batch.Id), CallbackJobPending, timeout).Err(); err != nil {
			return fmt.Errorf("init: could not set complete_st: %v", err)
		}
		return nil
	}

	batch.Meta.Total, err = strconv.Atoi(meta["total"])
	if err != nil {
		return fmt.Errorf("init: total: failed converting string to int: %v", err)
	}
	batch.Meta.Failed, err = strconv.Atoi(meta["failed"])
	if err != nil {
		return fmt.Errorf("init: failed: failed converting string to int: %v", err)
	}
	batch.Meta.Succeeded, err = strconv.Atoi(meta["succeeded"])
	if err != nil {
		return fmt.Errorf("init: succeeded: failed converting string to int: %v", err)
	}
	batch.Meta.Pending, err = strconv.Atoi(meta["pending"])
	if err != nil {
		return fmt.Errorf("init: pending: failed converting string to int: %v", err)
	}
	batch.Meta.ChildCount, err = strconv.Atoi(meta["child_count"])
	if err != nil {
		return fmt.Errorf("init: pending: failed converting string to int: %v", err)
	}

	batch.Meta.Committed, err = strconv.ParseBool(meta["committed"])
	if err != nil {
		return fmt.Errorf("init: committed: failed converting string to bool: %v", err)
	}
	batch.Meta.Description = meta["description"]
	batch.Meta.CreatedAt = meta["created_at"]
	batch.Meta.SuccessJob = meta["success_job"]
	batch.Meta.CompleteJob = meta["complete_job"]
	if childSearchDepth, ok := meta["child_search_depth"]; ok {
		depth, err := strconv.Atoi(childSearchDepth)
		if err != nil {
			util.Warnf("Unable to set childSearchDepth for batch: %s", batch.Id)
		} else {
			batch.Meta.ChildSearchDepth = &depth
		}
	}
	successJobState, err := m.rclient.Get(m.getSuccessJobStateKey(batch.Id)).Result()
	if err == redis.Nil {
		successJobState = CallbackJobPending
	} else if err != nil {
		return fmt.Errorf("init: get success job state: %v", err)
	}
	batch.Meta.SuccessJobState = successJobState
	completeJobState, err := m.rclient.Get(m.getCompleteJobStateKey(batch.Id)).Result()
	if err == redis.Nil {
		completeJobState = CallbackJobPending
	} else if err != nil {
		return fmt.Errorf("init: get completed job state: %v", err)
	}
	batch.Meta.CompleteJobState = completeJobState

	return nil
}

func (m *batchManager) commit(batch *batch) error {
	if err := m.updateCommitted(batch, true); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(batch, map[string]bool{batch.Id: true})
	}
	return nil
}

func (m *batchManager) open(batch *batch) error {
	if m.areBatchJobsCompleted(batch) {
		return fmt.Errorf("open: batch job (%s) has already completed", batch.Id)
	}
	if err := m.updateCommitted(batch, false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobQueued(batch *batch) error {
	if err := m.addJobToBatch(batch); err != nil {
		return fmt.Errorf("jobQueued: add job to batch: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobFinished(batch *batch, jobId string, success bool, isRetry bool) error {
	if err := m.removeJobFromBatch(batch, jobId, success, isRetry); err != nil {
		return fmt.Errorf("jobFinished: %v", err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(batch, map[string]bool{batch.Id: true})
	}
	return nil
}

func (m *batchManager) handleCallbackJobSucceeded(batch *batch, callbackType string) error {
	if err := m.updateJobCallbackState(batch, callbackType, CallbackJobSucceeded); err != nil {
		return fmt.Errorf("callbackJobSucceeded: update callback job state: %v", err)
	}
	return nil
}

func (m *batchManager) remove(batch *batch) error {
	if err := m.rclient.SRem("batches", batch.Id).Err(); err != nil {
		return fmt.Errorf("remove: batch (%s) %v", batch.Id, err)
	}
	if err := m.rclient.Del(m.getMetaKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch meta (%s) %v", batch.Id, err)
	}
	if err := m.rclient.Del(m.getParentsKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch parents (%s), %v", batch.Id, err)
	}
	if err := m.rclient.Del(m.getChildKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch children (%s), %v", batch.Id, err)
	}
	return nil
}

func (m *batchManager) updateCommitted(batch *batch, committed bool) error {
	batch.Meta.Committed = committed
	if err := m.rclient.HSet(m.getMetaKey(batch.Id), "committed", committed).Err(); err != nil {
		return fmt.Errorf("updateCommitted: could not update committed: %v", err)
	}

	if committed {
		// number of days a batch can exist
		if err := m.rclient.Expire(m.getBatchKey(batch.Id), time.Duration(m.Subsystem.Options.CommittedTimeoutDays)*time.Hour*24).Err(); err != nil {
			return fmt.Errorf("updatedCommitted: could not not expire after committed: %v", err)
		}
	} else {
		if err := m.rclient.Expire(m.getBatchKey(batch.Id), time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes)*time.Minute).Err(); err != nil {
			return fmt.Errorf("updatedCommitted: could not expire: %v", err)
		}
	}
	return nil
}

func (m *batchManager) updateJobCallbackState(batch *batch, callbackType string, state string) error {
	// locking must be handled outside of call
	timeout := time.Duration(m.Subsystem.Options.CommittedTimeoutDays) * 24 * time.Hour
	if callbackType == "success" {
		batch.Meta.SuccessJobState = state
		if err := m.rclient.Set(m.getSuccessJobStateKey(batch.Id), state, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set success_st: %v", err)
		}
		if state == CallbackJobSucceeded {
			m.removeBatch(batch)
		}
	} else {
		batch.Meta.CompleteJobState = state
		if err := m.rclient.Set(m.getCompleteJobStateKey(batch.Id), state, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set completed_st: %v", err)
		}
		if _, areChildrenSucceeded := m.areChildrenFinished(batch); areChildrenSucceeded && batch.Meta.SuccessJob == "" && state == CallbackJobSucceeded {
			m.removeBatch(batch)
		}
	}
	return nil
}

func (m *batchManager) addJobToBatch(batch *batch) error {
	batch.Meta.Total++
	if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "total", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify total: %v", err)
	}
	batch.Meta.Pending++
	if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "pending", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify pending: %v", err)
	}
	return nil
}

func (m *batchManager) removeJobFromBatch(batch *batch, jobId string, success bool, isRetry bool) error {
	if !isRetry {
		batch.Meta.Pending--
		if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "pending", -1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify pending: %v", err)
		}

	}
	if success {
		batch.Meta.Succeeded++
		if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "succeeded", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify succeeded: %v", err)
		}
	} else {
		batch.Meta.Failed++
		if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "failed", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify failed: %v", err)
		}
	}
	return nil
}

func (m *batchManager) areBatchJobsCompleted(batch *batch) bool {
	return batch.Meta.Committed && batch.Meta.Pending == 0
}

func (m *batchManager) areBatchJobsSucceeded(batch *batch) bool {
	return batch.Meta.Committed && batch.Meta.Succeeded == batch.Meta.Total
}

func (m *batchManager) handleBatchJobsCompleted(batch *batch, parentsVisited map[string]bool) {
	areChildrenFinished, areChildrenSucceeded := m.areChildrenFinished(batch)
	if areChildrenFinished {
		util.Debugf("batch: %s children are finished", batch.Id)
		m.handleBatchCompleted(batch, areChildrenSucceeded)
	}
	// notify parents child is done
	for _, parent := range batch.Parents {
		if parentsVisited[parent.Id] {
			// parent has already been notified
			continue
		}
		parent.mu.Lock()
		parentsVisited[parent.Id] = true
		m.handleChildComplete(parent, batch, areChildrenFinished, areChildrenSucceeded, parentsVisited)
		parent.mu.Unlock()
	}
}

func (m *batchManager) handleBatchCompleted(batch *batch, areChildrenSucceeded bool) {
	// only create callback jobs if searched children are completed
	if batch.Meta.CompleteJob != "" && batch.Meta.CompleteJobState == CallbackJobPending {
		m.queueBatchDoneJob(batch, batch.Meta.CompleteJob, "complete")
	}
	if areChildrenSucceeded && batch.Meta.Succeeded == batch.Meta.Total && batch.Meta.SuccessJob != "" && batch.Meta.SuccessJobState == CallbackJobPending {
		m.queueBatchDoneJob(batch, batch.Meta.SuccessJob, "success")
	}
	if areChildrenSucceeded {
		m.removeChildren(batch)
	}
}

func (m *batchManager) queueBatchDoneJob(batch *batch, jobData string, callbackType string) {
	var job client.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		util.Warnf("queueBatchDoneJob: unmarshal job(%s): %v", callbackType, err)
		return
	}
	if job.Jid == "" {
		job.Jid = fmt.Sprintf("%s-%s", batch.Id, callbackType)
	}
	// these are required to update the call back job state
	job.SetCustom("_bid", batch.Id)
	job.SetCustom("_cb", callbackType)
	if err := m.Subsystem.Server.Manager().Push(&job); err != nil {
		util.Warnf("queueBatchDoneJob: cannot push job (%s) %v", callbackType, err)
		return
	}
	if err := m.updateJobCallbackState(batch, callbackType, CallbackJobQueued); err != nil {
		util.Warnf("queueBatchDoneJob: could not update job callback state: %v", err)
	}
	util.Infof("Pushed %s job (jid: %s)", callbackType, job.Jid)
}
