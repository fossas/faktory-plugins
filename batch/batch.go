package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
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
	mu        sync.Mutex // this lock is only used for to lock access to Batches
}

const (
	// CallbackJobPending no status
	CallbackJobPending = ""
	// CallbackJobQueued callback job has been queued
	CallbackJobQueued = "1"
	// CallbackJobSucceeded callback job has succeeded
	CallbackJobSucceeded = "2"
)

func (m *batchManager) getBatchIdFromInterface(batchId interface{}) (string, error) {
	bid, ok := batchId.(string)
	if !ok {
		return "", errors.New("getBatchIdFromInterface: invalid custom bid value")
	}
	return bid, nil
}

func (m *batchManager) loadExistingBatches(ctx context.Context) error {
	vals, err := m.rclient.SMembers(ctx, "batches").Result()
	if err != nil {
		return fmt.Errorf("loadExistingBatches: retrieve batches: %v", err)
	}
	for idx := range vals {
		m.loadBatch(ctx, vals[idx])
	}

	// update parent and children
	for _, b := range m.Batches {
		parentIds, err := m.rclient.SMembers(ctx, m.getParentsKey(b.Id)).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, parentId := range parentIds {
			parentBatch, err := m.getBatch(ctx, parentId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting parent batch (%s) %v", parentId, err)
				continue
			}
			b.Parents = append(b.Parents, parentBatch)
		}

		childIds, err := m.rclient.SMembers(ctx, m.getChildKey(b.Id)).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, childId := range childIds {
			childBatch, err := m.getBatch(ctx, childId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting child batch (%s) %v", childId, err)
				continue
			}
			b.Children = append(b.Children, childBatch)
		}

		if m.areBatchJobsCompleted(b) {
			m.handleBatchJobsCompleted(ctx, b, map[string]bool{b.Id: true})
		}
	}
	util.Infof("Loaded %d batches", len(m.Batches))
	return nil
}

func (m *batchManager) lockBatchIfExists(batchId string) {
	m.mu.Lock()
	batchToLock, ok := m.Batches[batchId]
	if !ok {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	batchToLock.mu.Lock()
}

func (m *batchManager) unlockBatchIfExists(batchId string) {
	m.mu.Lock()
	batchToLock, ok := m.Batches[batchId]
	if !ok {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	batchToLock.mu.Unlock()
}

func (m *batchManager) getBatch(ctx context.Context, batchId string) (*batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if batchId == "" {
		return nil, fmt.Errorf("getBatch: batchId cannot be blank")
	}

	b, ok := m.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("getBatch: no batch found")
	}

	exists, err := m.rclient.Exists(ctx, m.getBatchKey(b.Id)).Result()
	if err != nil {
		util.Warnf("Cannot confirm batch exists: %v", err)
		return nil, fmt.Errorf("getBatch: unable to check if batch has timed out")
	}
	if exists == 0 {
		m.removeBatch(ctx, b)
		return nil, fmt.Errorf("getBatch: batch has timed out")
	}
	return b, nil
}

func (m *batchManager) removeBatch(ctx context.Context, batch *batch) {
	// locking must be handled outside the function
	if err := m.remove(ctx, batch); err != nil {
		util.Warnf("removeBatch: unable to remove batch: %v", err)
	}
	delete(m.Batches, batch.Id)

	batch = nil
}

func (m *batchManager) removeStaleBatches(ctx context.Context) {
	// in order to avoid dead locks
	// Step 1 create a list of batches to delete
	// Step 2 take a lock on each batch
	//   - this ensures we wait for any operations on a batch to finish
	// Step 3 lock access to any batch
	//   - this way no other locks can be taken
	// Step 4 delete batches
	util.Infof("checking for stale batches")
	var batchesToRemove []string
	// Step 1
	for _, b := range m.Batches {
		remove := false
		if b.Meta.CreatedAt != "" {
			createdAt, err := time.Parse(time.RFC3339Nano, b.Meta.CreatedAt)
			if err != nil {
				continue
			}
			uncommittedTimeout := time.Now().Add(-time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute).UTC()
			committedTimeout := time.Now().AddDate(0, 0, -m.Subsystem.Options.CommittedTimeoutDays).UTC()
			if !b.Meta.Committed && createdAt.Before(uncommittedTimeout) {
				remove = true
			} else if b.Meta.Committed && createdAt.Before(committedTimeout) {
				remove = true
			}
		} else {
			remove = true
		}

		if remove {
			batchesToRemove = append(batchesToRemove, b.Id)
		}
	}
	// Step 2 - lock each batch
	for _, batchId := range batchesToRemove {
		if b, ok := m.Batches[batchId]; ok {
			b.mu.Lock()
		}
	}
	// Step 3 - lock access to all batches
	m.mu.Lock()
	defer m.mu.Unlock()

	// Step 4 - delete batches and unlock (in case another goroutines is waiting on a lock)
	for _, batchId := range batchesToRemove {
		func() {
			if b, ok := m.Batches[batchId]; ok {
				defer b.mu.Unlock()
				m.removeBatch(ctx, b)
			}
		}()
	}
	util.Infof("Removed: %d stale batches", len(batchesToRemove))
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

func (m *batchManager) loadBatch(ctx context.Context, batchId string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch := &batch{
		Id:       batchId,
		Parents:  make([]*batch, 0),
		Children: make([]*batch, 0),
		Meta:     &batchMeta{},
		mu:       sync.Mutex{},
	}

	if err := m.loadMetadata(ctx, batch); err != nil {
		util.Warnf("loadExistingBatches: error load batch (%s) %v", batchId, err)
		m.remove(ctx, batch)
		return
	}
	m.Batches[batchId] = batch
}

func (m *batchManager) newBatch(ctx context.Context, batchId string, meta *batchMeta) (*batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := &batch{
		Id:       batchId,
		Parents:  make([]*batch, 0),
		Children: make([]*batch, 0),
		Meta:     meta,
		mu:       sync.Mutex{},
	}
	if err := m.init(ctx, b); err != nil {
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

func (m *batchManager) loadMetadata(ctx context.Context, batch *batch) error {
	meta, err := m.rclient.HGetAll(ctx, m.getMetaKey(batch.Id)).Result()
	if err != nil {
		return fmt.Errorf("init: unable to retrieve meta: %v", err)
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
	successJobState, err := m.rclient.Get(ctx, m.getSuccessJobStateKey(batch.Id)).Result()
	if err == redis.Nil {
		successJobState = CallbackJobPending
	} else if err != nil {
		return fmt.Errorf("init: get success job state: %v", err)
	}
	batch.Meta.SuccessJobState = successJobState
	completeJobState, err := m.rclient.Get(ctx, m.getCompleteJobStateKey(batch.Id)).Result()
	if err == redis.Nil {
		completeJobState = CallbackJobPending
	} else if err != nil {
		return fmt.Errorf("init: get completed job state: %v", err)
	}
	batch.Meta.CompleteJobState = completeJobState

	return nil
}

func (m *batchManager) init(ctx context.Context, batch *batch) error {
	if err := m.rclient.SAdd(ctx, "batches", batch.Id).Err(); err != nil {
		return fmt.Errorf("init: store batch: %v", err)
	}

	expiration := time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute
	if err := m.rclient.SetNX(ctx, m.getBatchKey(batch.Id), batch.Id, expiration).Err(); err != nil {
		return fmt.Errorf("init: set expiration: %v", err)
	}

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
	if err := m.rclient.HMSet(ctx, m.getMetaKey(batch.Id), data).Err(); err != nil {
		return fmt.Errorf("init: could not load meta for batch: %s: %v", batch.Id, err)
	}
	if err := m.rclient.Expire(ctx, m.getMetaKey(batch.Id), expiration).Err(); err != nil {
		return fmt.Errorf("init: could set expiration for batch meta: %v", err)
	}
	if err := m.rclient.SetNX(ctx, m.getSuccessJobStateKey(batch.Id), CallbackJobPending, expiration).Err(); err != nil {
		return fmt.Errorf("init: could not set success_st: %v", err)
	}
	if err := m.rclient.SetNX(ctx, m.getCompleteJobStateKey(batch.Id), CallbackJobPending, expiration).Err(); err != nil {
		return fmt.Errorf("init: could not set complete_st: %v", err)
	}

	return nil
}

func (m *batchManager) commit(ctx context.Context, batch *batch) error {
	if err := m.updateCommitted(ctx, batch, true); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(ctx, batch, map[string]bool{batch.Id: true})
	}
	return nil
}

func (m *batchManager) open(ctx context.Context, batch *batch) error {
	if m.areBatchJobsCompleted(batch) {
		return fmt.Errorf("open: batch job (%s) has already completed", batch.Id)
	}
	if err := m.updateCommitted(ctx, batch, false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobQueued(ctx context.Context, batch *batch) error {
	if err := m.addJobToBatch(ctx, batch); err != nil {
		return fmt.Errorf("jobQueued: add job to batch: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobFinished(ctx context.Context, batch *batch, jobId string, success bool, isRetry bool) error {
	if err := m.removeJobFromBatch(ctx, batch, jobId, success, isRetry); err != nil {
		return fmt.Errorf("jobFinished: %v", err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(ctx, batch, map[string]bool{batch.Id: true})
	}
	return nil
}

func (m *batchManager) handleCallbackJobSucceeded(ctx context.Context, batch *batch, callbackType string) error {
	if err := m.updateJobCallbackState(ctx, batch, callbackType, CallbackJobSucceeded); err != nil {
		return fmt.Errorf("callbackJobSucceeded: update callback job state: %v", err)
	}
	return nil
}

func (m *batchManager) remove(ctx context.Context, batch *batch) error {
	if err := m.rclient.SRem(ctx, "batches", batch.Id).Err(); err != nil {
		return fmt.Errorf("remove: batch (%s) %v", batch.Id, err)
	}
	if err := m.rclient.Del(ctx, m.getMetaKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch meta (%s) %v", batch.Id, err)
	}
	if err := m.rclient.Del(ctx, m.getParentsKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch parents (%s), %v", batch.Id, err)
	}
	if err := m.rclient.Del(ctx, m.getChildKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch children (%s), %v", batch.Id, err)
	}
	if err := m.rclient.Del(ctx, m.getSuccessJobStateKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: could delete expire success_st: %v", err)
	}
	if err := m.rclient.Del(ctx, m.getCompleteJobStateKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("updatedCommitted: could not deletecomplete_st: %v", err)
	}
	return nil
}

func (m *batchManager) updateCommitted(ctx context.Context, batch *batch, committed bool) error {
	batch.Meta.Committed = committed
	if err := m.rclient.HSet(ctx, m.getMetaKey(batch.Id), "committed", committed).Err(); err != nil {
		return fmt.Errorf("updateCommitted: could not update committed: %v", err)
	}
	var expiration time.Duration
	if committed {
		expiration = time.Duration(m.Subsystem.Options.CommittedTimeoutDays) * time.Hour * 24
	} else {
		expiration = time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute
	}
	if err := m.rclient.Expire(ctx, m.getBatchKey(batch.Id), expiration).Err(); err != nil {
		return fmt.Errorf("updatedCommitted: could not set expire for batch: %v", err)
	}
	if err := m.rclient.Expire(ctx, m.getMetaKey(batch.Id), expiration).Err(); err != nil {
		return fmt.Errorf("updatedCommitted: could not set expire for batch meta: %v", err)
	}
	if err := m.rclient.Expire(ctx, m.getSuccessJobStateKey(batch.Id), expiration).Err(); err != nil {
		return fmt.Errorf("updatedCommitted: could not set expire success_st: %v", err)
	}
	if err := m.rclient.Expire(ctx, m.getCompleteJobStateKey(batch.Id), expiration).Err(); err != nil {
		return fmt.Errorf("updatedCommitted: could not set expire for complete_st: %v", err)
	}

	return nil
}

func (m *batchManager) updateJobCallbackState(ctx context.Context, batch *batch, callbackType string, state string) error {
	// locking must be handled outside of call
	expire := time.Duration(m.Subsystem.Options.CommittedTimeoutDays) * 24 * time.Hour
	if callbackType == "success" {
		batch.Meta.SuccessJobState = state
		if err := m.rclient.Set(ctx, m.getSuccessJobStateKey(batch.Id), state, expire).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set success_st: %v", err)
		}
		if state == CallbackJobSucceeded {
			func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.removeBatch(ctx, batch)
			}()
		}
	} else {
		batch.Meta.CompleteJobState = state
		if err := m.rclient.Set(ctx, m.getCompleteJobStateKey(batch.Id), state, expire).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set completed_st: %v", err)
		}
		if _, areChildrenSucceeded := m.areChildrenFinished(batch); areChildrenSucceeded && batch.Meta.SuccessJob == "" && state == CallbackJobSucceeded {
			func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.removeBatch(ctx, batch)
			}()
		}
	}
	return nil
}

func (m *batchManager) addJobToBatch(ctx context.Context, batch *batch) error {
	batch.Meta.Total++
	if err := m.rclient.HIncrBy(ctx, m.getMetaKey(batch.Id), "total", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify total: %v", err)
	}
	batch.Meta.Pending++
	if err := m.rclient.HIncrBy(ctx, m.getMetaKey(batch.Id), "pending", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify pending: %v", err)
	}
	return nil
}

func (m *batchManager) removeJobFromBatch(ctx context.Context, batch *batch, jobId string, success bool, isRetry bool) error {
	if !isRetry {
		batch.Meta.Pending--
		if err := m.rclient.HIncrBy(ctx, m.getMetaKey(batch.Id), "pending", -1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify pending: %v", err)
		}

	}
	if success {
		batch.Meta.Succeeded++
		if err := m.rclient.HIncrBy(ctx, m.getMetaKey(batch.Id), "succeeded", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify succeeded: %v", err)
		}
	} else {
		batch.Meta.Failed++
		if err := m.rclient.HIncrBy(ctx, m.getMetaKey(batch.Id), "failed", 1).Err(); err != nil {
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

func (m *batchManager) handleBatchJobsCompleted(ctx context.Context, batch *batch, parentsVisited map[string]bool) {
	areChildrenFinished, areChildrenSucceeded := m.areChildrenFinished(batch)
	if areChildrenFinished {
		util.Debugf("batch: %s children are finished", batch.Id)
		m.handleBatchCompleted(ctx, batch, areChildrenSucceeded)
	}
	// notify parents child is done
	for _, parent := range batch.Parents {
		if parentsVisited[parent.Id] {
			// parent has already been notified
			continue
		}
		m.lockBatchIfExists(parent.Id)
		parentsVisited[parent.Id] = true
		m.mu.Lock()
		if _, ok := m.Batches[parent.Id]; !ok {
			if err := m.removeParent(ctx, batch, parent); err != nil {
				util.Warnf("handleBatchJobsCompleted: unable to delete parent: %v", err)
			}
			m.mu.Unlock()
			m.unlockBatchIfExists(parent.Id)
			continue
		}
		m.mu.Unlock()
		m.handleChildComplete(ctx, parent, batch, areChildrenFinished, areChildrenSucceeded, parentsVisited)
		m.unlockBatchIfExists(parent.Id)
	}
}

func (m *batchManager) handleBatchCompleted(ctx context.Context, batch *batch, areChildrenSucceeded bool) {
	// only create callback jobs if searched children are completed
	if batch.Meta.CompleteJob != "" && batch.Meta.CompleteJobState == CallbackJobPending {
		m.queueBatchDoneJob(ctx, batch, batch.Meta.CompleteJob, "complete")
	}
	if areChildrenSucceeded && batch.Meta.Succeeded == batch.Meta.Total && batch.Meta.SuccessJob != "" && batch.Meta.SuccessJobState == CallbackJobPending {
		m.queueBatchDoneJob(ctx, batch, batch.Meta.SuccessJob, "success")
	}
	if areChildrenSucceeded {
		m.removeChildren(ctx, batch)
	}
}

func (m *batchManager) queueBatchDoneJob(ctx context.Context, batch *batch, jobData string, callbackType string) {
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
	if err := m.Subsystem.Server.Manager().Push(ctx, &job); err != nil {
		util.Warnf("queueBatchDoneJob: cannot push job (%s) %v", callbackType, err)
		return
	}
	if err := m.updateJobCallbackState(ctx, batch, callbackType, CallbackJobQueued); err != nil {
		util.Warnf("queueBatchDoneJob: could not update job callback state: %v", err)
	}
	util.Infof("Pushed %s job (jid: %s)", callbackType, job.Jid)
}
