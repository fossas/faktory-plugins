package batch

import (
	"encoding/json"
	"fmt"
	"github.com/contribsys/faktory/client"
	"strconv"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
	"githum.com/go-redis/redis"
)

type batchManager struct {
	Batches   map[string]*batch
	Subsystem *BatchSubsystem
	rclient   *redis.Client
	mu        sync.Mutex
}

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
	vals, err := m.Server.Manager().Redis().SMembers("batches").Result()
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
			b.Parents = append(batch.Parents, parentBatch)
		}

		childIds, err := b.Server.Manager().Redis().SMembers(batch.ChildKey).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, childId := range childIds {
			childBatch, err := b.getBatch(childId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting child batch (%s) %v", childId, err)
				continue
			}
			b.Children = append(b.Children, childBatch)
		}

		if m.areBatchJobsCompleted(b) {
			m.handleBatchJobsCompleted(b)
		}
	}

	return nil
}

func (m *batchManager) getBatch(batchId string) (*batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if batchId == "" {
		return nil, fmt.Errorf("getBatch: batchId cannot be blank")
	}

	b, ok := m.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("getBatch: no batch found")
	}

	exists, err := m.Server.Manager().Redis().Exists(b.BatchKey).Result()
	if err != nil {
		util.Warnf("Cannot confirm batch exists: %v", err)
		return nil, fmt.Errorf("getBatch: unable to check if batch has timed out")
	}
	if exists == 0 {
		b.mu.Lock()
		m.removeBatch(b)
		b.mu.Unlock()
		return nil, fmt.Errorf("getBatch: batch was not committed within 2 hours")
	}

	return b, nil
}

func (m *batchManager) removeBatch(batch *batch) {
	fmt.Println("deleting batch")
	if err := m.remove(batch); err != nil {
		util.Warnf("removeBatch: unable to remove batch: %v", err)
	}
	delete(m.Batches, batch.Id)

	batch = nil
}

func (m *batchManager) removeStaleBatches() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range m.Batches {
		createdAt, err := time.Parse(time.RFC3339Nano, b.Meta.CreatedAt)
		if err != nil {
			continue
		}
		remove := false
		uncomittedTimeout := time.Now().Add(-time.Duration(m.Subsystem.Options.UncommittedTimeoutMinutes) * time.Minute)
		comittedTimeout := time.Now().AddDate(0, 0, -m.Subsystem.Options.CommittedTimeoutDays)
		if !batch.Meta.Committed && createdAt.Before(uncomittedTimeout) {
			remove = true
		} else if batch.Meta.Committed && createdAt.Before(comittedTimeout) {
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
		Description:      description,
		SuccessJobState:  CallbackJobPending,
		CompleteJobState: CallbackJobPending,
		ChildSearchDepth: childSearchDepth,
	}
}

func (m *batchManager) newBatch(batchId string, meta *batchMeta) (*batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch := &batch{
		Id: batchId,

		Parents:  make([]*batch, 0),
		Children: make([]*batch, 0),

		Meta:      meta,
		mu:        sync.Mutex{},
		Subsystem: m.Subsystem,
	}
	if err := batch.init(); err != nil {
		return nil, fmt.Errorf("newBatch: %v", err)
	}

	m.Batches[batchId] = batch

	return batch, nil
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
		}
		if batch.Meta.ChildSearchDepth != nil {
			data["child_search_depth"] = *batch.Meta.ChildSearchDepth
		}
		if err := batch.rclient.HMSet(batch.MetaKey, data).Err(); err != nil {
			return fmt.Errorf("init: could not load meta for batch: %s: %v", batch.Id, err)
		}
		if err := batch.rclient.Expire(batch.MetaKey, expiration).Err(); err != nil {
			return fmt.Errorf("init: could set expiration for batch meta: %v", err)
		}

		timeout := time.Duration(batch.Subsystem.Options.CommittedTimeoutDays) * 24 * time.Hour
		if err := batch.rclient.SetNX(batch.SuccessJobStateKey, CallbackJobPending, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set success_st: %v", err)
		}
		if err := batch.rclient.SetNX(batch.CompleteJobStateKey, CallbackJobPending, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set complete_st: %v", err)
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
	successJobState, err := batch.rclient.Get(batch.SuccessJobStateKey).Result()
	if err == redis.Nil {
		successJobState = CallbackJobPending
	} else if err != nil {
		return fmt.Errorf("init: get success job state: %v", err)
	}
	batch.Meta.SuccessJobState = successJobState
	completeJobState, err := batch.rclient.Get(batch.CompleteJobStateKey).Result()
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
		m.handleBatchJobsCompleted(batch)
	}
	return nil
}

func (m *batchManager) open(batch *batch) error {
	if m.areBatchJobsCompleted(batch) {
		return fmt.Errorf("open: batch job (%s) has already completed", m.Id)
	}
	if err := m.updateCommitted(batch, false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobQueued(batch *batch, jobId string) error {
	if err := m.addJobToBatch(batch, jobId); err != nil {
		return fmt.Errorf("jobQueued: add job to batch: %v", err)
	}
	return nil
}

func (m *batchManager) handleJobFinished(batch *batch, jobId string, success bool, isRetry bool) error {
	if err := m.removeJobFromBatch(batch, jobId, success, isRetry); err != nil {
		return fmt.Errorf("jobFinished: %v", err)
	}
	if m.areBatchJobsCompleted(batch) {
		m.handleBatchJobsCompleted(batch)
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
		return fmt.Errorf("remove: batch (%s) %v", m.Id, err)
	}
	if err := m.rclient.Del(m.getMetaKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch meta (%s) %v", m.Id, err)
	}
	if err := m.rclient.Del(m.getParentsKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch parents (%s), %v", m.Id, err)
	}
	if err := m.rclient.Del(m.getChildKey(batch.Id)).Err(); err != nil {
		return fmt.Errorf("remove: batch children (%s), %v", m.Id, err)
	}
	return nil
}

func (m *batchManager) updateCommitted(batch *batch, committed bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	timeout := time.Duration(m.Subsystem.Options.CommittedTimeoutDays) * 24 * time.Hour
	if callbackType == "success" {
		batch.Meta.SuccessJobState = state
		if err := m.rclient.SetNX(batch.SuccessJobStateKey, state, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set success_st: %v", err)
		}
		if state == CallbackJobSucceeded {
			m.Subsystem.removeBatch(batch)
		}
	} else {
		batch.Meta.CompleteJobState = state
		if err := m.rclient.SetNX(m.getSuccessJobStateKey(batch.Id), state, timeout).Err(); err != nil {
			return fmt.Errorf("updateJobCallbackState: could not set completed_st: %v", err)
		}
	}
	return nil
}

func (m *batchManager) addJobToBatch(batch *batch, jobId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch.Meta.Total += 1
	if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "total", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify total: %v", err)
	}
	batch.Meta.Pending += 1
	if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "pending", 1).Err(); err != nil {
		return fmt.Errorf("addJobToBatch: unable to modify pending: %v", err)
	}
	return nil
}

func (m *batchManager) removeJobFromBatch(batch *batch, jobId string, success bool, isRetry bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !isRetry {
		batch.Meta.Pending -= 1
		if err := m.rclient.HIncrBy(m.getMetaKey(batch.Id), "pending", -1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify pending: %v", err)
		}

	}
	if success {
		batch.Meta.Succeeded += 1
		if err := m.rclient.HIncrBy(batch.MetaKey, "succeeded", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify succeeded: %v", err)
		}
	} else {
		batch.Meta.Failed += 1
		if err := m.rclient.HIncrBy(batch.MetaKey, "failed", 1).Err(); err != nil {
			return fmt.Errorf("removeJobFromBatch: unable to modify failed: %v", err)
		}
	}
	return nil
}

func (m *batchManager) areBatchJobsCompleted(batch *batch) bool {
	return batch.Meta.Committed == true && batch.Meta.Pending == 0
}

func (m *batchManager) areBatchJobsSucceeded(batch *batch) bool {
	return batch.Meta.Committed == true && batch.Meta.Succeeded == batch.Meta.Total
}

func (m *batchManager) handleBatchJobsCompleted(batch *batch) {
	visited := map[string]bool{}
	areChildrenFinished, areChildrenSucceeded := m.areChildrenFinished(batch, visited)
	if areChildrenFinished {
		util.Infof("batch: %s children are finished", batch.Id)
		m.handleBatchCompleted(batch, areChildrenSucceeded)
	}
	// notify parents child is done
	for _, parent := range batch.Parents {
		m.handleChildComplete(parent, areChildrenFinished, areChildrenSucceeded, visited)
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
		job.Jid = fmt.Sprintf("%s-%s", m.Id, callbackType)
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
	util.Infof("Pushed %s job (jid: %s)", callbackType, jom.Jid)
}
