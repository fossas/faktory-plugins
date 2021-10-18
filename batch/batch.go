package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

type batch struct {
	Id       string
	BatchKey string
	JobsKey  string
	MetaKey  string
	Meta     *batchMeta
	rclient  *redis.Client
	mu       sync.Mutex
	Workers  map[string]string
	Server   *server.Server
}

const (
	// no status
	CallbackJobPending = ""
	// callback job has been queued
	CallbackJobQueued = "1"
	// callback job has succeeded
	CallbackJobSucceeded = "2"
)

type batchMeta struct {
	Total            int
	Failed           int
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
	meta, err := b.rclient.HGetAll(b.MetaKey).Result()
	if err != nil {
		return nil
	}

	if len(meta) == 0 {
		// set default values
		data := map[string]interface{}{
			"total":        b.Meta.Total,
			"failed":       b.Meta.Failed,
			"succeeded":    b.Meta.Succeeded,
			"created_at":   b.Meta.CreatedAt,
			"description":  b.Meta.Description,
			"committed":    b.Meta.Committed,
			"success_job":  b.Meta.SuccessJob,
			"complete_job": b.Meta.CompleteJob,
			"success_st":   b.Meta.SuccessJobState,
			"complete_st":  b.Meta.CompleteJobState,
		}
		b.rclient.HMSet(b.MetaKey, data)
		return nil
	}

	b.Meta.Total, err = strconv.Atoi(meta["total"])
	if err != nil {
		return fmt.Errorf("failed converting string to int: %v", err)
	}

	b.Meta.Failed, err = strconv.Atoi(meta["failed"])
	if err != nil {
		return fmt.Errorf("failed converting string to int: %v", err)
	}
	b.Meta.Succeeded, err = strconv.Atoi(meta["succeeded"])
	if err != nil {
		return fmt.Errorf("failed converting string to int: %v", err)
	}
	b.Meta.Committed, err = strconv.ParseBool(meta["committed"])
	if err != nil {
		return fmt.Errorf("failed converting string to bool: %v", err)
	}
	b.Meta.Description = meta["description"]
	b.Meta.CreatedAt = meta["created_at"]
	b.Meta.SuccessJob = meta["success_job"]
	b.Meta.CompleteJob = meta["complete_job"]
	b.Meta.SuccessJobState = meta["success_st"]
	b.Meta.CompleteJobState = meta["complete_st"]

	return nil
}

func (b *batch) commit() error {
	b.mu.Lock()
	if err := b.updateCommited(true); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) open() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isBatchDone() {
		return errors.New("batch job has already finished.")
	}
	if err := b.updateCommited(false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (b *batch) jobQueued(jobId string) error {
	b.mu.Lock()
	if err := b.addJobToBatch(jobId); err != nil {
		return fmt.Errorf("add job to batch: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) jobFinished(jobId string, success bool) error {
	b.mu.Lock()
	if err := b.removeJobFromBatch(jobId, success); err != nil {
		return fmt.Errorf("job finished: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) callbackJobSucceded(callbackType string) error {
	b.mu.Lock()
	if err := b.updateJobCallbackState(callbackType, CallbackJobSucceeded); err != nil {
		return fmt.Errorf("update callback job state: %v", err)
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
	if err := b.rclient.HDel(b.MetaKey).Err(); err != nil {
		return fmt.Errorf("remove batch data: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *batch) updateCommited(commited bool) error {
	b.Meta.Committed = commited
	if err := b.rclient.HSet(b.MetaKey, "commited", commited).Err(); err != nil {
		return fmt.Errorf("%v", err)
	}
	if commited {
		b.checkBatchDone()
	}
	return nil
}

func (b *batch) updateJobCallbackState(callbackType string, state string) error {
	if callbackType == "success" {
		b.Meta.SuccessJobState = state
		if err := b.rclient.HSet(b.MetaKey, "succes_st", state).Err(); err != nil {
			return err
		}
	} else {
		b.Meta.CompleteJobState = state
		if err := b.rclient.HSet(b.MetaKey, "completed_st", state).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (b *batch) addJobToBatch(jobId string) error {
	if err := b.rclient.SAdd(b.JobsKey, jobId).Err(); err != nil {
		return fmt.Errorf("add job to batch: %v", err)
	}
	if err := b.rclient.HIncrBy(b.MetaKey, "total", 1).Err(); err != nil {
		return fmt.Errorf("increase job total: %v", err)
	}
	b.Meta.Total += 1
	return nil
}

func (b *batch) removeJobFromBatch(jobId string, success bool) error {
	if err := b.rclient.SRem(b.JobsKey, jobId).Err(); err != nil {
		return fmt.Errorf("remove job from batch: %v", err)
	}
	if success {
		b.Meta.Succeeded += 1
		b.rclient.HIncrBy(b.MetaKey, "succeeded", 1)
	} else {
		b.Meta.Failed += 1
		b.rclient.HIncrBy(b.MetaKey, "failed", 1)
	}
	b.checkBatchDone()
	return nil
}

func (b *batch) isBatchDone() bool {
	totalFinished := b.Meta.Succeeded + b.Meta.Failed
	// TODO: check child jobs
	return b.Meta.Committed == true && totalFinished == b.Meta.Total
}

func (b *batch) checkBatchDone() {
	if b.isBatchDone() {
		if b.Meta.Succeeded == b.Meta.Total && b.Meta.SuccessJob != "" {
			b.queueBatchDoneJob(b.Meta.SuccessJob, "success")
		}
		if b.Meta.CompleteJob != "" {
			b.queueBatchDoneJob(b.Meta.CompleteJob, "complete")

		}
	}
}

func (b *batch) queueBatchDoneJob(jobData string, callbackType string) {
	var job client.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		util.Warnf("unmarshal job(%s): %v", callbackType, err)
		return
	}
	job.Jid = fmt.Sprintf("%s-%s", b.Id, callbackType)
	// these are required to update the call back job state
	job.SetCustom("_bid", b.Id)
	job.SetCustom("_cb", callbackType)
	if err := b.Server.Manager().Push(&job); err != nil {
		util.Warnf("cannot push job(%s) %v", callbackType, err)
		return
	}
	if err := b.updateJobCallbackState(callbackType, CallbackJobQueued); err != nil {
		util.Warnf("Could not update job callback state: %v", err)
	}
	util.Infof("Pushed %s job (jid: %s)", callbackType, job.Jid)
}
