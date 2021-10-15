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

type Batch struct {
	Id          string
	BatchKey    string
	JobsKey     string
	ChildrenKey string
	MetaKey     string
	Children    map[string]*Batch
	Meta        *BatchMeta
	rclient     *redis.Client
	mu          sync.Mutex
	Workers     map[string]string
	Server      *server.Server
}

type BatchMeta struct {
	ParentBid   string
	Total       int
	Failed      int
	Succeeded   int
	CreatedAt   string
	Description string
	Committed   bool
	SuccessJob  string
	CompleteJob string
	CanOpen     bool
}

func (b *Batch) init() error {
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
			"parent_bid":   b.Meta.ParentBid,
			"success_job":  b.Meta.SuccessJob,
			"complete_job": b.Meta.CompleteJob,
			"can_open":     b.Meta.CanOpen,
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
	b.Meta.ParentBid = meta["parent_bid"]
	b.Meta.CreatedAt = meta["created_at"]
	b.Meta.SuccessJob = meta["success_job"]
	b.Meta.CompleteJob = meta["complete_job"]

	return nil
}

func (b *Batch) commit() {
	b.mu.Lock()
	b.updateCommited(true)
	b.mu.Unlock()
}

func (b *Batch) open() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isBatchDone() {
		return errors.New("Batch job has already finished, cannot open a new batch")
	}
	b.updateCommited(false)
	return nil
}

func (b *Batch) jobQueued(jobId string) {
	b.mu.Lock()
	b.addJobToBatch(jobId)
	b.mu.Unlock()
}

func (b *Batch) jobFinished(jobId string, success bool) {
	b.mu.Lock()
	b.removeJobFromBatch(jobId, success)
	b.mu.Unlock()
}

func (b *Batch) setWorkerForJid(jid string, wid string) {
	b.mu.Lock()
	b.Workers[jid] = wid
	b.mu.Unlock()
}

func (b *Batch) removeWorkerForJid(jid string) {
	b.mu.Lock()
	delete(b.Workers, jid)
	b.mu.Unlock()
}

func (b *Batch) hasWorker(wid string) bool {
	for _, worker := range b.Workers {
		if worker == wid {
			return true
		}
	}
	return false
}

func (b *Batch) remove() {
	b.mu.Lock()
	b.rclient.HDel(b.MetaKey)
	b.mu.Unlock()
}

func (b *Batch) updateCommited(commited bool) {
	b.Meta.Committed = commited
	b.rclient.HSet(b.MetaKey, "commited", commited)
	if commited {
		b.checkBatchDone()
	}
}

func (b *Batch) addJobToBatch(jobId string) {
	b.rclient.SAdd(b.JobsKey, jobId)
	b.rclient.HIncrBy(b.MetaKey, "total", 1)
	b.Meta.Total += 1
}

func (b *Batch) removeJobFromBatch(jobId string, success bool) {
	b.rclient.SRem(b.JobsKey, jobId)
	if success {
		b.Meta.Succeeded += 1
		b.rclient.HIncrBy(b.MetaKey, "succeeded", 1)
	} else {
		b.Meta.Failed += 1
		b.rclient.HIncrBy(b.MetaKey, "failed", 1)
	}
	b.checkBatchDone()
}

func (b *Batch) isBatchDone() bool {
	totalFinished := b.Meta.Succeeded + b.Meta.Failed
	// TODO: check child jobs
	return b.Meta.Committed == true && totalFinished == b.Meta.Total
}

func (b *Batch) checkBatchDone() {
	if b.isBatchDone() {
		if b.Meta.Succeeded == b.Meta.Total && b.Meta.SuccessJob != "" {
			b.queueBatchDoneJob(b.Meta.SuccessJob, "success")
		}

		if b.Meta.CompleteJob != "" {
			b.queueBatchDoneJob(b.Meta.CompleteJob, "complete")

		}
	}
}

func (b *Batch) queueBatchDoneJob(jobData string, jobType string) {
	var job client.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		util.Warnf("Cannot unmarshal complete job %w", err)
		return
	}
	job.Jid = fmt.Sprintf("%s-%s", b.Id, jobType)
	if err := b.Server.Manager().Push(&job); err != nil {
		util.Warnf("Cannot push job %w", err)
		return
	}
	util.Infof("Pushed job %+v", job)
}
