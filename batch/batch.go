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

func (b *Batch) commit() error {
	b.mu.Lock()
	if err := b.updateCommited(true); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	b.mu.Unlock()
}

func (b *Batch) open() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isBatchDone() {
		return errors.New("Batch job has already finished, cannot open a new batch")
	}
	if err := b.updateCommited(false); err != nil {
		return fmt.Errorf("open: %v", err)
	}
	return nil
}

func (b *Batch) jobQueued(jobId string) error {
	b.mu.Lock()
	if err := b.addJobToBatch(jobId); err != nil {
		return fmt.Errorf("add job to batch: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *Batch) jobFinished(jobId string, success bool) error {
	b.mu.Lock()
	if err := b.removeJobFromBatch(jobId, success); err != nil {
		return fmt.Errorf("job finished: %v", err)
	}
	b.mu.Unlock()
	return nil
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

func (b *Batch) remove() error {
	b.mu.Lock()
	if err := b.rclient.HDel(b.MetaKey).Err(); err != nil {
		return fmt.Errorf("remove batch data: %v", err)
	}
	b.mu.Unlock()
	return nil
}

func (b *Batch) updateCommited(commited bool) error {
	b.Meta.Committed = commited
	if err := b.rclient.HSet(b.MetaKey, "commited", commited).Err(); err != nil {
		return fmt.Errorf("%v", err)
	}
	if commited {
		b.checkBatchDone()
	}
	return nil
}

func (b *Batch) addJobToBatch(jobId string) error {
	if err := b.rclient.SAdd(b.JobsKey, jobId).Err(); err != nil {
		return fmt.Errorf("add job to batch: %v", err)
	}
	if err := b.rclient.HIncrBy(b.MetaKey, "total", 1); err != nil {
		return fmt.Errorf("increase job total: %v", err)
	}
	b.Meta.Total += 1
	return nil
}

func (b *Batch) removeJobFromBatch(jobId string, success bool) error {
	if err := b.rclient.SRem(b.JobsKey, jobId); err != nil {
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
		util.Warnf("unmarshal job(%s): %v", jobType, err)
		return
	}
	job.Jid = fmt.Sprintf("%s-%s", b.Id, jobType)
	if err := b.Server.Manager().Push(&job); err != nil {
		util.Warnf("cannot push job(%s) %v", jobType, err)
		return
	}
	util.Infof("Pushed %s job (jid: %s)", jobType, job.Jid)
}
