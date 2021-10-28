package cron

import (
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/robfig/cron/v3"
)

var _ cron.Job = &QueueJob{}

// QueueJob - enqueues a faktory job
type QueueJob struct {
	Subsystem *CronSubsystem
	job       *CronJob
}

// Runs - creates a new faktory job and enqueues it
func (c *QueueJob) Run() {
	var job client.Job
	time.Sleep(45 * time.Second)
	err := json.Unmarshal(c.job.faktoryJob, &job)
	if err != nil {
		util.Warnf("parseCronJob: unable to unmarshal job: %v", err)
		return
	}
	// assign new job id
	job.Jid = client.RandomJid()
	job.CreatedAt = util.Nows()
	// job has already been validated as a proper job on startup
	if err := c.Subsystem.Server.Manager().Push(&job); err != nil {
		util.Warnf("Execute: unable to queue job (%s): %v", c.job.Name, err)
	}
	util.Infof("Queueing job: %s", c.job.Name)
	return
}
