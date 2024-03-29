package cron

import (
	"context"
	"encoding/json"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var _ cron.Job = &QueueJob{}

// QueueJob - enqueues a faktory job
type QueueJob struct {
	Subsystem *CronSubsystem
	job       *CronJob
}

// Run - creates a new faktory job and enqueues it
func (c *QueueJob) Run() {
	ctx := context.Background()

	var job client.Job
	err := json.Unmarshal(c.job.faktoryJob, &job)
	if err != nil {
		util.Warnf("parseCronJob: unable to unmarshal job: %v", err)
		return
	}
	// assign new job id
	job.Jid = uuid.NewString()
	job.CreatedAt = util.Nows()
	// job has already been validated as a proper job on startup
	if err := c.Subsystem.Server.Manager().Push(ctx, &job); err != nil {
		util.Warnf("Execute: unable to queue job (%s): %v", c.job.Name, err)
		return
	}
	util.Debugf("Queueing job: %s", c.job.Name)
}
