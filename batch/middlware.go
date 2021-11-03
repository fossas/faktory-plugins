package batch

import (
	"context"
	"fmt"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

// Fetch - wraps around Fetch (which retrieves a job)
// faktory does not expose the workerId (Wid) in middleware
// workerId is needed when a worker tries to re-open a batch
func (b *BatchSubsystem) Fetch(ctx context.Context, wid string, queues ...string) (manager.Lease, error) {
	lease, err := b.Fetcher.Fetch(ctx, wid, queues...)
	if err == nil && lease != manager.Nothing {
		job, err := lease.Job()
		if err == nil && job != nil {
			if bid, ok := job.GetCustom("bid"); ok {
				batch, err := b.getBatchFromInterface(bid)
				if err != nil {
					return nil, fmt.Errorf("fetch: unable to retrieve batch %s", bid)
				}
				batch.setWorkerForJid(job.Jid, wid)
				util.Infof("Added worker %s for job %s to %s", wid, job.Jid, batch.Id)
			}
		}

	}
	return lease, err
}

func (b *BatchSubsystem) pushMiddleware(next func() error, ctx manager.Context) error {
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("pushMiddleware: unable to get batch %s", bid)
		}
		if err := batch.jobQueued(ctx.Job().Jid); err != nil {
			util.Warnf("unable to add batch %v", err)
			return fmt.Errorf("pushMiddleware: Unable to add job %s to batch %s", ctx.Job().Jid, bid)
		}
		util.Infof("Added %s to batch %s", ctx.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubsystem) fetchMiddleware(next func() error, ctx manager.Context) error {
	middlewareErr := next() // runs the rest of the middleware
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("fetchMiddleware: unable to retrieve batch %s", bid)
		}
		if middlewareErr != nil {
			// clear the worker id for a job since the worker id was added in the custom fetcher
			batch.removeWorkerForJid(ctx.Job().Jid)
		}
	}

	return middlewareErr
}

func (b *BatchSubsystem) handleJobFinished(success bool) func(next func() error, ctx manager.Context) error {
	return func(next func() error, ctx manager.Context) error {
		if success {
			// check if this is a success / complete job from batch
			if bid, ok := ctx.Job().GetCustom("_bid"); ok {
				batch, err := b.getBatchFromInterface(bid)
				if err != nil {
					util.Warnf("Unable to retrieve batch %s: %v", bid, err)
					return next()
				}
				cb, ok := ctx.Job().GetCustom("_cb")
				if !ok {
					util.Warnf("Batch (%s) callback job (%s) does not have _cb specified", bid, ctx.Job().Type)
					return next()
				}
				callbackType, ok := cb.(string)
				if !ok {
					util.Warnf("Error converting callback job type %s", cb)
					return next()
				}
				if err := batch.callbackJobSucceeded(callbackType); err != nil {
					util.Warnf("Unable to update batch")
				}
				return next()
			}
		}
		if bid, ok := ctx.Job().GetCustom("bid"); ok {
			batch, err := b.getBatchFromInterface(bid)
			if err != nil {
				return fmt.Errorf("handleJobFinished: unable to retrieve batch %s", bid)
			}

			status := "succeeded"
			if !success {
				status = "failed"
			}
			util.Infof("Job %s (worker %s) %s for batch %s", ctx.Job().Jid, ctx.Reservation().Wid, status, batch.Id)
			batch.removeWorkerForJid(ctx.Job().Jid)

			isRetry := ctx.Job().Failure != nil && ctx.Job().Failure.RetryCount > 0

			if err := batch.jobFinished(ctx.Job().Jid, success, isRetry); err != nil {
				util.Warnf("error processing finished job for batch %v", err)
				return fmt.Errorf("handleJobFinished: unable to process finished job %s for batch %s", ctx.Job().Jid, batch.Id)
			}

		}
		return next()
	}
}
