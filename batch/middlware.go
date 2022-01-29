package batch

import (
	"fmt"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

func (b *BatchSubsystem) pushMiddleware(next func() error, ctx manager.Context) error {
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.batchManager.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("pushMiddleware: unable to get batch %s", bid)
		}
		if err := b.batchManager.handleJobQueued(batch, ctx.Job().Jid); err != nil {
			util.Warnf("unable to add batch %v", err)
			return fmt.Errorf("pushMiddleware: Unable to add job %s to batch %s", ctx.Job().Jid, bid)
		}
		util.Infof("Added %s to batch %s", ctx.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubsystem) handleJobFinished(success bool) func(next func() error, ctx manager.Context) error {
	return func(next func() error, ctx manager.Context) error {
		if success {
			// check if this is a success / complete job from batch
			if bid, ok := ctx.Job().GetCustom("_bid"); ok {
				batch, err := b.batchManager.getBatchFromInterface(bid)
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
				if err := b.batchManager.handleCallbackJobSucceeded(batch, callbackType); err != nil {
					util.Warnf("Unable to update batch")
				}
				return next()
			}
		}
		if bid, ok := ctx.Job().GetCustom("bid"); ok {
			batch, err := b.batchManager.getBatchFromInterface(bid)
			if err != nil {
				return fmt.Errorf("handleJobFinished: unable to retrieve batch %s", bid)
			}

			status := "succeeded"
			if !success {
				status = "failed"
			}
			util.Infof("Job(%s) %s for batch %s", ctx.Job().Jid, status, batch.Id)

			isRetry := ctx.Job().Failure != nil && ctx.Job().Failure.RetryCount > 0

			if err := b.batchManager.handleJobFinished(batch, ctx.Job().Jid, success, isRetry); err != nil {
				util.Warnf("error processing finished job for batch %v", err)
				return fmt.Errorf("handleJobFinished: unable to process finished job %s for batch %s", ctx.Job().Jid, batch.Id)
			}
		}
		return next()
	}
}
