package batch

import (
	"context"
	"fmt"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

func (b *BatchSubsystem) pushMiddleware(ctx context.Context, next func() error) error {
	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)

	if bid, ok := mh.Job().GetCustom("bid"); ok {
		batchId, err := b.batchManager.getBatchIdFromInterface(bid)
		if err != nil {
			return fmt.Errorf("pushMiddleware: unable to parse batch id %s", bid)
		}
		b.batchManager.lockBatchIfExists(batchId)
		defer b.batchManager.unlockBatchIfExists(batchId)
		batch, err := b.batchManager.getBatch(ctx, batchId)
		if err != nil {
			return fmt.Errorf("pushMiddleware: unable to retrieve batch %s", bid)
		}
		if err := b.batchManager.handleJobQueued(ctx, batch); err != nil {
			util.Warnf("unable to add batch %v", err)
			return fmt.Errorf("pushMiddleware: Unable to add job %s to batch %s", mh.Job().Jid, bid)
		}
		util.Debugf("Added %s to batch %s", mh.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubsystem) handleJobFinished(success bool) func(ctx context.Context, next func() error) error {
	return func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)

		if success {
			// check if this is a success / complete job from batch
			if bid, ok := mh.Job().GetCustom("_bid"); ok {

				batchId, err := b.batchManager.getBatchIdFromInterface(bid)
				if err != nil {
					util.Warnf("unable to parse batch id %s", bid)
					return next()
				}
				b.batchManager.lockBatchIfExists(batchId)
				defer b.batchManager.unlockBatchIfExists(batchId)
				batch, err := b.batchManager.getBatch(ctx, batchId)
				if err != nil {
					util.Warnf("unable to retrieve batch %s: %v", bid, err)
					return next()
				}
				cb, ok := mh.Job().GetCustom("_cb")
				if !ok {
					util.Warnf("Batch (%s) callback job (%s) does not have _cb specified", bid, mh.Job().Type)
					return fmt.Errorf("handleJobFinished: callback job (%s) does not have _cb specified", mh.Job().Type)
				}
				callbackType, ok := cb.(string)
				if !ok {
					util.Warnf("Error converting callback job type %s", cb)
					return fmt.Errorf("handleJobFinished: invalid callback type %s", cb)
				}
				if err := b.batchManager.handleCallbackJobSucceeded(ctx, batch, callbackType); err != nil {
					util.Warnf("Unable to update batch")
					return fmt.Errorf("handleJobFinished: unable to update batch %s", batch.Id)
				}
				return next()
			}
		}
		if bid, ok := mh.Job().GetCustom("bid"); ok {
			batchId, err := b.batchManager.getBatchIdFromInterface(bid)
			if err != nil {
				return fmt.Errorf("handleJobFinished: unable to parse batch id %s", bid)
			}
			b.batchManager.lockBatchIfExists(batchId)
			defer b.batchManager.unlockBatchIfExists(batchId)
			batch, err := b.batchManager.getBatch(ctx, batchId)
			if err != nil {
				util.Warnf("handleJobFinished: unable to retrieve batch %s", bid)
				return next()
			}
			status := "succeeded"
			if !success {
				status = "failed"
			}
			util.Debugf("Job(%s) %s for batch %s", mh.Job().Jid, status, batch.Id)

			isRetry := mh.Job().Failure != nil && mh.Job().Failure.RetryCount > 0

			if err := b.batchManager.handleJobFinished(ctx, batch, mh.Job().Jid, success, isRetry); err != nil {
				util.Warnf("error processing finished job for batch %v", err)
				return next()
			}
		}
		return next()
	}
}
