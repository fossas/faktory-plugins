package requeue

import (
	"context"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
)

type contextKey string

// RequeueContextKey is set on the context during a REQUEUE operation.
// Other middleware (e.g. batch) can check for this to skip accounting
// that should not apply when a job is being requeued rather than completed.
const RequeueContextKey contextKey = "requeue"

// IsRequeue returns true if the context indicates a REQUEUE operation is in progress.
func IsRequeue(ctx context.Context) bool {
	return ctx.Value(RequeueContextKey) != nil
}

var _ manager.Context = &Ctx{}

// Ctx implements Faktory's `manager.Context`.
// Duplicated from https://github.com/contribsys/faktory/blob/v1.8.0/manager/middleware.go#L24-L40
type Ctx struct {
	job *client.Job
	mgr manager.Manager
	res *manager.Reservation
}

func (c Ctx) Reservation() *manager.Reservation {
	return c.res
}

func (c Ctx) Job() *client.Job {
	return c.job
}

func (c Ctx) Manager() manager.Manager {
	return c.mgr
}
