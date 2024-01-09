package requeue

import (
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
)

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
