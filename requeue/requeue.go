package requeue

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Subsystem = &RequeueSubsystem{}

// RequeueSubsystem allows a client to indicate that it is not able to complete
// the job and that it should be retried.
type RequeueSubsystem struct{}

// Start loads the plugin.
func (r *RequeueSubsystem) Start(s *server.Server) error {
	server.CommandSet["REQUEUE"] = r.requeueCommand
	util.Info("Loaded the requeue jobs plugin")
	return nil
}

// Name returns the name of the plugin.
func (r *RequeueSubsystem) Name() string {
	return "Requeue"
}

// Reload does not do anything.
func (r *RequeueSubsystem) Reload(s *server.Server) error {
	return nil
}

// requeueCommand implements the REQUEUE client command which
// - ACKs the job
// - Requeues the job to the _front_ of the queue
// REQUEUE {"jid":"123456789"}
func (r *RequeueSubsystem) requeueCommand(c *server.Connection, s *server.Server, cmd string) {
	data := cmd[8:]

	var hash map[string]string
	err := json.Unmarshal([]byte(data), &hash)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid REQUEUE %s", data))
		return
	}
	jid, ok := hash["jid"]
	if !ok {
		_ = c.Error(cmd, fmt.Errorf("invalid REQUEUE %s", data))
		return
	}
	job, err := s.Manager().Acknowledge(jid)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	q, err := s.Store().GetQueue(job.Queue)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	mgr := s.Manager()
	// Extract the PUSH middleware chain
	rm := reflect.ValueOf(mgr).Elem()
	rf := rm.FieldByName("pushChain")
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	pushChain := rf.Interface().(manager.MiddlewareChain)
	// Build a manager.Ctx
	// We have to create new Values using pointers because reflect does not allow
	// reading or setting unexported fields.
	ctx := manager.Ctx{Context: context.Background()}
	rctx := reflect.ValueOf(&ctx).Elem()
	rjob := rctx.FieldByName("job")
	rjob = reflect.NewAt(rjob.Type(), unsafe.Pointer(rjob.UnsafeAddr())).Elem()
	rjob.Set(reflect.ValueOf(job))
	rmgr := rctx.FieldByName("mgr")
	rmgr = reflect.NewAt(rmgr.Type(), unsafe.Pointer(rmgr.UnsafeAddr())).Elem()
	// s.Manager()'s type is the interface manager.Manager but we need a *manager.manager
	// so we're getting the underlying value's address
	rmgr.Set(reflect.ValueOf(mgr).Elem().Addr())

	err = callMiddleware(pushChain, ctx, func() error {
		job.EnqueuedAt = util.Nows()
		jdata, err := json.Marshal(job)
		if err != nil {
			return err
		}
		s.Manager().Redis().RPush(q.Name(), jdata)
		return nil
	})
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

// Run the given job through the given middleware chain.
// `final` is the function called if the entire chain passes the job along.
// copied from `manager/middleware.go`
func callMiddleware(chain manager.MiddlewareChain, ctx manager.Context, final func() error) error {
	if len(chain) == 0 {
		return final()
	}

	link := chain[0]
	rest := chain[1:]
	return link(func() error { return callMiddleware(rest, ctx, final) }, ctx)
}
