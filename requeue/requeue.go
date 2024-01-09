package requeue

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

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
	util.Info("Loaded requeue jobs plugin")
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
	job, err := s.Manager().Acknowledge(c.Context, jid)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}
	// s.Manager().Acknowledge() will return (nil, nil) if there's no in-memory
	// reservation for the given jid.
	if job == nil {
		_ = c.Error(cmd, fmt.Errorf("requeue: Can't requeue job with no reservation"))
		return
	}

	q, err := s.Store().GetQueue(c.Context, job.Queue)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	pushChain := pushChain(s.Manager())
	// Our final function in the middleware chain is based off of the
	// manager.Push -> manager.enqueue -> redisQueue.Push implementation upstream.
	// https://github.com/contribsys/faktory/blob/v1.8.0/manager/manager.go#L222-L236
	// https://github.com/contribsys/faktory/blob/v1.8.0/manager/manager.go#L251-L255
	// https://github.com/contribsys/faktory/blob/v1.8.0/storage/queue_redis.go#L104
	ctxh := context.WithValue(c.Context, manager.MiddlewareHelperKey, Ctx{job, s.Manager(), nil})
	err = callMiddleware(ctxh, pushChain, func() error {
		job.EnqueuedAt = util.Nows()
		jdata, err := json.Marshal(job)
		if err != nil {
			return err
		}
		// redisQueue.Push does an LPUSH and redisQueue.Pop does an RPOP, so to
		// insert a job at the front of the queue we want to do an RPUSH.
		s.Manager().Redis().RPush(c.Context, q.Name(), jdata)
		return nil
	})
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

// callMiddleware runs the given job through the given middleware chain.
// `final` is the function called if the entire chain passes the job along.
// Copied from https://github.com/contribsys/faktory/blob/v1.8.0/manager/middleware.go#L59-L67
func callMiddleware(ctx context.Context, chain manager.MiddlewareChain, final func() error) error {
	if len(chain) == 0 {
		return final()
	}

	link := chain[0]
	rest := chain[1:]
	return link(ctx, func() error { return callMiddleware(ctx, rest, final) })
}

// pushChain pulls the PUSH middleware chain out of a manager.manager
// https://github.com/contribsys/faktory/blob/v1.6.2/manager/manager.go#L183
func pushChain(mgr manager.Manager) manager.MiddlewareChain {
	rm := reflect.ValueOf(mgr).Elem()
	rf := rm.FieldByName("pushChain")
	rf = reflect.NewAt(rf.Type(), rf.Addr().UnsafePointer()).Elem()
	return rf.Interface().(manager.MiddlewareChain)
}
