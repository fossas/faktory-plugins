package requeue

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

func TestRequeue(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		j1 := client.NewJob("JobOne", 1)
		err := cl.Push(j1)
		assert.Nil(t, err)

		j2 := client.NewJob("JobTwo", 2)
		err = cl.Push(j2)
		assert.Nil(t, err)

		job, _ := cl.Fetch("default")
		_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, job.Jid))
		assert.Nil(t, err)

		// j2 would be at the front of the queue if j1 had ACKed without REQUEUE, so
		// if the fetched job is j1 then REQUEUE successfully put j1 at the front of
		// the queue.
		job, _ = cl.Fetch("default")
		assert.Equal(t, j1.Jid, job.Jid)
		// Verify the j2 is next
		job, _ = cl.Fetch("default")
		assert.Equal(t, j2.Jid, job.Jid)
		// Verify that the queue is empty
		job, _ = cl.Fetch("default")
		assert.Nil(t, job)
	})
}

func TestPushMiddleware(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		// Sentinel values
		attr := "push_chain"
		val := "hello world"

		// Queue a job without any custom attributes being set
		j1 := client.NewJob("JobOne", 1)
		err := cl.Push(j1)
		assert.Nil(t, err)
		_, ok := j1.GetCustom(attr)
		assert.False(t, ok)

		// Add a PUSH middleware that will run when the job is REQUEUEd
		// This sets the custom attribute
		s.Manager().AddMiddleware("push", func(next func() error, ctx manager.Context) error {
			ctx.Job().SetCustom(attr, val)
			return next()
		})

		// Fetch and REQUEUE the job
		cl.Fetch("default")
		_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, j1.Jid))
		assert.Nil(t, err)

		// Check that the middleware ran and added the custom attribute
		j2, _ := cl.Fetch("default")
		assert.Equal(t, j1.Jid, j2.Jid)
		v, ok := j2.GetCustom(attr)
		assert.True(t, ok)
		assert.Equal(t, val, v)
	})
}

func withServer(runner func(s *server.Server, cl *client.Client)) {
	dir := "/tmp/requeue_test.db"
	defer os.RemoveAll(dir)

	opts := &cli.CliOptions{
		CmdBinding:       "localhost:7414",
		Environment:      "development",
		ConfigDirectory:  ".",
		LogLevel:         "debug",
		StorageDirectory: dir,
	}
	s, stopper, err := cli.BuildServer(opts)
	if err != nil {
		panic(err)
	}
	defer stopper()

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}
	s.Register(new(RequeueSubsystem))

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	cl, err := getClient()
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	runner(s, cl)
	close(s.Stopper())
	s.Stop(nil)
}

func getClient() (*client.Client, error) {
	// this is a worker process so we need to set the global WID before connecting
	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	srv := client.DefaultServer()
	srv.Address = "localhost:7414"
	cl, err := client.Dial(srv, "123456")
	if err != nil {
		return nil, err
	}
	if _, err = cl.Beat(); err != nil {
		return nil, err
	}

	return cl, nil
}
