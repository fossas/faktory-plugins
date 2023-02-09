package expire

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

func TestExpiresAt(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		// Push a job that has already expired
		j1 := client.NewJob("JobOne", 1)
		j1.SetExpiresAt(time.Now().Add(time.Minute * -1))
		err := cl.Push(j1)
		assert.Nil(t, err)

		// Push a job that will expire
		j2 := client.NewJob("JobOne", 1)
		j2.SetExpiresAt(time.Now().Add(time.Minute * 1))
		err = cl.Push(j2)
		assert.Nil(t, err)

		// The fetched job should be j2 because j1 is expired
		job, err := cl.Fetch("default")
		assert.Nil(t, err)
		assert.Equal(t, j2.Jid, job.Jid)

		// Verify that the queue is empty
		job, err = cl.Fetch("default")
		assert.Nil(t, err)
		assert.Nil(t, job)
	})
}

func TestExpiresIn(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		// Push a job that has already expired
		j1 := client.NewJob("JobOne", 1)
		j1.SetExpiresIn(time.Minute * -1)
		err := cl.Push(j1)
		assert.Nil(t, err)

		// Push a job that will expire
		j2 := client.NewJob("JobOne", 1)
		j2.SetExpiresIn(time.Minute * 1)
		err = cl.Push(j2)
		assert.Nil(t, err)

		// The fetched job should be j2 because j1 is expired
		job, err := cl.Fetch("default")
		assert.Nil(t, err)
		assert.Equal(t, j2.Jid, job.Jid)

		// Verify that the queue is empty
		job, err = cl.Fetch("default")
		assert.Nil(t, err)
		assert.Nil(t, job)
	})
}

// The `expires_in` value is converted to a timestamp and saved as `expires_at`
func TestRemovesExpiresIn(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		// Push a job that has already expired
		j1 := client.NewJob("JobOne", 1)
		j1.SetCustom("expires_in", (time.Duration(1) * time.Minute).String())
		err := cl.Push(j1)
		assert.Nil(t, err)

		// The fetched job should not have `expires_in` set
		job, err := cl.Fetch("default")
		assert.Nil(t, err)
		assert.Equal(t, j1.Jid, job.Jid)
		jobExpIn, _ := job.GetCustom("expires_in")
		assert.Nil(t, jobExpIn)
		jobExpAt, _ := job.GetCustom("expires_at")
		assert.NotNil(t, jobExpAt)
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
	s.Register(new(ExpireSubsystem))

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
