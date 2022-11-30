package retryable

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	withServer(func(cl *client.Client) {
		j1 := client.NewJob("JobOne", 1)
		err := cl.Push(j1)
		assert.Nil(t, err)

		j2 := client.NewJob("JobTwo", 2)
		err = cl.Push(j2)
		assert.Nil(t, err)

		job, _ := cl.Fetch("default")
		_, err = cl.Generic(fmt.Sprintf(`RETRY {"jid":%q}`, job.Jid))
		assert.Nil(t, err)

		// j2 would be at the front of the queue if j1 had ACKed, so if the fetched
		// job is j1 then RETRY successfully put j1 at the front of the queue.
		job, _ = cl.Fetch("default")
		assert.Equal(t, j1.Jid, job.Jid)
	})
}

func withServer(runner func(cl *client.Client)) {
	dir := "/tmp/retry_test.db"
	defer os.RemoveAll(dir)

	opts := &cli.CliOptions{
		CmdBinding:       "localhost:7414",
		WebBinding:       "localhost:7415",
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
	s.Register(new(RetryableSubsystem))

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

	runner(cl)
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
