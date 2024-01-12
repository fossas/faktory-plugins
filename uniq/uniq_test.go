package uniq

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

const enabledUniqConfig = `
[uniq]
enabled = true
`

func TestUniq(t *testing.T) {
	opts := cli.ParseArguments()
	util.InitLogger("info")

	configDir := t.TempDir()
	os.Mkdir(fmt.Sprintf("%s/conf.d", configDir), os.FileMode(0777))
	dir := "/tmp/uniq_system.db"
	defer os.RemoveAll(dir)
	opts.ConfigDirectory = configDir
	opts.StorageDirectory = dir
	confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
	if err := ioutil.WriteFile(confgFile, []byte(enabledUniqConfig), os.FileMode(0444)); err != nil {
		panic(err)
	}
	s, stopper, err := cli.BuildServer(&opts)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	s.Register(new(UniqSubsystem))

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("jobs are uniq", func(t *testing.T) {
		ctx := context.Background()

		// this is a worker process so we need to set the global WID before connecting
		client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

		cl, err := client.Dial(client.DefaultServer(), "123456")
		if err != nil {
			panic(err)
		}
		defer cl.Close()
		sig, err := cl.Beat()
		assert.Equal(t, "", sig)
		assert.NoError(t, err)

		// create uniqjob
		job1 := client.NewJob("UniqJob", 1, "string", 3)
		job1.SetCustom("unique_until", "start")
		job1.SetCustom("unique_for", 10)

		job2 := client.NewJob("UniqJob", 1, "string", 3)
		job2.SetCustom("unique_until", "success")
		job2.SetCustom("unique_for", 10)

		job3 := client.NewJob("UniqJob", 1, "string", 3)
		job3.SetCustom("unique_until", "success")
		job3.SetCustom("unique_for", 1)

		job4 := client.NewJob("UniqJob", 1, "string", 3)
		job4.SetCustom("unique_until", "success")
		job4.SetCustom("unique_for", 1)

		otherUniqJob := client.NewJob("Uniq", 2, "string", 3)

		err = cl.Push(job1)
		assert.Nil(t, err)

		err = cl.Push(job2)
		assert.EqualError(t, err, "NOTUNIQUE Job has already been queued.")

		queue, err := s.Store().GetQueue(ctx, "default")
		if err != nil {
			panic(err)
		}
		assert.Equal(t, queue.Size(ctx), uint64(1))

		// job1 processing
		processJob(cl, func() {
			// uniq job can be queued since util is start
			err = cl.Push(job2)
			assert.Nil(t, err)
		})

		// job2 processing
		processJob(cl, func() {
			err = cl.Push(job3)
			assert.EqualError(t, err, "NOTUNIQUE Job has already been queued.")
		})

		// job can be pushed since it has been finished
		err = cl.Push(job3)
		assert.Nil(t, err)

		// wait 1 second for expiration
		time.Sleep(2 * time.Second)
		err = cl.Push(job4)
		assert.Nil(t, err)

		// push another job with same title but other arguments
		err = cl.Push(otherUniqJob)
		assert.Nil(t, err)

		// job3, job4, otherUniqJob
		assert.Equal(t, queue.Size(ctx), uint64(3))

		invalidJob := client.NewJob("UniqJobTwo", 3, "string", 3)
		invalidJob.SetCustom("unique_until", "other")
		invalidJob.SetCustom("unique_for", 100)
		err = cl.Push(invalidJob)
		assert.EqualError(t, err, "ERR invalid value for unique_until.")

		invalidJob.SetCustom("unique_until", "start")
		invalidJob.SetCustom("unique_for", 0)
		err = cl.Push(invalidJob)
		assert.EqualError(t, err, "ERR unique_for must be greater than or equal to 1.")

		jobWithOutUntil := client.NewJob("UniqueJobThree", 3, "string", 3)
		jobWithOutUntil.SetCustom("unique_for", 10)

		jobWithOutUntil2 := client.NewJob("UniqueJobThree", 3, "string", 3)
		jobWithOutUntil2.SetCustom("unique_for", 10)

		err = cl.Push(jobWithOutUntil)
		assert.Nil(t, err)
		err = cl.Push(jobWithOutUntil2)
		assert.EqualError(t, err, "NOTUNIQUE Job has already been queued.")

		hash, err := cl.Info()
		if err != nil {
			handleError(err)
			return
		}
		util.Infof("%v", hash)
	})

	t.Run("disabled config", func(t *testing.T) {
		system := &UniqSubsystem{}
		config := make(map[string]interface{})
		config["enabled"] = false
		s.Options.GlobalConfig["uniq"] = config

		system.Start(s)
		assert.False(t, system.Options.Enabled)
	})

	t.Run("no config", func(t *testing.T) {
		system := &UniqSubsystem{}
		delete(s.Options.GlobalConfig, "uniq")

		system.Start(s)
		assert.False(t, system.Options.Enabled)
	})

	s.Stop(nil)
}

func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
}

func processJob(cl *client.Client, runner func()) {
	fetchedJob, err := cl.Fetch("default")
	if err != nil {
		panic(err)
	}

	runner()

	err = cl.Ack(fetchedJob.Jid)
	if err != nil {
		panic(err)
	}

}
