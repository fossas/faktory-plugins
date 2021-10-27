package metrics

import (
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
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/fossas/faktory-plugins/metrics/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	enabledStatsdConfig = `
	[metrics]
	enabled = true
	tags = ["tag1:value1", "tag2:value2"]
	namespace = "test"
	`

	disabledStatsdConfig = `
	[metrics]
	enabled = false
	tags = ["tag1:value1", "tag2:value2"]
	`

	statsdConfig = `
	[metrics]
	enabled = true
	tags = ["tag1:value1", "tag2:value2"]
	`
)

func createConfigDir(t *testing.T) string {
	tmpDir := t.TempDir()
	os.Mkdir(fmt.Sprintf("%s/conf.d", tmpDir), os.FileMode(0777))
	return tmpDir
}

func TestMetrics(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	os.Setenv("DD_AGENT_HOST", "localhost")

	t.Run("no configuration", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			err := system.Start(server)
			assert.EqualError(t, err, "No [metrics] configuration found, plugin cannot start")
		})
	})
	t.Run("plugin is enabled", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(enabledStatsdConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			err := system.Start(server)
			assert.Nil(t, err)
			assert.True(t, system.Options.Enabled)
			assert.Equal(t, "test", system.Options.Namespace)
			assert.Equal(t, []string{"tag1:value1", "tag2:value2"}, system.Options.Tags)
			assert.NotNil(t, system.statsDClient)
		})
	})
	t.Run("plugin is disabled", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(disabledStatsdConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			err := system.Start(server)
			assert.Nil(t, err)
			assert.False(t, system.Options.Enabled)
			assert.Nil(t, system.statsDClient)
		})
	})

	mockDoer := mocks.NewMockClientInterface(mockCtrl)
	t.Run("Metrics are collected in the middleware", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(statsdConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			system.Server = server
			system.Options = system.getOptions(server)
			system.statsDClient = mockDoer
			system.addMiddleware()

			// middleware jobs
			tags := []string{"tag1:value1", "tag2:value2"}
			mockDoer.EXPECT().Incr("jobs.succeeded.count", gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("jobs.succeeded.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Incr("jobs.failed.count", gomock.Any(), gomock.Any()).Return(nil).Times(2)
			mockDoer.EXPECT().Timing("jobs.failed.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)

			// task calls
			mockDoer.EXPECT().Count("jobs.working.count", int64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.scheduled.count", int64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.retries.count", int64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.dead.count", int64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.enqueued.count", int64(8), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.enqueued.default.count", int64(1), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.default.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.enqueued.builds.count", int64(6), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.builds.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.enqueued.tests.count", int64(1), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.tests.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)

			// create 15 jobs
			// default queue
			if err := cl.Push(createJob("default", "Scan", 1)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("default", "Scan", 2)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("default", "RScan", 1)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("default", "RScan", 2)); err != nil {
				panic(err)
			}

			// 8 builds queue
			if err := cl.Push(createJob("builds", "ProvidedBuild", 2)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "ProvidedBuild", 1)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "ProvidedBuild", 3)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "ProvidedBuild", 4)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "NormalBuild", 1)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "NormalBuild", 2)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "NormalBuild", 3)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("builds", "NormalBuild", 4)); err != nil {
				panic(err)
			}

			// 3 test queue
			job := createJob("tests", "Test", 1)
			job.Retry = 3
			if err := cl.Push(job); err != nil {
				panic(err)
			}

			job2 := createJob("tests", "Test", 2)
			job2.At = time.Now().Add(10 * time.Hour).Format(util.TimestampFormat)
			if err := cl.Push(job2); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("tests", "Test", 3)); err != nil {
				panic(err)
			}

			// process 3 jobs
			processJob("default", cl, true, nil)
			processJob("default", cl, true, nil)
			processJob("builds", cl, true, nil)

			// fail 3 jobs
			processJob("default", cl, false, nil)
			server.Manager().RetryJobs(time.Now().Add(-60 * time.Second))
			processJob("builds", cl, false, nil)
			processJob("tests", cl, false, nil)

			m := &metricsTask{system}
			m.Execute()
		})
	})
}

func processJob(queue string, cl *client.Client, success bool, runner func()) {
	fetchedJob, err := cl.Fetch(queue)
	if err != nil {
		panic(err)
	}

	if fetchedJob == nil {
		panic("Job does not exist")
	}

	if runner != nil {
		runner()
	}

	if success {
		err = cl.Ack(fetchedJob.Jid)
		if err != nil {
			handleError(err)
			return
		}
		return
	} else {
		err = cl.Fail(fetchedJob.Jid, os.ErrClosed, nil)
		if err != nil {
			handleError(err)
			return
		}
	}

}
func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
}

func createJob(queue string, jobtype string, args ...interface{}) *client.Job {
	job := client.NewJob(jobtype, args...)
	job.Queue = queue
	job.Retry = 0
	return job
}

func runSystem(configDir string, runner func(s *server.Server, cl *client.Client)) {
	dir := "/tmp/batching_system.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7418", "localhost:7420", "development", configDir, "debug", dir}
	s, stopper, err := cli.BuildServer(opts)

	if err != nil {
		panic(err)
	}
	defer stopper()
	defer s.Stop(nil)

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	srv := client.DefaultServer()
	srv.Address = "localhost:7418"
	cl, err := client.Dial(srv, "123456")
	if err != nil {
		panic(err)
	}
	if _, err = cl.Beat(); err != nil {
		panic(err)
	}
	defer cl.Close()
	if err != nil {
		panic(err)
	}

	runner(s, cl)
}
