package metrics

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
			assert.Nil(t, err)
			assert.False(t, system.Options.Enabled)
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
		ctx := context.Background()
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
			mockDoer.EXPECT().Timing("jobs.succeeded.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("faktory.jobs.processed", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("jobs.failed.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("faktory.jobs.processed", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Incr("faktory.jobs.pushed", gomock.Any(), float64(1)).Return(nil).Times(15)
			mockDoer.EXPECT().Incr("faktory.jobs.fetched", gomock.Any(), float64(1)).Return(nil).Times(6)

			// task calls
			mockDoer.EXPECT().Gauge("jobs.connections.count", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.ops.connections", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.working.count", float64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.working", float64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.scheduled.count", float64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.scheduled", float64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.retries.count", float64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.retries", float64(1), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.dead.count", float64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.dead", float64(0), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.count", float64(8), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.default.count", float64(1), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.enqueued", float64(1), append(tags, "queue:default"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.default.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("jobs.enqueued.default.time_hist", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.current_latency", gomock.Any(), append(tags, "queue:default"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("faktory.jobs.latency", gomock.Any(), append(tags, "queue:default"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.builds.count", float64(6), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.enqueued", float64(6), append(tags, "queue:builds"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.builds.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("jobs.enqueued.builds.time_hist", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.current_latency", gomock.Any(), append(tags, "queue:builds"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("faktory.jobs.latency", gomock.Any(), append(tags, "queue:builds"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.tests.count", float64(1), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.enqueued", float64(1), append(tags, "queue:tests"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.enqueued.tests.time", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("jobs.enqueued.tests.time_hist", gomock.Any(), tags, gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("faktory.jobs.current_latency", gomock.Any(), append(tags, "queue:tests"), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Timing("faktory.jobs.latency", gomock.Any(), append(tags, "queue:tests"), gomock.Any()).Return(nil).Times(1)

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
			three := 3
			job.Retry = &three
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
			server.Manager().RetryJobs(ctx, time.Now().Add(-60*time.Second))
			processJob("builds", cl, false, nil)
			processJob("tests", cl, false, nil)

			m := &metricsTask{system}
			m.Execute(ctx)
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
	job.Retry = new(int)
	return job
}

func runSystem(configDir string, runner func(s *server.Server, cl *client.Client)) {
	dir := fmt.Sprintf("/tmp/metrics_test_%d.db", rand.Int())
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{
		CmdBinding:       "localhost:7418",
		Environment:      "development",
		ConfigDirectory:  configDir,
		LogLevel:         "debug",
		StorageDirectory: dir,
	}
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
