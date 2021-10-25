package metrics

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/fossas/faktory-plugins/metrics/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	validStatsdConfig = `
	[metrics]
	statsd_server = "localhost:1000"
	`
	invalidStatsdConfig = `
	[metrics]
	statsd_server = 1
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

	mockDoer := mocks.NewMockClientInterface(mockCtrl)

	t.Run("statsd_server is invalid", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(invalidStatsdConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			system.Server = server
			system.Options = system.getOptions(server)
			err := system.connectStatsd()
			assert.EqualError(t, err, "statsd server not configured")
		})
	})
	t.Run("configuration is valid", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(validStatsdConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			system.Server = server
			system.Options = system.getOptions(server)
			err := system.connectStatsd()
			assert.Nil(t, err)
			assert.NotNil(t, system.statsdClient)

		})
	})
	t.Run("Reload reloads the config", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/statsd.toml", configDir)
		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			system.Server = server
			system.Options = system.getOptions(server)
			server.Register(system)
			assert.Equal(t, system.Options.statsdServer, "")
			if err := ioutil.WriteFile(confgFile, []byte(validStatsdConfig), os.FileMode(0444)); err != nil {
				panic(err)
			}
			syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
			time.Sleep(1 * time.Second)

			assert.Equal(t, system.Options.statsdServer, "localhost:1000")
		})
	})
	t.Run("Metrics are collected", func(t *testing.T) {
		system := new(MetricsSubsystem)
		configDir := createConfigDir(t)

		runSystem(configDir, func(server *server.Server, cl *client.Client) {
			system.Server = server
			system.Options = system.getOptions(server)
			system.statsdClient = mockDoer
			system.addMiddleware()

			mockDoer.EXPECT().Incr("jobs.succeeded.count", gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("jobs.succeeded.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Incr("jobs.failed.count", gomock.Any(), gomock.Any()).Return(nil).Times(3)
			mockDoer.EXPECT().Timing("jobs.failed.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(4)
			mockDoer.EXPECT().Count("jobs.default.queued.count", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.default.queued.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.builds.queued.count", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.builds.queued.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.tests.queued.count", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.tests.queued.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

			mockDoer.EXPECT().Incr("jobs.retried_at_least_once.count", gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Count("jobs.retry.queued.count", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
			mockDoer.EXPECT().Gauge("jobs.retry.queued.time", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

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

			if err := cl.Push(createJob("tests", "Test", 1)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("tests", "Test", 2)); err != nil {
				panic(err)
			}
			if err := cl.Push(createJob("tests", "Test", 3)); err != nil {
				panic(err)
			}

			job := client.NewJob("retry", 1)
			job.Retry = 2
			job.Queue = "retry"
			cl.Push(job)

			time.Sleep(1 * time.Second)
			processJob("default", cl, true, nil)
			processJob("default", cl, true, nil)
			processJob("builds", cl, true, nil)

			processJob("default", cl, false, nil)
			processJob("builds", cl, false, nil)
			processJob("tests", cl, false, nil)

			m := &metrics{server.Store(), system.Client, 1, []string{}}
			m.Execute()

			processJob("retry", cl, false, nil)
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
