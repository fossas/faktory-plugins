package deadjobs

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	enabledConfig = `
	[dead_job_cleanup]
	enabled = true
	retention_days = 7
	threshold = 5
	batch_size = 100
	interval_seconds = 60
	`

	disabledConfig = `
	[dead_job_cleanup]
	enabled = false
	`

	customConfig = `
	[dead_job_cleanup]
	enabled = true
	retention_days = 30
	threshold = 50000
	batch_size = 5000
	interval_seconds = 7200
	`

	// Only enabled, all other values should use defaults
	enabledOnlyConfig = `
	[dead_job_cleanup]
	enabled = true
	`
)

func createConfigDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/conf.d", tmpDir), os.FileMode(0777))
	require.NoError(t, err)
	return tmpDir
}

func writeConfig(t *testing.T, configDir string, config string) {
	t.Helper()
	configFile := fmt.Sprintf("%s/conf.d/dead_job_cleanup.toml", configDir)
	err := os.WriteFile(configFile, []byte(config), os.FileMode(0444))
	require.NoError(t, err)
}

func runSystem(configDir string, runner func(s *server.Server, cl *client.Client)) {
	dir := fmt.Sprintf("/tmp/deadjobs_test_%d.db", rand.Int())
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

	runner(s, cl)
}

// addDeadJobs adds n dead jobs to the dead set with timestamps starting at baseTime,
// each offset by 1 minute.
func addDeadJobs(t *testing.T, ctx context.Context, deadSet interface {
	Add(context.Context, *client.Job) error
}, jobType string, n int, baseTime time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		job := client.NewJob(jobType, i)
		job.At = util.Thens(baseTime.Add(time.Duration(i) * time.Minute))
		err := deadSet.Add(ctx, job)
		require.NoError(t, err)
	}
}

func TestSubsystemInterface(t *testing.T) {
	t.Run("Name returns correct value", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		assert.Equal(t, "dead_job_cleanup", system.Name())
	})

	t.Run("Shutdown returns nil", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			require.NoError(t, err)
			err = system.Shutdown(s)
			assert.NoError(t, err)
		})
	})

	t.Run("Reload updates options", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			require.NoError(t, err)
			assert.Equal(t, int64(7), system.loadOptions().RetentionDays)

			// Reload should re-parse config (same values since file hasn't changed,
			// but exercises the code path)
			err = system.Reload(s)
			assert.NoError(t, err)
			assert.Equal(t, int64(7), system.loadOptions().RetentionDays)
		})
	})
}

func TestTaskInterface(t *testing.T) {
	t.Run("Name returns correct value", func(t *testing.T) {
		task := &deadJobCleanupTask{}
		assert.Equal(t, "Dead job cleanup", task.Name())
	})

	t.Run("Stats returns correct values", func(t *testing.T) {
		task := &deadJobCleanupTask{
			sweeps:       5,
			totalRemoved: 42,
		}
		ctx := context.Background()
		stats := task.Stats(ctx)
		assert.Equal(t, int64(5), stats["sweeps"])
		assert.Equal(t, int64(42), stats["total_removed"])
	})

	t.Run("Stats returns zeros initially", func(t *testing.T) {
		task := &deadJobCleanupTask{}
		ctx := context.Background()
		stats := task.Stats(ctx)
		assert.Equal(t, int64(0), stats["sweeps"])
		assert.Equal(t, int64(0), stats["total_removed"])
	})
}

func TestConfiguration(t *testing.T) {
	t.Run("no configuration disables plugin", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.False(t, system.loadOptions().Enabled)
		})
	})

	t.Run("explicitly disabled", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, disabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.False(t, system.loadOptions().Enabled)
		})
	})

	t.Run("enabled with only enabled flag uses defaults", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledOnlyConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.True(t, system.loadOptions().Enabled)
			assert.Equal(t, int64(7), system.loadOptions().RetentionDays)
			assert.Equal(t, int64(10000), system.loadOptions().Threshold)
			assert.Equal(t, int64(1000), system.loadOptions().BatchSize)
			assert.Equal(t, int64(3600), system.loadOptions().IntervalSeconds)
		})
	})

	t.Run("enabled with explicit values", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.True(t, system.loadOptions().Enabled)
			assert.Equal(t, int64(7), system.loadOptions().RetentionDays)
			assert.Equal(t, int64(5), system.loadOptions().Threshold)
			assert.Equal(t, int64(100), system.loadOptions().BatchSize)
			assert.Equal(t, int64(60), system.loadOptions().IntervalSeconds)
		})
	})

	t.Run("custom configuration values", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, customConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.True(t, system.loadOptions().Enabled)
			assert.Equal(t, int64(30), system.loadOptions().RetentionDays)
			assert.Equal(t, int64(50000), system.loadOptions().Threshold)
			assert.Equal(t, int64(5000), system.loadOptions().BatchSize)
			assert.Equal(t, int64(7200), system.loadOptions().IntervalSeconds)
		})
	})

	t.Run("threshold of zero is valid", func(t *testing.T) {
		zeroThresholdConfig := `
		[dead_job_cleanup]
		enabled = true
		threshold = 0
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, zeroThresholdConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), system.loadOptions().Threshold)
		})
	})
}

func TestCleanupExecution(t *testing.T) {
	t.Run("skips when below threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // threshold = 5
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 3 old dead jobs (below threshold of 5)
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 3,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), task.sweeps)
			assert.Equal(t, int64(0), task.totalRemoved)
			// Jobs should still be there
			assert.Equal(t, uint64(3), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("skips when exactly at threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // threshold = 5
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add exactly 5 old dead jobs (== threshold, should NOT trigger cleanup)
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 5,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), task.totalRemoved)
			assert.Equal(t, uint64(5), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("cleans when one above threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // threshold = 5
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 6 old dead jobs (> threshold of 5)
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 6,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(6), task.totalRemoved)
			assert.Equal(t, uint64(0), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("removes only old jobs, keeps recent ones", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // retention_days = 7, threshold = 5
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()
			deadSet := s.Store().Dead()

			// Add 10 old dead jobs (14 days ago, older than 7-day retention)
			addDeadJobs(t, ctx, deadSet, "OldJob", 10,
				time.Now().Add(-14*24*time.Hour))

			// Add 3 recent dead jobs (1 hour ago, within retention)
			addDeadJobs(t, ctx, deadSet, "RecentJob", 3,
				time.Now().Add(-1*time.Hour))

			assert.Equal(t, uint64(13), deadSet.Size(ctx))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(10), task.totalRemoved)
			assert.Equal(t, uint64(3), deadSet.Size(ctx))
		})
	})

	t.Run("removes nothing when all jobs are recent despite exceeding threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // retention_days = 7, threshold = 5
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 10 recent dead jobs (all within retention period)
			addDeadJobs(t, ctx, s.Store().Dead(), "RecentJob", 10,
				time.Now().Add(-1*time.Hour))

			assert.Equal(t, uint64(10), s.Store().Dead().Size(ctx))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), task.totalRemoved)
			// All jobs should remain
			assert.Equal(t, uint64(10), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("removes all jobs when all are old", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig) // threshold = 5, batch_size = 100
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 10 old dead jobs
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 10,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(10), task.totalRemoved)
			assert.Equal(t, uint64(0), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("threshold zero always cleans old jobs", func(t *testing.T) {
		zeroThresholdConfig := `
		[dead_job_cleanup]
		enabled = true
		retention_days = 7
		threshold = 0
		batch_size = 100
		interval_seconds = 60
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, zeroThresholdConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()
			deadSet := s.Store().Dead()

			// Add just 1 old dead job (threshold is 0, so any count > 0 triggers)
			addDeadJobs(t, ctx, deadSet, "OldJob", 1,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), task.totalRemoved)
			assert.Equal(t, uint64(0), deadSet.Size(ctx))
		})
	})

	t.Run("respects batch size limit", func(t *testing.T) {
		batchLimitConfig := `
		[dead_job_cleanup]
		enabled = true
		retention_days = 7
		threshold = 2
		batch_size = 3
		interval_seconds = 60
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, batchLimitConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 10 old dead jobs
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 10,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}

			// First sweep: should remove only 3 (batch_size)
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(3), task.totalRemoved)
			assert.Equal(t, uint64(7), s.Store().Dead().Size(ctx))

			// Second sweep: should remove another 3
			err = task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(6), task.totalRemoved)
			assert.Equal(t, uint64(4), s.Store().Dead().Size(ctx))
		})
	})

	t.Run("multiple sweeps drain the set completely", func(t *testing.T) {
		batchLimitConfig := `
		[dead_job_cleanup]
		enabled = true
		retention_days = 7
		threshold = 0
		batch_size = 4
		interval_seconds = 60
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, batchLimitConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Add 10 old dead jobs
			addDeadJobs(t, ctx, s.Store().Dead(), "OldJob", 10,
				time.Now().Add(-14*24*time.Hour))

			task := &deadJobCleanupTask{subsystem: system}

			// Sweep until empty
			for i := 0; i < 5; i++ {
				err := task.Execute(ctx)
				assert.NoError(t, err)
			}

			assert.Equal(t, int64(10), task.totalRemoved)
			assert.Equal(t, uint64(0), s.Store().Dead().Size(ctx))
			assert.Equal(t, int64(5), task.sweeps)
		})
	})

	t.Run("empty dead set with zero threshold is no-op", func(t *testing.T) {
		zeroThresholdConfig := `
		[dead_job_cleanup]
		enabled = true
		threshold = 0
		batch_size = 100
		interval_seconds = 60
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, zeroThresholdConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.storeOptions(system.parseOptions(s))
			ctx := context.Background()

			// Empty dead set, threshold 0 -> 0 <= 0, should skip
			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), task.totalRemoved)
		})
	})
}

func TestFormatCount(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1.0K"},
		{1500, "1.5K"},
		{10500, "10.5K"},
		{999999, "1000.0K"},
		{1000000, "1.0M"},
		{2500000, "2.5M"},
		{1000000000, "1000.0M"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			assert.Equal(t, tt.expected, formatCount(tt.input))
		})
	}
}
