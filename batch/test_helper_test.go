package batch

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
)

const testPort = "7413"

// withServer creates a test server with the batch subsystem enabled and runs the test function
func withServer(runner func(s *server.Server, cl *client.Client)) {
	withServerConfig(true, runner)
}

// withServerConfig creates a test server with configurable batch subsystem state
func withServerConfig(batchEnabled bool, runner func(s *server.Server, cl *client.Client)) {
	dir := fmt.Sprintf("/tmp/batch_test_%d.db", rand.Int())
	defer os.RemoveAll(dir)

	configDir := fmt.Sprintf("/tmp/batch_test_config_%d", rand.Int())
	os.MkdirAll(configDir+"/conf.d", 0755)
	defer os.RemoveAll(configDir)

	// Write batch config
	configContent := fmt.Sprintf(`[batch]
enabled = %t
`, batchEnabled)
	if err := os.WriteFile(configDir+"/conf.d/batch.toml", []byte(configContent), 0644); err != nil {
		panic(err)
	}

	opts := &cli.CliOptions{
		CmdBinding:       "localhost:" + testPort,
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

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}
	s.Register(new(BatchSubsystem))

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
	srv.Address = "localhost:" + testPort
	cl, err := client.Dial(srv, "123456")
	if err != nil {
		return nil, err
	}
	if _, err = cl.Beat(); err != nil {
		return nil, err
	}

	return cl, nil
}
