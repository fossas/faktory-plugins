package main

import (
	"fmt"
	"log"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
	"github.com/fossas/faktory-plugins/batch"
	"github.com/fossas/faktory-plugins/cron"
	"github.com/fossas/faktory-plugins/expire"
	"github.com/fossas/faktory-plugins/metrics"
	"github.com/fossas/faktory-plugins/retryable"
	"github.com/fossas/faktory-plugins/uniq"
)

var (
	version = "dev"
	commit  = "n/a"
)

func logPreamble() {
	log.SetFlags(0)
	log.Println(client.Name, version, fmt.Sprintf("commit: %s", commit))
	log.Println(fmt.Sprintf("Copyright © %d Contributed Systems LLC and FOSSA Inc", time.Now().Year()))
	log.Println("Licensed under the GNU Affero Public License 3.0")
}

func main() {
	logPreamble()

	opts := cli.ParseArguments()
	util.InitLogger(opts.LogLevel)
	util.Debugf("Options: %v", opts)

	s, stopper, err := cli.BuildServer(&opts)
	if stopper != nil {
		defer stopper()
	}

	if err != nil {
		util.Error("Unable to create Faktory server", err)
		return
	}

	err = s.Boot()
	if err != nil {
		util.Error("Unable to boot the command server", err)
		return
	}

	s.Register(webui.Subsystem(opts.WebBinding))
	// fossa plugins
	s.Register(new(uniq.UniqSubsystem))
	s.Register(new(metrics.MetricsSubsystem))
	s.Register(new(cron.CronSubsystem))
	s.Register(new(batch.BatchSubsystem))
	s.Register(new(expire.ExpireSubsystem))
	s.Register(new(retryable.RetryableSubsystem))
	go cli.HandleSignals(s)

	go func() {
		_ = s.Run()
	}()

	<-s.Stopper()
	s.Stop(nil)
}
