package uniq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

// The UniqSubsytem generates a lock using redis to ensure jobs from the same queue, params and name
// are not queued more than once
// it follows the spec found at: https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs
type UniqSubsystem struct {
	Server *server.Server
	// uniq plugin options
	Options *Options
}

type Options struct {
	// whether or not to enable the plugin
	Enabled bool
}

// starts the subsystem and adds the needed middleware
func (u *UniqSubsystem) Start(s *server.Server) error {
	u.Server = s
	u.Options = u.getOptions(s)
	if !u.Options.Enabled {
		return nil
	}
	u.addMiddleware()
	util.Info("Unique subsystem started")
	return nil
}

// returns the name of the subsystem or plugin
func (u *UniqSubsystem) Name() string {
	return "Uniq"
}

// reload - nothing needs to be done but the function must exist for subsystems
func (u *UniqSubsystem) Reload(s *server.Server) error {
	return nil
}

// shutdown - nothing needs to be done but the function must exist for subsystems
func (u *UniqSubsystem) Shutdown(s *server.Server) error {
	return nil
}

func (u *UniqSubsystem) getOptions(s *server.Server) *Options {
	enabledValue := s.Options.Config("uniq", "enabled", false)
	enabled, ok := enabledValue.(bool)
	if !ok {
		enabled = false
	}
	return &Options{
		Enabled: enabled,
	}
}

func (u *UniqSubsystem) addMiddleware() {
	u.Server.Manager().AddMiddleware("push", u.lockMiddleware)
	u.Server.Manager().AddMiddleware("fetch", u.releaseLockMiddleware(string(client.UntilStart)))
	u.Server.Manager().AddMiddleware("ack", u.releaseLockMiddleware(string(client.UntilSuccess)))
}

func (u *UniqSubsystem) lockMiddleware(ctx context.Context, next func() error) error {
	mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)

	var uniqueFor float64
	uniqueForValue, ok := mh.Job().GetCustom("unique_for")
	if !ok {
		return next()
	}
	uniqueFor, ok = uniqueForValue.(float64)
	if !ok {
		return manager.Halt("ERR", "Invalid value for unique_for.")
	}
	if uniqueFor < 1 {
		return manager.Halt("ERR", "unique_for must be greater than or equal to 1.")
	}

	uniqueUntilValue, ok := mh.Job().GetCustom("unique_until")
	if ok {
		uniqueUntil, ok := uniqueUntilValue.(string)
		if !ok || (uniqueUntil != string(client.UntilStart) && uniqueUntil != string(client.UntilSuccess)) {
			return manager.Halt("ERR", "invalid value for unique_until.")
		}
	} else {
		mh.Job().SetUniqueness(client.UntilSuccess)
	}

	key, err := u.generateKey(mh.Job())
	if err != nil {
		return fmt.Errorf("generate key: %v", err)
	}

	lockTime := time.Duration(uniqueFor) * time.Second
	if mh.Job().At != "" {
		t, err := util.ParseTime(mh.Job().At)
		if err != nil {
			return fmt.Errorf("invalid timestamp for 'at': '%s'", mh.Job().At)
		}
		if t.After(time.Now()) {
			addedTime := time.Duration(time.Until(t))
			lockTime += addedTime
		}

	}
	status, err := u.Server.Manager().Redis().SetNX(ctx, key, mh.Job().Jid, lockTime).Result()
	if err != nil {
		return fmt.Errorf("redis unable to set key: %w", err)
	}
	if !status {
		return manager.Halt("NOTUNIQUE", "Job has already been queued.")
	}
	util.Info(fmt.Sprintf("Locking key: %s for %d seconds", key, lockTime/time.Second))

	return next()
}

func (u *UniqSubsystem) releaseLockMiddleware(releaseAt string) func(context.Context, func() error) error {
	return func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)

		if _, ok := mh.Job().GetCustom("unique_for"); !ok {
			return next()
		}

		uniqueUntilValue, ok := mh.Job().GetCustom("unique_until")

		if !ok {
			return next()
		}

		key, err := u.generateKey(mh.Job())

		if err != nil {
			return fmt.Errorf("generate key: %v", err)
		}

		uniqueUntil, ok := uniqueUntilValue.(string)
		if !ok {
			// "success" is the default value
			// instead of throwing an error we can assume the default
			uniqueUntil = string(client.UntilSuccess)
			util.Info("unable to convert unique_until to a string, using default value for 'unique_until'")
		}

		if uniqueUntil == releaseAt {
			released, err := u.Server.Manager().Redis().Unlink(ctx, key).Result()
			// we can ignore the error since all unique keys have an expiration
			// the worst case is we are unable to queue up another job with the same parameters
			if err != nil {
				util.Info(fmt.Sprintf("Unable to release lock %s", key))
			}
			// if released is less than 0 then the lock has already expired
			if released > 0 {
				util.Info(fmt.Sprintf("Releasing lock (at=%s) %s", releaseAt, key))
			}
		}

		return next()
	}
}

func (u *UniqSubsystem) generateKey(job *client.Job) (string, error) {
	data, err := json.Marshal(job.Args)
	if err != nil {
		return "", fmt.Errorf("marshal job args to JSON: %v", err)
	}
	h := sha256.New()
	if _, err := fmt.Fprintf(h, "%s-%s-%s", job.Queue, job.Type, string(data)); err != nil {
		return "", fmt.Errorf("write key to hasher: %v", err)
	}

	key := hex.EncodeToString(h.Sum(nil))
	return key, nil
}
