package uniq

import (
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

type Uniq struct {
	Server *server.Server
}

type Lifecycle struct {
	Uniq *Uniq
}

func Subsystem() *Lifecycle {
	return &Lifecycle{}
}

func (l *Lifecycle) Start(s *server.Server) error {
	uniq := newUniq(s)
	l.Uniq = uniq
	l.Uniq.AddMiddleware()
	util.Info("Uniq subsystem started")
	return nil
}

func (l *Lifecycle) Name() string {
	return "Uniq"
}

func (l *Lifecycle) Reload(s *server.Server) error {
	return nil
}

func (l *Lifecycle) Shutdown(s *server.Server) error {
	return nil
}

func newUniq(s *server.Server) *Uniq {
	return &Uniq{
		Server: s,
	}
}

func (u *Uniq) AddMiddleware() {
	u.Server.Manager().AddMiddleware("push", u.LockMiddleware)
	u.Server.Manager().AddMiddleware("fetch", u.ReleaseLockMiddleware(string(client.UntilStart)))
	u.Server.Manager().AddMiddleware("ack", u.ReleaseLockMiddleware(string(client.UntilSuccess)))
}

func (u *Uniq) LockMiddleware(next func() error, ctx manager.Context) error {
	var uniqueFor float64
	uniqueForValue, ok := ctx.Job().GetCustom("unique_for")
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

	uniqueUntilValue, ok := ctx.Job().GetCustom("unique_until")
	if ok {
		uniqueUntil, ok := uniqueUntilValue.(string)
		if !ok || (uniqueUntil != string(client.UntilStart) && uniqueUntil != string(client.UntilSuccess)) {
			return manager.Halt("ERR", "invalid value for unique_until.")
		}
	} else {
		ctx.Job().SetUniqueness(client.UntilSuccess)
	}

	key, err := u.generateKey(ctx.Job())
	if err != nil {
		return fmt.Errorf("Unable to generate key %w", err)
	}

	lockTime := time.Duration(uniqueFor) * time.Second
	if ctx.Job().At != "" {
		t, err := util.ParseTime(ctx.Job().At)
		if err != nil {
			return fmt.Errorf("Invalid timestamp for 'at': '%s'", ctx.Job().At)
		}
		if t.After(time.Now()) {
			addedTime := time.Duration(time.Until(t))
			lockTime += addedTime
		}

	}
	status, err := u.Server.Manager().Redis().SetNX(key, ctx.Job().Jid, lockTime).Result()
	if err != nil {
		return fmt.Errorf("Redis unable to set key: %w", err)
	}
	if !status {
		return manager.Halt("NOTUNIQUE", "Job has already been queued.")
	}
	util.Info(fmt.Sprintf("Locking key: %s for %d seconds", key, lockTime/time.Second))

	return next()
}

func (u *Uniq) ReleaseLockMiddleware(releaseAt string) func(next func() error, ctx manager.Context) error {
	return func(next func() error, ctx manager.Context) error {
		_, ok := ctx.Job().GetCustom("unique_for")
		if !ok {
			return next()
		}

		var uniqueUntil string

		uniqueUntilValue, ok := ctx.Job().GetCustom("unique_until")

		if !ok {
			return next()
		}

		key, err := u.generateKey(ctx.Job())

		if err != nil {
			return fmt.Errorf("Unable to generate key %w", err)
		}

		uniqueUntil = uniqueUntilValue.(string)

		if uniqueUntil == releaseAt {
			released, _ := u.Server.Manager().Redis().Unlink(key).Result()
			if released > 0 {
				util.Info(fmt.Sprintf("Releasing lock (at=%s) %s", releaseAt, key))
			}
		}

		return next()
	}
}

func (u *Uniq) generateKey(job *client.Job) (string, error) {
	data, err := json.Marshal(job.Args)
	if err != nil {
		return "", err
	}
	keyString := fmt.Sprintf("%s-%s-%s", job.Queue, job.Type, string(data))
	h := sha256.New()
	_, err = h.Write([]byte(keyString))
	if err != nil {
		return "", err
	}
	key := hex.EncodeToString(h.Sum(nil))
	return key, nil
}
