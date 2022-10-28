package expire

import (
	"errors"
	"fmt"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Subsystem = &ExpireSubsystem{}

// ExpireSubsystem allows jobs to expire.
// It's implementation is similar to that of https://github.com/contribsys/faktory/wiki/Ent-Expiring-Jobs
// except that `expires_in` takes a duration string rather than seconds.
type ExpireSubsystem struct{}

// Start loads the plugin by installing the middlewares.
func (e *ExpireSubsystem) Start(s *server.Server) error {
	s.Manager().AddMiddleware("push", e.parseExpiration)
	s.Manager().AddMiddleware("fetch", e.skipExpiredJobs)
	util.Info("Loaded expiring jobs plugin")
	return nil
}

// Name returns the name of the plugin.
func (e *ExpireSubsystem) Name() string {
	return "ExpiringJobs"
}

// Reload does not do anything.
func (e *ExpireSubsystem) Reload(s *server.Server) error {
	return nil
}

// parseExpiration ensures that `expires_at` and `expires_in` are valid.
// If `expires_in` is set it will be parsed and used to set `expires_at`.
func (e *ExpireSubsystem) parseExpiration(next func() error, ctx manager.Context) error {
	// If `expires_at` is set then validate that it's a proper ISO 8601 timestamp.
	ea, aok := ctx.Job().GetCustom("expires_at")
	if aok {
		expires_at, ok := ea.(string)
		if !ok {
			return errors.New("expire: Could not cast expires_at")
		}
		if _, err := time.Parse(time.RFC3339Nano, expires_at); err != nil {
			return fmt.Errorf("expire: Could not parse expires_at: %w", err)
		}
	}

	// Set `expires_at` if `expires_in` is set
	if ei, iok := ctx.Job().GetCustom("expires_in"); iok {
		// Error out if `expires_at` is already set.
		if aok {
			return errors.New("expire: Can not queue job with both expires_at and expires_in set")
		}

		expires_in, ok := ei.(string)
		if !ok {
			return errors.New("expire: Could not cast expires_in")
		}
		duration, err := time.ParseDuration(expires_in)
		if err != nil {
			return fmt.Errorf("expire: Could not parse expires_in: %w", err)
		}
		ctx.Job().SetExpiresIn(duration)
	}

	return next()
}

// skipExpiredJobs is a FETCH chain middleware that ensures a job is not expired.
// If the job is expired it will return an error that instructs Faktory to
// restart the fetch.
func (e *ExpireSubsystem) skipExpiredJobs(next func() error, ctx manager.Context) error {
	if ea, ok := ctx.Job().GetCustom("expires_at"); ok {
		expires_at, err := time.Parse(time.RFC3339Nano, ea.(string))
		if err != nil {
			util.Warnf("expire: error parsing expires_at: %v", err)
			return next()
		}

		if time.Now().After(expires_at) {
			return manager.Discard("job expired")
		}
	}

	return next()
}
