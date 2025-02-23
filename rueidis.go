package echoratelimiter

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/labstack/echo/v4/middleware"
	"github.com/redis/rueidis"
	"golang.org/x/time/rate"
)

var (
	//go:embed set_or_incr.lua
	setIncr string
)

type RueidisRateLimiter struct {
	ctx    context.Context
	client rueidis.Client
	rate   rate.Limit
	window time.Duration
	prefix string
	logger Logger

	exec *rueidis.Lua
}

type RueidisRateLimiterConfig struct {
	Client rueidis.Client

	// Rate of requests allowed
	Rate rate.Limit

	// The expiry time of the redis object
	Window time.Duration

	// A prefix for the rate limiting key used for the redis key
	Prefix string

	// An optional logger to log details about usage/limits
	Logger Logger
}

func NewRueidisRateLimiter(ctx context.Context, conf RueidisRateLimiterConfig) *RueidisRateLimiter {
	if conf.Logger == nil {
		conf.Logger = &nilLogger{}
	}
	return &RueidisRateLimiter{
		ctx:    ctx,
		client: conf.Client,
		rate:   conf.Rate,
		window: conf.Window,
		prefix: conf.Prefix,
		logger: conf.Logger,
		exec:   rueidis.NewLuaScript(setIncr),
	}
}

func (r *RueidisRateLimiter) Allow(identifier string) (bool, error) {
	key := r.visitorKey(identifier)
	current, err := r.getCount(key)
	if err != nil {
		return false, err
	}

	if current <= int(r.rate) {
		r.logger.Debug(
			"allowing request",
			"identifier",
			identifier,
			"rate",
			r.rate,
			"hits",
			current,
		)
		return true, nil
	}
	return false, nil
}

func (r *RueidisRateLimiter) getCount(key string) (int, error) {
	res := r.exec.Exec(
		r.ctx,
		r.client,
		[]string{key},
		[]string{fmt.Sprintf("%d", int(r.window.Seconds()))},
	)
	if err := res.Error(); err != nil {
		return 0, err
	}
	val, err := res.AsInt64()
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

func (r *RueidisRateLimiter) visitorKey(identifier string) string {
	out := []string{"rate_limit"}
	if r.prefix != "" {
		out = append(out, r.prefix)
	}
	out = append(out, identifier)
	return strings.Join(out, ":")
}

var _ middleware.RateLimiterStore = &RueidisRateLimiter{}
