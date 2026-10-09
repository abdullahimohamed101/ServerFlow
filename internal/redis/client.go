package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ErrBackoff is returned instead of calling Redis while it is being left alone after a failure (or while
// another caller is probing it).
var ErrBackoff = errors.New("redis: unavailable, backing off after a recent failure")

// Client is a Redis connection pool with health tracking. It is safe for concurrent use.
type Client struct {
	rdb     *goredis.Client
	cfg     Config
	log     *slog.Logger
	now     func() time.Time
	secret  string
	backoff time.Duration
	timeout time.Duration

	cmds atomic.Int64 // commands sent to the server

	mu        sync.Mutex
	down      bool      // between the first failed call and the next success
	downUntil time.Time // while down, Redis is not called before this
	probing   bool      // a call is testing whether Redis is back
	since     time.Time // when the current outage began
}

// New builds a client. It does not connect: the first command (or Ping) does. The transport is not
// checked here; call CheckTransport first where it applies.
func New(cfg Config) (*Client, error) {
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, secret: opts.Password, log: cfg.Logger, now: cfg.Now, backoff: cfg.Backoff, timeout: opts.ReadTimeout}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.backoff <= 0 {
		c.backoff = time.Second
	}
	c.rdb = goredis.NewClient(opts)
	c.rdb.AddHook(countHook{&c.cmds})
	return c, nil
}

// countHook counts every command (and pipelined command) the driver sends.
type countHook struct{ n *atomic.Int64 }

func (h countHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }
func (h countHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}
func (h countHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		h.n.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// Commands returns how many commands this client has sent to the server.
func (c *Client) Commands() int64 { return c.cmds.Load() }

// Close releases the pool.
func (c *Client) Close() error { return c.rdb.Close() }

// Ping checks that Redis answers (and accepts the password). It does not touch the health state: it is
// for a startup check.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.scrub(c.rdb.Ping(ctx).Err())
}

// PoolStats reports the connections the pool holds, for leak tests.
func (c *Client) PoolStats() (total, idle uint32) {
	s := c.rdb.PoolStats()
	return s.TotalConns, s.IdleConns
}

// Script is a Lua script, loaded once per server and invoked by digest.
type Script struct {
	s   *goredis.Script
	len int
}

// NewScript prepares a script whose reply is an array of replyLen integers; any other reply counts as a
// failure of Redis. Nothing is sent until it runs.
func NewScript(src string, replyLen int) *Script {
	return &Script{s: goredis.NewScript(src), len: replyLen}
}

// Run runs a script whose reply is an array of integers. The call is bounded by the client's timeout and
// is detached from ctx's cancellation (a client that hangs up must not abandon a script that already took
// effect, nor be mistaken for a Redis failure); ctx's values still apply. While Redis is down it returns
// ErrBackoff without calling it. Any other error counts as a failure of Redis and starts a backoff.
func (c *Client) Run(ctx context.Context, s *Script, keys []string, args ...any) ([]int64, error) {
	probe, err := c.admit()
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
	defer cancel()
	res, err := s.s.Run(cctx, c.rdb, keys, args...).Result()
	if err == nil {
		var out []int64
		if out, err = ints(res, s.len); err == nil {
			c.done(probe, nil)
			return out, nil
		}
	}
	err = c.scrub(err)
	c.done(probe, err)
	return nil, err
}

func ints(res any, want int) ([]int64, error) {
	arr, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redis: script returned %T, want an array", res)
	}
	if len(arr) != want {
		return nil, fmt.Errorf("redis: script returned %d values, want %d", len(arr), want)
	}
	out := make([]int64, len(arr))
	for i, v := range arr {
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("redis: script returned %T at %d, want an integer", v, i)
		}
		out[i] = n
	}
	return out, nil
}

// Set stores key = value with an expiry. It is for best-effort data: it is skipped (ErrBackoff) while Redis
// is down, and its failures neither start nor extend a backoff.
func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if down, _ := c.Down(); down {
		return ErrBackoff
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
	defer cancel()
	return c.scrub(c.rdb.Set(cctx, key, value, ttl).Err())
}

// Get reads a key; it is for tests and debugging, not the request path.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rdb.Get(cctx, key).Result()
	return v, c.scrub(err)
}

// TTL returns a key's remaining life, for tests and debugging.
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	d, err := c.rdb.PTTL(cctx, key).Result()
	return d, c.scrub(err)
}

// Down reports whether Redis is being left alone, and for how long at least (zero when a probe is due).
func (c *Client) Down() (bool, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.down {
		return false, 0
	}
	if d := c.downUntil.Sub(c.now()); d > 0 {
		return true, d
	}
	return true, 0
}

// RetryAfter is how long a caller turned away by an outage should wait: the rest of the backoff, or a
// full backoff interval when a probe is already running.
func (c *Client) RetryAfter() time.Duration {
	if _, d := c.Down(); d > 0 {
		return d
	}
	return c.backoff
}

// admit decides whether a call may go to Redis. probe is true for the one call allowed through once the
// backoff has elapsed.
func (c *Client) admit() (probe bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.down {
		return false, nil
	}
	if c.now().Before(c.downUntil) || c.probing {
		return false, ErrBackoff
	}
	c.probing = true
	return true, nil
}

// done records the outcome of a call that went to Redis. The first failure of an outage is logged, and
// so is the first success after it; failures in between are not.
func (c *Client) done(probe bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if probe {
		c.probing = false
	}
	now := c.now()
	if err != nil {
		if !c.down {
			c.down, c.since = true, now
			c.log.Warn("redis unavailable: the rate limiter is failing per its failure mode and will retry shortly",
				"component", "redis", "error", err.Error(), "backoff", c.backoff.String())
		}
		c.downUntil = now.Add(c.backoff)
		return
	}
	if c.down {
		c.down = false
		c.log.Info("redis recovered", "component", "redis", "outage", now.Sub(c.since).Round(time.Millisecond).String())
	}
}

// scrub removes the password from an error text, in case the driver ever echoes it.
func (c *Client) scrub(err error) error {
	if err == nil || c.secret == "" || !strings.Contains(err.Error(), c.secret) {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), c.secret, "<redacted>"))
}
