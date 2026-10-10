package redis

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// Error kinds counted by redis_errors_total.
const (
	errTimeout = iota
	errConnection
	errScript
	errOther
	numErrorKinds
)

var errorKindNames = [numErrorKinds]string{"timeout", "connection", "script", "other"}

// classify puts a failed Redis call into one of the fixed kinds. The error text is never used as a label.
func classify(err error) int {
	var ne net.Error
	var re goredis.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()):
		return errTimeout
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, goredis.ErrClosed) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.As(err, &ne):
		return errConnection
	case errors.As(err, &re):
		return errScript
	}
	return errOther
}

// noteError counts a failed call that reached Redis. A backoff refusal (ErrBackoff, ErrBusy) never went to
// Redis and is not counted; a missing key (redis.Nil) is not a failure.
func (c *Client) noteError(err error) {
	if err == nil || errors.Is(err, ErrBackoff) || errors.Is(err, ErrBusy) || errors.Is(err, goredis.Nil) {
		return
	}
	c.errs[classify(err)].Add(1)
}

// Collector exports the client's health and traffic at scrape time: redis_up (0 while the client is leaving
// Redis alone after a failure), commands sent, failures by kind, and the connection pool.
func (c *Client) Collector() prometheus.Collector { return &collector{c: c} }

type collector struct{ c *Client }

var (
	upDesc       = prometheus.NewDesc("redis_up", "1 when the gateway is talking to Redis, 0 while it is backing off after a failure.", nil, nil)
	commandsDesc = prometheus.NewDesc("redis_commands_total", "Commands sent to Redis.", nil, nil)
	errorsDesc   = prometheus.NewDesc("redis_errors_total", "Failed Redis calls, by kind (timeout, connection, script or other).", []string{"kind"}, nil)
	poolDesc     = prometheus.NewDesc("redis_pool_connections", "Connections in the Redis pool, by state (total, idle or max).", []string{"state"}, nil)
)

func (*collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- upDesc
	ch <- commandsDesc
	ch <- errorsDesc
	ch <- poolDesc
}

func (k *collector) Collect(ch chan<- prometheus.Metric) {
	down, _ := k.c.Down()
	up := 1.0
	if down {
		up = 0
	}
	ch <- prometheus.MustNewConstMetric(upDesc, prometheus.GaugeValue, up)
	ch <- prometheus.MustNewConstMetric(commandsDesc, prometheus.CounterValue, float64(k.c.Commands()))
	for i := range errorKindNames {
		ch <- prometheus.MustNewConstMetric(errorsDesc, prometheus.CounterValue, float64(k.c.errs[i].Load()), errorKindNames[i])
	}
	total, idle := k.c.PoolStats()
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(total), "total")
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(idle), "idle")
	size := k.c.cfg.PoolSize
	if size <= 0 {
		size = DefaultPoolSize
	}
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(size), "max")
}
