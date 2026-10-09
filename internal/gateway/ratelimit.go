package gateway

import (
	"errors"
	"net/http"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/ratelimit"
)

// WithLimiter turns on rate limiting: after a request is parsed and its model allowed, and before any worker
// is touched, the limiter decides whether it may proceed. Without the option (the default,
// rate_limit.mode=off) the gateway behaves exactly as before.
//
// Passing a nil limiter does not turn limiting off: the server then refuses to Serve and answers every chat
// request with an error, so a wiring mistake cannot leave quotas unenforced. If the limiter has a
// Run(ctx) method, Serve runs it (lease renewal and release).
func WithLimiter(l ratelimit.Limiter) Option {
	return func(s *Server) {
		s.limiter, s.limitRequired = l, true
		s.registerLimiterStats(l)
	}
}

// limiterStats is what a limiter may expose for the gateway's metrics: the concurrency leases this process
// tracks, and releases that were not sent (the lease then holds its slot until it expires).
type limiterStats interface {
	LocalLeases() int
	DroppedReleases() int64
}

// registerLimiterStats exports rate_limit_local_leases and rate_limit_dropped_releases_total when the limiter
// can report them (a replaced limiter, or a fake without the methods, simply has none).
func (s *Server) registerLimiterStats(l ratelimit.Limiter) {
	st, ok := l.(limiterStats)
	if !ok || st == nil {
		return
	}
	s.metrics.registerLimiterStats(st)
}

// SetLimiter is WithLimiter for a Server that already exists. Call it before Serve.
func (s *Server) SetLimiter(l ratelimit.Limiter) { WithLimiter(l)(s) }

// RateLimitRequired reports whether chat requests are subject to rate limiting.
func (s *Server) RateLimitRequired() bool { return s.limitRequired }

// RequestRecorder receives best-effort request metadata (redis.request_metadata). Record must never block
// and never fail a request.
type RequestRecorder interface {
	Record(requestID, tenantID, model, worker string, attempt int)
}

// WithRequestRecorder records which worker served each request.
func WithRequestRecorder(r RequestRecorder) Option { return func(s *Server) { s.recorder = r } }

// requireLimitIfConfigured marks limiting as required when the mode says so. A later WithLimiter supplies it.
func requireLimitIfConfigured(mode string) Option {
	return func(s *Server) {
		if mode == "required" {
			s.limitRequired = true
		}
	}
}

// admit asks the limiter about a parsed request. On refusal it writes the response and returns ok=false;
// otherwise it returns the function that gives back the concurrency slot, which the caller must defer.
func (s *Server) admit(w http.ResponseWriter, r *http.Request, info *reqInfo, cost int, model string) (release func(), ok bool) {
	info.cost = cost
	var lim ratelimit.Limits
	if p := info.principal; p != nil {
		lim = ratelimit.Limits{RequestsPerMinute: p.Policy.RequestsPerMinute, TokensPerMinute: p.Policy.TokensPerMinute, MaxConcurrent: p.Policy.MaxConcurrent}
	}
	t0 := time.Now()
	d, err := s.limiter.Allow(r.Context(), ratelimit.Request{TenantID: info.tenantID, Model: model, Cost: cost, Limits: lim})
	s.metrics.observeRateDecision(time.Since(t0))
	switch {
	case err != nil:
		retry := 1
		var ue *ratelimit.UnavailableError
		if errors.As(err, &ue) {
			retry = ue.RetryAfterSeconds()
		}
		info.rateLimit = string(ratelimit.LimitUnavailable)
		s.metrics.observeRateReject(ratelimit.LimitUnavailable)
		s.log.Warn("rate limit check unavailable", append([]any{"request_id", info.id, "limit", info.rateLimit, "error", err.Error()}, tenantAttrs(info)...)...)
		s.fail(w, info, api.ErrRateLimitUnavailable(retry))
		return nil, false
	case !d.Allowed:
		info.rateLimit = string(d.Limit)
		s.metrics.observeRateReject(d.Limit)
		s.log.Info("rate limited", append([]any{"request_id", info.id, "limit", info.rateLimit, "est_cost", cost,
			"retry_after_s", d.RetryAfterSeconds()}, tenantAttrs(info)...)...)
		s.fail(w, info, api.ErrRateLimited(string(d.Limit), d.RetryAfterSeconds()))
		return nil, false
	}
	if d.Bypassed {
		info.rateBypassed = true
		s.metrics.rateBypassed.Inc()
	}
	if d.Release == nil {
		return func() {}, true
	}
	return d.Release, true
}

// recordWorker notes which worker a request went to, if a recorder is configured.
func (s *Server) recordWorker(info *reqInfo, worker string, attempt int) {
	if s.recorder != nil {
		s.recorder.Record(info.id, info.tenantID, info.model, worker, attempt)
	}
}
