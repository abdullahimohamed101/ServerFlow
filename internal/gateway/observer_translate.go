package gateway

import (
	"context"
	"net/http"
	"time"

	"serverflow/internal/api"
)

// This file is the only place reqInfo and friends are turned into observer events, so a change to reqInfo
// touches one file. The events copy plain values; nothing here passes a pointer to gateway state.

// isInferenceRequest reports whether the request is one the observer lifecycle covers.
func isInferenceRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == chatCompletionsPath
}

func capHeader(v string) string {
	if len(v) > MaxTraceHeaderBytes {
		return v[:MaxTraceHeaderBytes]
	}
	return v
}

func requestStartFrom(r *http.Request, id string, at time.Time) RequestStart {
	e := RequestStart{ID: id, Method: r.Method, Path: r.URL.Path, Time: at}
	if v := r.Header["Traceparent"]; len(v) > 0 {
		e.TraceHeaders.Traceparent = capHeader(v[0])
	}
	if v := r.Header["Tracestate"]; len(v) > 0 {
		e.TraceHeaders.Tracestate = capHeader(v[0])
	}
	return e
}

func (s *Server) admitted(ctx context.Context, info *reqInfo) {
	s.obs.RequestAdmitted(ctx, Admission{
		RequestID: info.id, Model: info.requested, Stream: info.stream, TenantID: info.tenantID,
		EstimatedCost: info.cost, RateLimitChecked: info.rateChecked, RateLimitDuration: info.rateDuration,
		RateLimitBypassed: info.rateBypassed,
	})
}

// rejected tells the observers a request was refused before it reached a worker.
func (s *Server) rejected(ctx context.Context, info *reqInfo, kind, reason string, status int) {
	dur := time.Duration(0)
	switch kind {
	case RejectRateLimit:
		dur = info.rateDuration
	case RejectModel, RejectCapacity, RejectInternal:
		dur = info.selectDuration
	}
	s.obs.RequestRejected(ctx, Rejection{
		RequestID: info.id, TenantID: info.tenantID, Kind: kind, Reason: reason, Status: status,
		DecisionDuration: dur, Model: info.model,
	})
}

// rejectedAPI is rejected for a refusal that is an *api.Error: the kind follows the error code.
func (s *Server) rejectedAPI(ctx context.Context, info *reqInfo, e *api.Error) {
	kind, reason := RejectValidation, "invalid_request"
	switch e.Code {
	case api.CodeInvalidRequest:
		if e.HTTPStatus == http.StatusRequestEntityTooLarge {
			reason = "body_too_large"
		}
	case api.CodeModelNotFound:
		kind, reason = RejectModel, "model_not_found"
	case api.CodeForbidden:
		kind, reason = RejectModel, "model_forbidden"
	case api.CodeNoCapacity:
		kind, reason = RejectCapacity, "no_capacity"
	case api.CodeWorkerUnavailable:
		kind, reason = RejectCapacity, "worker_unavailable"
	default:
		kind, reason = RejectInternal, "internal"
	}
	s.rejected(ctx, info, kind, reason, e.HTTPStatus)
}

func completionFrom(info *reqInfo, status int, d time.Duration) Completion {
	return Completion{
		RequestID: info.id, Model: info.model, Stream: info.stream, TenantID: info.tenantID, Status: status,
		ErrorCode: info.errCode, Duration: d, Attempts: info.attempted, TTFT: info.ttft, Handled: info.inference,
	}
}

func (s *Server) attemptStartEvent(info *reqInfo, number int, worker, model, strategy, state string, eligible bool, selectDur time.Duration) AttemptStart {
	return AttemptStart{
		RequestID: info.id, AttemptID: info.attemptID, Number: number, WorkerID: worker, Model: model, Strategy: strategy,
		SelectDuration: selectDur, SinceRequestStart: time.Since(info.start), WorkerState: state, WorkerEligible: eligible,
	}
}

// attemptContext is the context the observers returned for the current attempt, or ctx before one exists.
func attemptContext(ctx context.Context, info *reqInfo) context.Context {
	if info.attemptCtx != nil {
		return info.attemptCtx
	}
	return ctx
}

// endStaticAttempt closes the single attempt of a static-mode request. The outcome follows the registry
// path's rule: the client left, or the attempt failed (an error code, a panic, or a worker 5xx).
func (s *Server) endStaticAttempt(ctx context.Context, info *reqInfo, began time.Time, upstreamStatus int, panicked bool) {
	outcome := outcomeOK
	switch {
	case info.clientClosed:
		outcome = outcomeClientClosed
	case panicked, info.errCode != "", upstreamStatus >= 500:
		outcome = outcomeFailed
	}
	s.obs.AttemptEnded(ctx, AttemptEnd{
		RequestID: info.id, AttemptID: info.attemptID, Number: 1, Model: info.model, Outcome: outcome, Duration: time.Since(began),
	})
}
