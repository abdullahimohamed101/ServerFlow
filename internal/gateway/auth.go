package gateway

import (
	"errors"
	"net/http"
	"strings"

	"serverflow/internal/api"
	"serverflow/internal/auth"
)

// Option configures a Server at construction.
type Option func(*Server)

// WithAuthenticator turns on client authentication: every /v1 request must then carry a valid API
// key as "Authorization: Bearer <key>". /healthz, /readyz and /metrics stay open. Without the
// option (the default, auth.mode=off) the gateway behaves exactly as before.
//
// Passing a nil authenticator does not turn authentication off: the server then refuses to Serve
// and answers every /v1 request with an error, so a wiring mistake cannot leave the API open.
func WithAuthenticator(a *auth.Authenticator) Option {
	return func(s *Server) { s.authn, s.authRequired = a, true }
}

// SetAuthenticator is WithAuthenticator for a Server that already exists. Call it before Serve.
func (s *Server) SetAuthenticator(a *auth.Authenticator) { WithAuthenticator(a)(s) }

// AuthRequired reports whether /v1 requires an API key.
func (s *Server) AuthRequired() bool { return s.authRequired }

// Authentication failure reasons, for logs and the rejection counter. They never reach a client:
// the response for every 401 is identical.
const (
	rejectMissing     = "missing"
	rejectMalformed   = "invalid"
	rejectRevoked     = "revoked"
	rejectExpired     = "expired"
	rejectSuspended   = "suspended"
	rejectUnavailable = "unavailable"
	rejectFault       = "fault"
)

// authenticate guards a /v1 handler. With no authenticator configured it is a pass-through.
// Authentication runs before the handler reads the request body, so an unauthenticated client
// cannot make the gateway read, parse or buffer anything it sends.
func (s *Server) authenticate(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authRequired {
			next(w, r)
			return
		}
		info := infoFrom(r.Context())
		if s.authn == nil { // required but not wired: fail closed
			s.reject(w, r, info, api.ErrInternal(), rejectFault)
			return
		}
		bearer, ok := bearerToken(r)
		if !ok {
			s.reject(w, r, info, api.ErrUnauthorized(), rejectMissing)
			return
		}
		p, err := s.authn.Authenticate(r.Context(), bearer)
		switch {
		case err == nil:
			info.tenantID, info.apiKeyID, info.principal = p.TenantID, p.KeyID, p
			next(w, r)
		case errors.Is(err, auth.ErrBadRecord):
			// One key's database row is unreadable: an operator problem, not the client's, and not an
			// outage. Say so plainly without saying anything about the key.
			s.reject(w, r, info, api.ErrInternal(), rejectFault)
		case errors.Is(err, auth.ErrUnavailable):
			s.reject(w, r, info, api.ErrAuthUnavailable(), rejectUnavailable)
		case errors.Is(err, auth.ErrSuspended):
			// The caller proved it holds a valid key, so telling it the tenant is suspended leaks nothing.
			s.reject(w, r, info, api.ErrForbidden(), rejectSuspended)
		case errors.Is(err, auth.ErrRevoked):
			s.reject(w, r, info, api.ErrUnauthorized(), rejectRevoked)
		case errors.Is(err, auth.ErrExpired):
			s.reject(w, r, info, api.ErrUnauthorized(), rejectExpired)
		default:
			s.reject(w, r, info, api.ErrUnauthorized(), rejectMalformed)
		}
	})
}

// reject answers an unauthenticated request. The reason goes to the log and the counter only.
func (s *Server) reject(w http.ResponseWriter, r *http.Request, info *reqInfo, e *api.Error, reason string) {
	info.errCode = e.Code
	info.authReject = reason
	s.rejected(r.Context(), info, RejectAuth, reason, e.HTTPStatus)
	// The request body is never read. Without this net/http would try to drain it before replying,
	// which lets an unauthenticated client that stalls mid-body hold the response (and the
	// connection) hostage; with it the reply goes out at once and the connection is closed.
	w.Header().Set("Connection", "close")
	api.WriteError(w, e)
}

// bearerToken returns the token of a request's single Authorization header if it uses the Bearer
// scheme. Nothing is trimmed: the key parser accepts exactly one spelling.
func bearerToken(r *http.Request) (string, bool) {
	vals := r.Header.Values("Authorization")
	if len(vals) != 1 {
		return "", false
	}
	const scheme = "bearer "
	v := vals[0]
	if len(v) <= len(scheme) || !strings.EqualFold(v[:len(scheme)], scheme) {
		return "", false
	}
	return v[len(scheme):], true
}

// tenantAttrs returns log attributes naming the authenticated tenant and key (never the key itself).
func tenantAttrs(info *reqInfo) []any {
	if info.tenantID == "" {
		return nil
	}
	return []any{"tenant_id", info.tenantID, "api_key_id", info.apiKeyID}
}
