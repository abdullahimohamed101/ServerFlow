package protocol

import (
	"fmt"
	"math"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// WorkerState is a worker's lifecycle state (spec section 11).
//
// A worker reports REGISTERING through DRAINING and FAILED itself. UNHEALTHY
// and LOST are never reported: the registry derives them from heartbeat age.
type WorkerState string

const (
	StateRegistering  WorkerState = "REGISTERING"
	StateLoadingModel WorkerState = "LOADING_MODEL"
	StateWarming      WorkerState = "WARMING"
	StateReady        WorkerState = "READY"
	StateDraining     WorkerState = "DRAINING"
	StateTerminated   WorkerState = "TERMINATED"
	StateFailed       WorkerState = "FAILED"
	StateUnhealthy    WorkerState = "UNHEALTHY"
	StateLost         WorkerState = "LOST"
)

// Reportable reports whether a worker may report s in a heartbeat.
func (s WorkerState) Reportable() bool {
	switch s {
	case StateRegistering, StateLoadingModel, StateWarming, StateReady, StateDraining, StateFailed:
		return true
	}
	return false
}

// CanTransition reports whether a worker whose last reported state is from may
// report to next, judged on that one step. FAILED can recover to any state a
// starting worker could reach. A drain is one-way for the whole incarnation,
// including through FAILED (a worker that drained must register again to
// serve), which needs history and so is enforced by the registry.
func CanTransition(from, next WorkerState) bool {
	if !next.Reportable() {
		return false
	}
	switch from {
	case StateRegistering:
		return true
	case StateLoadingModel:
		return next != StateRegistering
	case StateWarming:
		return next == StateWarming || next == StateReady || next == StateDraining || next == StateFailed
	case StateReady:
		return next == StateReady || next == StateDraining || next == StateFailed
	case StateDraining:
		return next == StateDraining || next == StateFailed
	case StateFailed:
		return next != StateRegistering
	}
	return false
}

// Health is the registry's judgement of a worker from heartbeat age.
type Health string

const (
	HealthHealthy   Health = "healthy"
	HealthSuspect   Health = "suspect"
	HealthUnhealthy Health = "unhealthy"
	HealthLost      Health = "lost"
)

// Limits on registration and heartbeat fields.
const (
	MaxWorkerIDLen     = 64
	MaxModelLen        = 128
	MaxAddressLen      = 2048
	MaxConcurrencyCap  = 100000
	MaxQueueSizeCap    = 1000000
	MaxReasonLen       = 256
	MaxRegistrationLen = 128
	maxCounter         = int64(1) << 40
)

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidWorkerID reports whether id is an acceptable worker ID.
func ValidWorkerID(id string) bool { return workerIDPattern.MatchString(id) }

// WorkerInfo is what a worker states when it registers.
type WorkerInfo struct {
	WorkerID string `json:"worker_id"`
	Model    string `json:"model"`
	// Address is the base URL the gateway uses to reach the worker.
	Address        string `json:"address"`
	MaxConcurrency int    `json:"max_concurrency"`
	QueueSize      int    `json:"queue_size"`
}

// Validate checks every field. Error messages never include the address,
// which could carry credentials.
func (w WorkerInfo) Validate() error {
	if !ValidWorkerID(w.WorkerID) {
		return fmt.Errorf("worker_id must be 1-%d characters of letters, digits, '.', '_' or '-', starting with a letter or digit", MaxWorkerIDLen)
	}
	if err := validModel(w.Model); err != nil {
		return err
	}
	if err := ValidateAddress(w.Address); err != nil {
		return err
	}
	if w.MaxConcurrency < 1 || w.MaxConcurrency > MaxConcurrencyCap {
		return fmt.Errorf("max_concurrency must be in 1-%d", MaxConcurrencyCap)
	}
	if w.QueueSize < 0 || w.QueueSize > MaxQueueSizeCap {
		return fmt.Errorf("queue_size must be in 0-%d", MaxQueueSizeCap)
	}
	return nil
}

func validModel(m string) error {
	if m == "" || len(m) > MaxModelLen {
		return fmt.Errorf("model must be 1-%d characters", MaxModelLen)
	}
	if !cleanText(m, false) {
		return fmt.Errorf("model must be valid text without whitespace, control, or invisible formatting characters")
	}
	return nil
}

// cleanText reports whether s is valid UTF-8 free of control characters and of
// invisible formatting characters (zero-width, bidirectional overrides, byte
// order marks, soft hyphens), which could disguise a value or corrupt a
// terminal or dashboard that later shows it. Spaces are allowed only if
// allowSpace is set.
func cleanText(s string, allowSpace bool) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || (!allowSpace && unicode.IsSpace(r)) {
			return false
		}
	}
	return true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ValidateAddress checks a worker base URL: http or https, with a host, no
// credentials, query, or fragment, no ".." path segment, and, if the host is an
// IP literal, not an unspecified, link-local, or multicast address (which
// includes the cloud metadata address 169.254.169.254). Loopback, private
// ranges, and hostnames are legitimate. The error never echoes the value.
func ValidateAddress(addr string) error {
	if addr == "" || len(addr) > MaxAddressLen {
		return fmt.Errorf("address must be an http(s) URL with a host")
	}
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("address must be an http(s) URL with a host")
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("address must not contain credentials, a query, or a fragment")
	}
	if err := validateHost(u.Hostname()); err != nil {
		return err
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("address port must be 1-65535")
		}
	}
	for _, r := range u.Path {
		if r <= ' ' || r == 0x7f {
			return fmt.Errorf("address path must not contain spaces or control characters")
		}
	}
	for _, seg := range strings.Split(u.Path, "/") {
		if seg == ".." {
			return fmt.Errorf("address path must not contain '..'")
		}
	}
	return nil
}

// validateHost accepts only a canonical IP literal or a strict ASCII DNS name.
// Anything else (fullwidth digits, decimal/hex/octal integers, zone ids, a
// trailing dot) is refused outright, because Go's dialer and the OS resolver
// accept spellings that net.ParseIP does not, and each could reach an address
// the policy below is meant to exclude.
func validateHost(host string) error {
	bad := fmt.Errorf("address host must be a canonical IP address or an ASCII hostname")
	if host == "" || len(host) > 253 {
		return bad
	}
	if strings.ContainsAny(host, ":%") { // an IPv6 literal (brackets already stripped); a zone id contains '%'
		addr, err := netip.ParseAddr(host)
		if err != nil || addr.Zone() != "" || !addr.Is6() {
			return bad
		}
		return checkRoutable(addr.Unmap())
	}
	labels := strings.Split(host, ".")
	for _, l := range labels {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return bad
		}
		for i := 0; i < len(l); i++ {
			c := l[i]
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
				return bad
			}
		}
	}
	// inet_aton-style spellings: a numeric last label means the host is meant as an IPv4
	// address, so it must be a canonical dotted quad.
	last := strings.ToLower(labels[len(labels)-1])
	if strings.HasPrefix(last, "0x") || strings.Trim(last, "0123456789") == "" {
		addr, err := netip.ParseAddr(host)
		if err != nil || !addr.Is4() {
			return bad
		}
		return checkRoutable(addr)
	}
	return nil
}

func checkRoutable(a netip.Addr) error {
	if ForbiddenAddr(a) {
		return fmt.Errorf("address host must be a routable address, not unspecified, link-local, multicast, or broadcast")
	}
	return nil
}

// metadataAddrs are cloud instance-metadata endpoints outside the link-local
// block: Azure's wire server, Alibaba's metadata service, and AWS's IPv6 one.
var metadataAddrs = map[netip.Addr]bool{
	netip.AddrFrom4([4]byte{168, 63, 129, 16}):   true,
	netip.AddrFrom4([4]byte{100, 100, 100, 200}): true,
	netip.MustParseAddr("fd00:ec2::254"):         true,
}

// ForbiddenAddr reports whether a is never a valid place to send inference
// traffic: unspecified, link-local (which includes the cloud metadata address
// 169.254.169.254), multicast, broadcast, the IPv4 "this network" block, or a
// well-known cloud metadata endpoint. An IPv4-mapped IPv6 address, and an
// address that embeds an IPv4 one through NAT64 (64:ff9b::/96) or 6to4
// (2002::/16), is judged as the IPv4 address it wraps.
func ForbiddenAddr(a netip.Addr) bool {
	a = a.Unmap().WithZone("")
	if a.IsUnspecified() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() || a.IsInterfaceLocalMulticast() ||
		a.IsMulticast() || a == netip.AddrFrom4([4]byte{255, 255, 255, 255}) || (a.Is4() && a.As4()[0] == 0) ||
		metadataAddrs[a] {
		return true
	}
	if a.Is6() {
		b := a.As16()
		switch {
		case b[0] == 0x00 && b[1] == 0x64 && b[2] == 0xff && b[3] == 0x9b && allZero(b[4:12]):
			return ForbiddenAddr(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
		case b[0] == 0x00 && b[1] == 0x64 && b[2] == 0xff && b[3] == 0x9b && b[4] == 0x00 && b[5] == 0x01:
			return true // 64:ff9b:1::/48, local-use NAT64: the embedded address sits at a variable offset
		case b[0] == 0x20 && b[1] == 0x02:
			return ForbiddenAddr(netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}))
		case allZero(b[:12]) && !a.IsLoopback():
			// ::a.b.c.d, the deprecated IPv4-compatible form
			return ForbiddenAddr(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
		case allZero(b[:8]) && b[8] == 0xff && b[9] == 0xff && allZero(b[10:12]):
			// ::ffff:0:a.b.c.d, SIIT
			return ForbiddenAddr(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
		}
	}
	return false
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// Metrics is a worker's load as reported in a heartbeat (spec section 10).
// GPU fields are optional; a worker without a GPU omits them.
type Metrics struct {
	ActiveRequests        int      `json:"active_requests"`
	QueueDepth            int      `json:"queue_depth"`
	QueuedInputTokens     int      `json:"queued_input_tokens"`
	RecentTokensPerSecond float64  `json:"recent_tokens_per_second"`
	GPUUtilization        *float64 `json:"gpu_utilization,omitempty"`
	GPUMemoryUsedMB       *int64   `json:"gpu_memory_used_mb,omitempty"`
}

// Validate rejects negative, absurd, or non-finite values.
func (m Metrics) Validate() error {
	for name, v := range map[string]int64{
		"active_requests": int64(m.ActiveRequests), "queue_depth": int64(m.QueueDepth), "queued_input_tokens": int64(m.QueuedInputTokens),
	} {
		if v < 0 || v > maxCounter {
			return fmt.Errorf("%s is out of range", name)
		}
	}
	if !finite(m.RecentTokensPerSecond) || m.RecentTokensPerSecond < 0 || m.RecentTokensPerSecond > 1e9 {
		return fmt.Errorf("recent_tokens_per_second is out of range")
	}
	if m.GPUUtilization != nil && (!finite(*m.GPUUtilization) || *m.GPUUtilization < 0 || *m.GPUUtilization > 100) {
		return fmt.Errorf("gpu_utilization must be in 0-100")
	}
	if m.GPUMemoryUsedMB != nil && (*m.GPUMemoryUsedMB < 0 || *m.GPUMemoryUsedMB > maxCounter) {
		return fmt.Errorf("gpu_memory_used_mb is out of range")
	}
	return nil
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// Heartbeat is a worker's periodic report. The registry timestamps it on
// receipt; worker clocks are never trusted.
type Heartbeat struct {
	RegistrationID string      `json:"registration_id"`
	State          WorkerState `json:"state"`
	Metrics        Metrics     `json:"metrics"`
	// Reason explains a FAILED or DRAINING state for operators.
	Reason string `json:"reason,omitempty"`
}

// Validate checks the heartbeat's own fields (not whether the state change is
// legal, which depends on the worker's history).
func (h Heartbeat) Validate() error {
	if h.RegistrationID == "" || len(h.RegistrationID) > MaxRegistrationLen {
		return fmt.Errorf("registration_id is required")
	}
	if !h.State.Reportable() {
		return fmt.Errorf("state %q cannot be reported by a worker", truncate(string(h.State), 32))
	}
	if len(h.Reason) > MaxReasonLen || !cleanText(h.Reason, true) {
		return fmt.Errorf("reason must be at most %d characters of plain text", MaxReasonLen)
	}
	return h.Metrics.Validate()
}

// RegisterResponse is returned when a worker registers.
type RegisterResponse struct {
	// RegistrationID identifies this incarnation of the worker; heartbeats and
	// deregistration must present it. Treat it as a secret.
	RegistrationID string `json:"registration_id"`
	// HeartbeatIntervalSeconds is how often the control plane expects
	// heartbeats; the worker should follow it.
	HeartbeatIntervalSeconds float64 `json:"heartbeat_interval_seconds"`
}

// WorkerSnapshot is a consistent, read-only view of a worker (spec section
// 13). It deliberately omits the registration ID.
type WorkerSnapshot struct {
	WorkerID       string      `json:"worker_id"`
	Model          string      `json:"model"`
	Address        string      `json:"address"`
	MaxConcurrency int         `json:"max_concurrency"`
	QueueSize      int         `json:"queue_size"`
	State          WorkerState `json:"state"`
	Health         Health      `json:"health"`
	// Eligible is true only for a READY worker with acceptable health.
	Eligible            bool      `json:"eligible"`
	Metrics             Metrics   `json:"metrics"`
	Reason              string    `json:"reason,omitempty"`
	RegisteredAt        time.Time `json:"registered_at"`
	LastHeartbeat       time.Time `json:"last_heartbeat"`
	HeartbeatAgeSeconds float64   `json:"heartbeat_age_seconds"`
}

// ModelInfo summarizes the workers serving one model.
type ModelInfo struct {
	Model    string `json:"model"`
	Workers  int    `json:"workers"`
	Eligible int    `json:"eligible"`
}
