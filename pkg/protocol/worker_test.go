package protocol

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func validInfo() WorkerInfo {
	return WorkerInfo{WorkerID: "worker-qwen-01", Model: "qwen-7b", Address: "http://10.0.0.5:8000", MaxConcurrency: 4, QueueSize: 32}
}

func TestWorkerInfoValidate(t *testing.T) {
	if err := validInfo().Validate(); err != nil {
		t.Fatalf("a valid registration was rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*WorkerInfo)
	}{
		{"empty id", func(w *WorkerInfo) { w.WorkerID = "" }},
		{"id with slash", func(w *WorkerInfo) { w.WorkerID = "a/b" }},
		{"id with space", func(w *WorkerInfo) { w.WorkerID = "a b" }},
		{"id starting with dash", func(w *WorkerInfo) { w.WorkerID = "-a" }},
		{"id too long", func(w *WorkerInfo) { w.WorkerID = strings.Repeat("a", 65) }},
		{"id with newline", func(w *WorkerInfo) { w.WorkerID = "a\nb" }},
		{"empty model", func(w *WorkerInfo) { w.Model = "" }},
		{"model with space", func(w *WorkerInfo) { w.Model = "qwen 7b" }},
		{"model with control char", func(w *WorkerInfo) { w.Model = "qwen\x00" }},
		{"model too long", func(w *WorkerInfo) { w.Model = strings.Repeat("m", 129) }},
		{"empty address", func(w *WorkerInfo) { w.Address = "" }},
		{"address without scheme", func(w *WorkerInfo) { w.Address = "10.0.0.5:8000" }},
		{"address ftp", func(w *WorkerInfo) { w.Address = "ftp://h:1" }},
		{"address without host", func(w *WorkerInfo) { w.Address = "http://" }},
		{"address with credentials", func(w *WorkerInfo) { w.Address = "http://u:pw@h:1" }},
		{"address with query", func(w *WorkerInfo) { w.Address = "http://h:1/?k=v" }},
		{"address with bare question mark", func(w *WorkerInfo) { w.Address = "http://h:1?" }},
		{"address with fragment", func(w *WorkerInfo) { w.Address = "http://h:1/#f" }},
		{"address too long", func(w *WorkerInfo) { w.Address = "http://h/" + strings.Repeat("a", 2100) }},
		{"zero concurrency", func(w *WorkerInfo) { w.MaxConcurrency = 0 }},
		{"huge concurrency", func(w *WorkerInfo) { w.MaxConcurrency = 1 << 30 }},
		{"negative queue", func(w *WorkerInfo) { w.QueueSize = -1 }},
		{"huge queue", func(w *WorkerInfo) { w.QueueSize = 1 << 30 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := validInfo()
			tt.mutate(&w)
			if err := w.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestWorkerInfoAcceptsTypicalModelsAndAddresses(t *testing.T) {
	for _, m := range []string{"qwen-7b", "Qwen/Qwen2.5-7B-Instruct", "llama3:8b", "m"} {
		w := validInfo()
		w.Model = m
		if err := w.Validate(); err != nil {
			t.Fatalf("model %q rejected: %v", m, err)
		}
	}
	for _, a := range []string{"http://h:1", "https://worker.internal", "http://[::1]:8000", "http://h:1/prefix"} {
		w := validInfo()
		w.Address = a
		if err := w.Validate(); err != nil {
			t.Fatalf("address %q rejected: %v", a, err)
		}
	}
	w := validInfo()
	w.QueueSize = 0
	if err := w.Validate(); err != nil {
		t.Fatalf("a worker with no queue is valid: %v", err)
	}
}

func TestValidationErrorsNeverEchoTheAddress(t *testing.T) {
	for _, a := range []string{"http://user:topsecret@h:1", "http://h:1/?token=topsecret", "topsecret", "http://h:1/#topsecret"} {
		err := ValidateAddress(a)
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "topsecret") {
			t.Fatalf("the error leaks the address: %v", err)
		}
	}
	if err := ValidateAddress("http://user:topsecret@h:1"); err == nil {
		t.Fatal("credentials must be rejected")
	}
}

func TestReportableStates(t *testing.T) {
	for _, s := range []WorkerState{StateRegistering, StateLoadingModel, StateWarming, StateReady, StateDraining, StateFailed} {
		if !s.Reportable() {
			t.Fatalf("%s must be reportable", s)
		}
	}
	for _, s := range []WorkerState{StateUnhealthy, StateLost, StateTerminated, "", "BOGUS", "ready"} {
		if s.Reportable() {
			t.Fatalf("%q must not be reportable", s)
		}
	}
}

func TestCanTransition(t *testing.T) {
	all := []WorkerState{StateRegistering, StateLoadingModel, StateWarming, StateReady, StateDraining, StateFailed}
	allowed := map[WorkerState][]WorkerState{
		StateRegistering:  {StateRegistering, StateLoadingModel, StateWarming, StateReady, StateDraining, StateFailed},
		StateLoadingModel: {StateLoadingModel, StateWarming, StateReady, StateDraining, StateFailed},
		StateWarming:      {StateWarming, StateReady, StateDraining, StateFailed},
		StateReady:        {StateReady, StateDraining, StateFailed},
		StateDraining:     {StateDraining, StateFailed},
		StateFailed:       {StateFailed, StateLoadingModel, StateWarming, StateReady, StateDraining},
	}
	for from, ok := range allowed {
		set := map[WorkerState]bool{}
		for _, s := range ok {
			set[s] = true
		}
		for _, to := range all {
			if got := CanTransition(from, to); got != set[to] {
				t.Errorf("%s -> %s: got %v, want %v", from, to, got, set[to])
			}
		}
	}
	// FAILED is reachable from every reported state, and nothing unreportable
	// is ever a legal target.
	for _, from := range all {
		if !CanTransition(from, StateFailed) {
			t.Errorf("FAILED must be reachable from %s", from)
		}
		for _, bad := range []WorkerState{StateUnhealthy, StateLost, StateTerminated, "X"} {
			if CanTransition(from, bad) {
				t.Errorf("%s -> %s must be illegal", from, bad)
			}
		}
	}
	if CanTransition("", StateReady) || CanTransition(StateLost, StateReady) {
		t.Error("an unknown or derived 'from' state must not allow transitions")
	}
}

func TestMetricsValidate(t *testing.T) {
	good := Metrics{ActiveRequests: 3, QueueDepth: 2, QueuedInputTokens: 100, RecentTokensPerSecond: 55.5}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	util, mem, badUtil, negMem := 88.0, int64(18421), 101.0, int64(-1)
	nan := math.NaN()
	withGPU := good
	withGPU.GPUUtilization, withGPU.GPUMemoryUsedMB = &util, &mem
	if err := withGPU.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Metrics)
	}{
		{"negative active", func(m *Metrics) { m.ActiveRequests = -1 }},
		{"negative queue", func(m *Metrics) { m.QueueDepth = -1 }},
		{"negative queued tokens", func(m *Metrics) { m.QueuedInputTokens = -1 }},
		{"huge queue", func(m *Metrics) { m.QueueDepth = math.MaxInt64 }},
		{"NaN rate", func(m *Metrics) { m.RecentTokensPerSecond = math.NaN() }},
		{"infinite rate", func(m *Metrics) { m.RecentTokensPerSecond = math.Inf(1) }},
		{"negative rate", func(m *Metrics) { m.RecentTokensPerSecond = -1 }},
		{"absurd rate", func(m *Metrics) { m.RecentTokensPerSecond = 1e12 }},
		{"gpu over 100", func(m *Metrics) { m.GPUUtilization = &badUtil }},
		{"gpu NaN", func(m *Metrics) { m.GPUUtilization = &nan }},
		{"negative gpu memory", func(m *Metrics) { m.GPUMemoryUsedMB = &negMem }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := good
			tt.mutate(&m)
			if err := m.Validate(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestHeartbeatValidate(t *testing.T) {
	ok := Heartbeat{RegistrationID: "reg_abc", State: StateReady}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]Heartbeat{
		"no registration id":   {State: StateReady},
		"huge registration id": {RegistrationID: strings.Repeat("r", 129), State: StateReady},
		"unreportable state":   {RegistrationID: "r", State: StateLost},
		"terminated":           {RegistrationID: "r", State: StateTerminated},
		"empty state":          {RegistrationID: "r"},
		"reason too long":      {RegistrationID: "r", State: StateFailed, Reason: strings.Repeat("x", 257)},
		"bad metrics":          {RegistrationID: "r", State: StateReady, Metrics: Metrics{QueueDepth: -5}},
	} {
		if err := h.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// --- independent review and verification findings ----------------------------------------

func TestTextFieldsRejectInvisibleAndControlCharacters(t *testing.T) {
	for name, s := range map[string]string{
		"zero-width space": "qwen\u200b7b", "zero-width joiner": "qwen\u200d", "bidi override": "qwen\u202e7b",
		"bidi isolate": "qwen\u2066", "BOM": "\ufeffqwen", "soft hyphen": "qwen\u00ad7b", "NUL": "qwen\x00", "invalid utf8": "qwen\xff",
	} {
		w := validInfo()
		w.Model = s
		if err := w.Validate(); err == nil {
			t.Errorf("model with %s was accepted", name)
		}
		if err := (Heartbeat{RegistrationID: "r", State: StateFailed, Reason: "backend " + s}).Validate(); err == nil {
			t.Errorf("reason with %s was accepted", name)
		}
	}
	// Ordinary punctuation, spaces in a reason, and non-ASCII letters are fine.
	if err := (Heartbeat{RegistrationID: "r", State: StateFailed, Reason: "backend unreachable: connection refused (retrying)"}).Validate(); err != nil {
		t.Fatalf("a normal reason was rejected: %v", err)
	}
	w := validInfo()
	w.Model = "模型-7b"
	if err := w.Validate(); err != nil {
		t.Fatalf("a model with non-ASCII letters is fine: %v", err)
	}
}

func TestHeartbeatErrorsDoNotEchoClientText(t *testing.T) {
	huge := strings.Repeat("x", 60000)
	err := Heartbeat{RegistrationID: "r", State: WorkerState(huge)}.Validate()
	if err == nil || len(err.Error()) > 300 {
		t.Fatalf("a validation error must not reflect client input back at 64KiB: %d bytes", len(fmt.Sprint(err)))
	}
}

func TestAddressPolicy(t *testing.T) {
	for _, bad := range []string{
		"http://169.254.169.254:80", "http://169.254.0.1", "http://[fe80::1]:80", "http://0.0.0.0:8000", "http://[::]:8000",
		"http://224.0.0.1:80", "http://[ff02::1]:80", "http://239.1.2.3:80", "http://[ff0e::1]:80", "http://[ff05::2]:80", "http://h:1/../etc", "http://h:1/a/../b", "http://h:1/..",
	} {
		if err := ValidateAddress(bad); err == nil {
			t.Errorf("%s must be rejected (link-local, unspecified, multicast, or a path traversal)", bad)
		}
	}
	for _, good := range []string{
		"http://127.0.0.1:8000", "http://[::1]:8000", "http://10.0.0.5:8000", "http://192.168.1.2", "http://172.16.0.9:8000",
		"http://worker.internal:8000", "https://worker.example.com", "http://h:1/prefix/v1", "http://h:1/a..b",
	} {
		if err := ValidateAddress(good); err != nil {
			t.Errorf("%s must be accepted (loopback, private, and hostnames are legitimate): %v", good, err)
		}
	}
}

func TestExactFieldBoundaries(t *testing.T) {
	at := func(mut func(*WorkerInfo)) error { w := validInfo(); mut(&w); return w.Validate() }
	for name, c := range map[string]struct {
		ok  func(*WorkerInfo)
		bad func(*WorkerInfo)
	}{
		"worker id 64 chars":    {func(w *WorkerInfo) { w.WorkerID = strings.Repeat("a", 64) }, func(w *WorkerInfo) { w.WorkerID = strings.Repeat("a", 65) }},
		"worker id leading dot": {func(w *WorkerInfo) { w.WorkerID = "a.b" }, func(w *WorkerInfo) { w.WorkerID = ".a" }},
		"worker id dot dot":     {func(w *WorkerInfo) { w.WorkerID = "a..b" }, func(w *WorkerInfo) { w.WorkerID = ".." }},
		"model 128 chars":       {func(w *WorkerInfo) { w.Model = strings.Repeat("m", 128) }, func(w *WorkerInfo) { w.Model = strings.Repeat("m", 129) }},
		"address 2048 chars":    {func(w *WorkerInfo) { w.Address = "http://h/" + strings.Repeat("a", 2048-9) }, func(w *WorkerInfo) { w.Address = "http://h/" + strings.Repeat("a", 2048-8) }},
		"concurrency 100000":    {func(w *WorkerInfo) { w.MaxConcurrency = 100000 }, func(w *WorkerInfo) { w.MaxConcurrency = 100001 }},
		"queue size 1000000":    {func(w *WorkerInfo) { w.QueueSize = 1000000 }, func(w *WorkerInfo) { w.QueueSize = 1000001 }},
	} {
		if err := at(c.ok); err != nil {
			t.Errorf("%s: the boundary value must be accepted: %v", name, err)
		}
		if err := at(c.bad); err == nil {
			t.Errorf("%s: one past the boundary must be rejected", name)
		}
	}

	hbAt := func(mut func(*Heartbeat)) error {
		h := Heartbeat{RegistrationID: "r", State: StateReady}
		mut(&h)
		return h.Validate()
	}
	util100, util101, memMax, memOver := 100.0, 100.0001, int64(1)<<40, int64(1)<<40+1
	for name, c := range map[string]struct{ ok, bad func(*Heartbeat) }{
		"registration id 128":   {func(h *Heartbeat) { h.RegistrationID = strings.Repeat("r", 128) }, func(h *Heartbeat) { h.RegistrationID = strings.Repeat("r", 129) }},
		"reason 256":            {func(h *Heartbeat) { h.Reason = strings.Repeat("r", 256) }, func(h *Heartbeat) { h.Reason = strings.Repeat("r", 257) }},
		"gpu utilization 100":   {func(h *Heartbeat) { h.Metrics.GPUUtilization = &util100 }, func(h *Heartbeat) { h.Metrics.GPUUtilization = &util101 }},
		"gpu memory 2^40":       {func(h *Heartbeat) { h.Metrics.GPUMemoryUsedMB = &memMax }, func(h *Heartbeat) { h.Metrics.GPUMemoryUsedMB = &memOver }},
		"counter 2^40":          {func(h *Heartbeat) { h.Metrics.QueuedInputTokens = 1 << 40 }, func(h *Heartbeat) { h.Metrics.QueuedInputTokens = 1<<40 + 1 }},
		"tokens per second 1e9": {func(h *Heartbeat) { h.Metrics.RecentTokensPerSecond = 1e9 }, func(h *Heartbeat) { h.Metrics.RecentTokensPerSecond = 1e9 + 1 }},
	} {
		if err := hbAt(c.ok); err != nil {
			t.Errorf("%s: the boundary value must be accepted: %v", name, err)
		}
		if err := hbAt(c.bad); err == nil {
			t.Errorf("%s: one past the boundary must be rejected", name)
		}
	}
}

// --- second verification: the address policy must not be bypassable by spelling ---------------------

func TestAddressPolicyCannotBeBypassedByAlternativeSpellings(t *testing.T) {
	// Each of these reaches 169.254.169.254, 0.0.0.0, or a link-local host when dialed,
	// but is not understood by net.ParseIP.
	for name, bad := range map[string]string{
		"fullwidth digits and ideographic dots": "http://１６９。２５４。１６９。２５４",
		"fullwidth loopback look-alike":         "http://１２７．０．０．１:8000",
		"decimal integer":                       "http://2852039166",
		"hex integer":                           "http://0xa9fea9fe",
		"octal dotted":                          "http://0251.0376.0251.0376",
		"two-part numeric":                      "http://169.254.43518",
		"mixed hex and decimal":                 "http://169.0xfe.169.254",
		"zero":                                  "http://0",
		"hex zero":                              "http://0x0",
		"zone id":                               "http://[fe80::1%25eth0]",
		"zone id on a global address":           "http://[2001:db8::1%25eth0]:80",
		"trailing dot":                          "http://169.254.169.254.",
		"hostname with trailing dot":            "http://worker.internal.",
		"broadcast":                             "http://255.255.255.255",
		"this-network":                          "http://0.1.2.3",
		"ipv4-mapped metadata address":          "http://[::ffff:169.254.169.254]:80",
		"ipv4-mapped unspecified":               "http://[::ffff:0.0.0.0]",
		"non-ascii hostname":                    "http://例え.jp:80",
		"hostname with underscore":              "http://under_score.internal",
		"hostname with a leading dash":          "http://-bad.internal",
		"hostname with an empty label":          "http://a..b",
		"label over 63 characters":              "http://" + strings.Repeat("a", 64) + ".internal",
		"hostname over 253 characters":          "http://" + strings.Repeat("a.", 130) + "internal",
		"port 0":                                "http://h:0",
		"port too large":                        "http://1.2.3.4:99999",
		"path with a space":                     "http://h:1/a b",
		"path with an encoded NUL":              "http://h:1/a%00b",
		"percent-encoded dot dot":               "http://h:1/%2e%2e/etc",
		"percent-encoded dot dot mixed":         "http://h:1/a/%2E%2e/b",
	} {
		if err := ValidateAddress(bad); err == nil {
			t.Errorf("%s must be rejected: %q", name, bad)
		} else if strings.Contains(err.Error(), "169") || strings.Contains(err.Error(), "example") {
			t.Errorf("%s: the error echoes the address: %v", name, err)
		}
	}
	for name, good := range map[string]string{
		"ipv4":                     "http://192.0.2.10:8000",
		"private ipv4":             "http://10.1.2.3",
		"loopback":                 "http://127.0.0.1:8000",
		"ipv6":                     "http://[2001:db8::1]:8000",
		"ipv6 loopback":            "http://[::1]",
		"simple hostname":          "http://worker01",
		"dotted hostname":          "http://worker-01.pool.internal:8000",
		"hostname with digits":     "http://10.0.0.5.nip.io",
		"digit-led label":          "http://1password.internal",
		"punycode hostname":        "http://xn--r8jz45g.jp",
		"uppercase hostname":       "http://Worker.Example.COM",
		"https":                    "https://worker.example.com:8443/v1",
		"port 65535":               "http://h:65535",
		"hostname label of 63":     "http://" + strings.Repeat("a", 63) + ".internal",
		"path with a dash and dot": "http://h:1/prefix-1.2/v1",
	} {
		if err := ValidateAddress(good); err != nil {
			t.Errorf("%s must be accepted: %q: %v", name, good, err)
		}
	}
}
