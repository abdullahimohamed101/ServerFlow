package tracing

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// fakeReceiver is an OTLP/HTTP trace receiver. mode decides how it answers: ok, error (500) or hang
// (never answers until the test ends).
type fakeReceiver struct {
	srv      *httptest.Server
	mode     atomic.Value // string
	mu       sync.Mutex
	requests []*collectortrace.ExportTraceServiceRequest
	headers  []http.Header
	paths    []string
	hits     atomic.Int64
	release  chan struct{}
}

func newFakeReceiver(t *testing.T) *fakeReceiver {
	t.Helper()
	f := &fakeReceiver{release: make(chan struct{})}
	f.mode.Store("ok")
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		switch f.mode.Load().(string) {
		case "error":
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		case "hang":
			select {
			case <-f.release:
			case <-r.Context().Done():
			}
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.requests = append(f.requests, &req)
		f.headers = append(f.headers, r.Header.Clone())
		f.paths = append(f.paths, r.URL.Path)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		resp, _ := proto.Marshal(&collectortrace.ExportTraceServiceResponse{})
		_, _ = w.Write(resp)
	}))
	t.Cleanup(func() {
		select {
		case <-f.release:
		default:
			close(f.release)
		}
		f.srv.Close()
	})
	return f
}

func (f *fakeReceiver) setMode(m string) { f.mode.Store(m) }

func (f *fakeReceiver) spanNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.requests {
		for _, rs := range r.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					out = append(out, s.Name)
				}
			}
		}
	}
	return out
}
