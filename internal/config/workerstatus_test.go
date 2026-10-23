package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func agentWorker() WorkerConfig {
	w := Default().Worker
	w.ID, w.Model = "worker-1", "qwen-7b"
	w.BackendURL, w.AdvertiseURL = "http://127.0.0.1:9001", "http://127.0.0.1:9001"
	return w
}

func TestWorkerStatusListenIsOffByDefault(t *testing.T) {
	if got := Default().Worker.StatusListen; got != "" {
		t.Fatalf("the status endpoint must be off by default, status_listen = %q", got)
	}
	w := agentWorker()
	if err := w.validateAgent(); err != nil {
		t.Fatalf("an agent without a status endpoint must validate: %v", err)
	}
}

func TestWorkerStatusListenAcceptsLoopbackOnly(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:9102", "[::1]:9102", "localhost:9102", "127.0.0.2:9102", "127.0.0.1:0", "127.0.0.1:65535"} {
		w := agentWorker()
		w.StatusListen = ok
		if err := w.validateAgent(); err != nil {
			t.Errorf("%q must be accepted: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"0.0.0.0:9102", ":9102", "[::]:9102", "10.0.0.5:9102", "192.168.1.2:9102", "example.com:9102", "pod.local:9102",
		"9102", "127.0.0.1", "127.0.0.1:", "127.0.0.1:65536", "127.0.0.1:-1", "127.0.0.1:http", "127.0.0.1:09102", "http://127.0.0.1:9102",
		"127.0.0.1:9102/healthz", " 127.0.0.1:9102",
	} {
		w := agentWorker()
		w.StatusListen = bad
		if err := w.validateAgent(); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestWorkerStatusListenErrorsDoNotEchoTheValue(t *testing.T) {
	for _, bad := range []string{"user:topsecret@10.0.0.5:9102", "topsecret.example.com:9102", "127.0.0.1:topsecret"} {
		w := agentWorker()
		w.StatusListen = bad
		err := w.validateAgent()
		if err == nil {
			t.Errorf("%q must be rejected", bad)
		} else if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("the error echoes the value: %v", err)
		}
	}
}

func TestWorkerStatusListenIsReadFromEnvAndFile(t *testing.T) {
	t.Setenv("SERVERFLOW_WORKER_STATUS_LISTEN", "127.0.0.1:9102")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.StatusListen != "127.0.0.1:9102" {
		t.Fatalf("env not applied: %q", cfg.Worker.StatusListen)
	}

	_ = os.Unsetenv("SERVERFLOW_WORKER_STATUS_LISTEN") // t.Setenv above restores it afterwards
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("worker:\n  status_listen: 127.0.0.1:9103\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.StatusListen != "127.0.0.1:9103" {
		t.Fatalf("file not applied: %q", cfg.Worker.StatusListen)
	}
}
