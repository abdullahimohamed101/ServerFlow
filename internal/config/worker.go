package config

import (
	"fmt"
	"net/url"
	"time"

	"serverflow/pkg/protocol"
)

// WorkerConfig configures worker agents and how the registry judges them.
//
// HeartbeatInterval is how often workers report state. The registry measures
// heartbeat age at receipt: a worker is suspect once SuspectTimeout passes
// without one, unhealthy after UnhealthyTimeout, and lost after LostTimeout;
// a lost worker is evicted once Retention more has passed (spec section 12).
// SuspectTimeout must be at least two heartbeat intervals so one late
// heartbeat does not flap a worker.
//
// ID, Model, BackendURL, AdvertiseURL and ControlPlaneURL identify an agent's
// worker; only the agent requires them (see ValidateAgent).
type WorkerConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	SuspectTimeout    time.Duration `yaml:"suspect_timeout"`
	UnhealthyTimeout  time.Duration `yaml:"unhealthy_timeout"`
	LostTimeout       time.Duration `yaml:"lost_timeout"`
	Retention         time.Duration `yaml:"retention"`

	ID              string `yaml:"id"`
	Model           string `yaml:"model"`
	BackendURL      string `yaml:"backend_url"`
	AdvertiseURL    string `yaml:"advertise_url"`
	ControlPlaneURL string `yaml:"control_plane_url"`
}

func (w *WorkerConfig) validate() error {
	if w.HeartbeatInterval < minHeartbeatInterval {
		return fmt.Errorf("worker.heartbeat_interval must be at least %v", minHeartbeatInterval)
	}
	if w.SuspectTimeout <= 0 || w.UnhealthyTimeout <= 0 || w.LostTimeout <= 0 || w.Retention <= 0 {
		return fmt.Errorf("worker.suspect_timeout, unhealthy_timeout, lost_timeout and retention must be > 0")
	}
	if w.SuspectTimeout >= w.UnhealthyTimeout || w.UnhealthyTimeout >= w.LostTimeout {
		return fmt.Errorf("worker timeouts must satisfy suspect_timeout < unhealthy_timeout < lost_timeout")
	}
	if w.SuspectTimeout < 2*w.HeartbeatInterval {
		return fmt.Errorf("worker.suspect_timeout must be at least twice worker.heartbeat_interval")
	}
	return nil
}

// ValidateAgent checks everything an agent needs from the whole configuration:
// its own identity and addresses, and that the control plane token is not sent
// in cleartext to another machine.
func (c *Config) ValidateAgent() error {
	if err := c.Worker.validateAgent(); err != nil {
		return err
	}
	if c.ControlPlane.Token != "" {
		if u, err := url.Parse(c.Worker.ControlPlaneURL); err == nil && u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
			return fmt.Errorf("worker.control_plane_url must use https (or a loopback address) when control_plane.token is set, or the token would cross the network in cleartext")
		}
	}
	return nil
}

// validateAgent checks the settings an agent needs. Error messages never echo
// URLs, which could carry credentials.
func (w *WorkerConfig) validateAgent() error {
	if !protocol.ValidWorkerID(w.ID) {
		return fmt.Errorf("worker.id is required: 1-%d characters of letters, digits, '.', '_' or '-'", protocol.MaxWorkerIDLen)
	}
	if w.Model == "" {
		return fmt.Errorf("worker.model is required")
	}
	for name, v := range map[string]string{
		"worker.backend_url": w.BackendURL, "worker.advertise_url": w.AdvertiseURL, "worker.control_plane_url": w.ControlPlaneURL,
	} {
		if err := protocol.ValidateAddress(v); err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
	}
	return nil
}

// minHeartbeatInterval keeps a typo from turning every agent into a hot loop.
const minHeartbeatInterval = 10 * time.Millisecond
