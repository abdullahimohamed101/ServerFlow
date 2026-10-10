package config

import (
	"fmt"
	"net"
)

// ControlPlaneConfig configures the control plane service. Addr is where it
// listens (loopback by default). Token, if set, is the shared secret every
// request must present as a bearer token; it is required for any address that
// is not loopback (see ValidateServe). MaxWorkers bounds registry memory.
type ControlPlaneConfig struct {
	Addr       string `yaml:"addr"`
	Token      string `yaml:"token"`
	MaxWorkers int    `yaml:"max_workers"`
}

const minTokenLen = 16

func (c *ControlPlaneConfig) validate() error {
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("control_plane.addr must be host:port")
	}
	if c.MaxWorkers < 1 || c.MaxWorkers > 100000 {
		return fmt.Errorf("control_plane.max_workers must be in 1-100000")
	}
	if c.Token != "" && len(c.Token) < minTokenLen {
		// Never echo the token itself.
		return fmt.Errorf("control_plane.token must be at least %d characters", minTokenLen)
	}
	return nil
}

// ValidateServe checks that the control plane may listen where it is
// configured to. Anything but loopback requires a token: without one any
// process on the network could register as a worker and steer traffic.
func (c *ControlPlaneConfig) ValidateServe() error {
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return fmt.Errorf("control_plane.addr must be host:port")
	}
	if c.Token == "" && !isLoopbackHost(host) {
		return fmt.Errorf("control_plane.token is required when control_plane.addr is not a loopback address")
	}
	return nil
}
