package config

import (
	"fmt"
	"net"
)

// ControlPlaneConfig configures the control plane service. Addr is where it
// listens (loopback by default). Token, if set, is the shared secret every
// request must present as a bearer token; it is required for any address that
// is not loopback (see ValidateServe). MaxWorkers bounds registry memory.
//
// AllowInsecureTransport lets the gateway and the worker agent send the token over plaintext http:// to a
// control plane that is not on this machine (gateway.control_plane_url, worker.control_plane_url). Without it
// those two settings must be https or loopback. It exists for the single-host container stack (ADR-021), where
// the control plane must listen on a non-loopback address inside a private network; it is off by default,
// like the other allow_insecure_transport settings, and nothing that holds a secret ever prints it.
type ControlPlaneConfig struct {
	Addr                   string `yaml:"addr"`
	Token                  string `yaml:"token"`
	MaxWorkers             int    `yaml:"max_workers"`
	AllowInsecureTransport bool   `yaml:"allow_insecure_transport"`
}

// cleartextTokenError is the one message both client guards return. It never includes the URL or the token.
func cleartextTokenError(setting string) error {
	return fmt.Errorf("%s must use https (or a loopback address) when control_plane.token is set, or the token would cross the network in cleartext; set control_plane.allow_insecure_transport only on a network you control", setting)
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
