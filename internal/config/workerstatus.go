package config

import (
	"fmt"
	"net"
	"strconv"
)

// validateStatusListen checks worker.status_listen (Phase 17, ADR-022 D6): the address of the worker agent's
// status endpoint. Empty turns the endpoint off, which is the default. Otherwise it must be host:port with a
// loopback host (localhost or a loopback IP) and a port in 0-65535 (0 lets the kernel choose, which the agent logs).
// There is deliberately no switch to expose it: the endpoint is for a probe running in the same container or pod
// (the healthcheck binary, which accepts loopback hosts only), never for the network. The message does not echo
// the value.
func validateStatusListen(addr string) error {
	if addr == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("worker.status_listen must be host:port with a loopback host, for example 127.0.0.1:9102")
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || strconv.FormatUint(n, 10) != port {
		return fmt.Errorf("worker.status_listen port must be a number in 0-65535")
	}
	if !isLoopbackHost(host) {
		return fmt.Errorf("worker.status_listen must be a loopback address (127.0.0.1, ::1 or localhost): the status endpoint is not for the network")
	}
	return nil
}
