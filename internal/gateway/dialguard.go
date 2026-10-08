package gateway

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"serverflow/pkg/protocol"
)

// AddressPolicy decides which IP addresses the gateway may connect to when it
// forwards a request to a worker. A registered worker names its own address, so
// the registry can only check spelling; this is the check on the address
// actually dialed (docs/decisions/ADR-011-scheduler-and-routing.md).
type AddressPolicy struct {
	allowed []netip.Prefix

	mu       sync.Mutex
	selfPort uint16
	selfIP   netip.Addr // the listening address; unspecified means every address of this host
	ifaces   map[netip.Addr]bool
	ifacesAt time.Time
}

// selfRefresh bounds how stale the cached list of this host's interface addresses may be, so an
// interface that appears later (a VPN, a container bridge) is still recognized.
const selfRefresh = 30 * time.Second

// SetSelf tells the policy where the gateway itself listens, so a worker that
// registers the gateway's own address cannot loop requests back through it.
func (p *AddressPolicy) SetSelf(a net.Addr) {
	t, ok := a.(*net.TCPAddr)
	if !ok {
		return
	}
	ip, _ := netip.AddrFromSlice(t.IP)
	p.mu.Lock()
	p.selfPort, p.selfIP = uint16(t.Port), ip.Unmap()
	p.ifaces, p.ifacesAt = nil, time.Time{}
	p.mu.Unlock()
}

// isSelf reports whether ap is the gateway's own listener. A wildcard listener answers on every
// loopback address (all of 127.0.0.0/8 on Linux) and every interface address; a specific one only
// on its own address.
func (p *AddressPolicy) isSelf(ap netip.AddrPort) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.selfPort == 0 || ap.Port() != p.selfPort {
		return false
	}
	ip := ap.Addr().Unmap().WithZone("")
	if p.selfIP.IsValid() && !p.selfIP.IsUnspecified() {
		return ip == p.selfIP.WithZone("")
	}
	if ip.IsLoopback() {
		return true
	}
	if p.ifaces == nil || time.Since(p.ifacesAt) > selfRefresh {
		p.ifaces, p.ifacesAt = map[netip.Addr]bool{}, time.Now()
		if addrs, err := net.InterfaceAddrs(); err == nil {
			for _, ia := range addrs {
				if ipn, ok := ia.(*net.IPNet); ok {
					if x, ok := netip.AddrFromSlice(ipn.IP); ok {
						p.ifaces[x.Unmap().WithZone("")] = true
					}
				}
			}
		}
	}
	return p.ifaces[ip]
}

// NewAddressPolicy builds a policy. With no CIDRs any address that is not
// forbidden outright is allowed; with CIDRs, only addresses inside them are.
func NewAddressPolicy(cidrs []string) (*AddressPolicy, error) {
	p := &AddressPolicy{}
	for _, c := range cidrs {
		pre, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, fmt.Errorf("worker network is not a CIDR")
		}
		p.allowed = append(p.allowed, pre.Masked())
	}
	return p, nil
}

// Check returns an error when ip must not be dialed. It never echoes the
// address, which came from a worker.
func (p *AddressPolicy) Check(ip netip.Addr) error {
	ip = ip.Unmap()
	if !ip.IsValid() || protocol.ForbiddenAddr(ip) {
		return errForbiddenAddress
	}
	if len(p.allowed) == 0 {
		return nil
	}
	for _, pre := range p.allowed {
		if pre.Contains(ip.WithZone("")) {
			return nil
		}
	}
	return errOutsideNetworks
}

var (
	errForbiddenAddress = fmt.Errorf("worker address is not allowed (unspecified, link-local, multicast, or broadcast)")
	errOutsideNetworks  = fmt.Errorf("worker address is outside the configured worker networks")
	errSelfAddress      = fmt.Errorf("worker address is the gateway itself")
)

// Control is a net.Dialer.Control hook. It runs once per connection attempt,
// after name resolution and with the concrete IP about to be connected, so
// DNS tricks (rebinding, odd spellings, IPv4-mapped forms) cannot get around it.
func (p *AddressPolicy) Control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return errForbiddenAddress
	}
	if err := p.Check(ap.Addr()); err != nil {
		return err
	}
	if p.isSelf(ap) {
		return errSelfAddress
	}
	return nil
}

// workerDialTimeout bounds connecting to a worker, so a blackholed address cannot hold a slot.
const workerDialTimeout = 5 * time.Second

func newWorkerDialer(p *AddressPolicy, r *net.Resolver) *net.Dialer {
	return &net.Dialer{Timeout: workerDialTimeout, KeepAlive: 30 * time.Second, Control: p.Control, Resolver: r}
}

// newWorkerTransport returns the transport used to reach workers: guarded
// dialing, no proxy (workers are internal), no compression (bytes are relayed
// exactly as sent).
func newWorkerTransport(p *AddressPolicy, headerTimeout time.Duration) *http.Transport {
	return newWorkerTransportResolver(p, headerTimeout, nil)
}

// newWorkerTransportResolver is newWorkerTransport with an explicit resolver, for tests that
// need name resolution to answer differently over time.
func newWorkerTransportResolver(p *AddressPolicy, headerTimeout time.Duration, r *net.Resolver) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.ResponseHeaderTimeout = headerTimeout
	tr.DisableCompression = true
	tr.MaxIdleConnsPerHost = 256
	tr.DialContext = newWorkerDialer(p, r).DialContext
	return tr
}
