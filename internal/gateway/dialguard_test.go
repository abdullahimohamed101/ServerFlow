package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAddressPolicyForbidsTheAlwaysDangerousAddresses(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	for _, bad := range []string{
		"169.254.169.254", "169.254.0.1", "0.0.0.0", "0.1.2.3", "255.255.255.255", "224.0.0.1", "239.255.255.250",
		"::", "fe80::1", "fe80::1%eth0", "ff02::1", "ff01::1", "::ffff:169.254.169.254", "::ffff:0.0.0.0", "::ffff:255.255.255.255",
	} {
		ip, err := netip.ParseAddr(bad)
		if err != nil {
			t.Fatalf("%s: %v", bad, err)
		}
		if err := p.Check(ip); !errors.Is(err, errForbiddenAddress) {
			t.Errorf("%s must be refused, got %v", bad, err)
		}
	}
	for _, ok := range []string{"127.0.0.1", "::1", "10.1.2.3", "192.168.1.1", "172.16.0.9", "8.8.8.8", "2001:db8::1", "fd00::5", "::ffff:10.0.0.1"} {
		ip, _ := netip.ParseAddr(ok)
		if err := p.Check(ip); err != nil {
			t.Errorf("%s must be allowed, got %v", ok, err)
		}
	}
	if err := p.Check(netip.Addr{}); err == nil {
		t.Error("an invalid address must be refused")
	}
}

func TestAddressPolicyWithNetworksOnlyAllowsThoseNetworks(t *testing.T) {
	p, err := NewAddressPolicy([]string{"10.0.0.0/8", "fd00::/8", "127.0.0.1/32"})
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{
		"10.9.9.9": true, "10.0.0.0": true, "10.255.255.255": true, "11.0.0.1": false, "192.168.1.1": false,
		"127.0.0.1": true, "127.0.0.2": false, "fd12::1": true, "fe00::1": false, "::ffff:10.1.1.1": true,
		"169.254.169.254": false, // forbidden anyway
	} {
		err := p.Check(netip.MustParseAddr(ip))
		if (err == nil) != want {
			t.Errorf("%s: allowed=%v, want %v (%v)", ip, err == nil, want, err)
		}
	}
	// A network listing a forbidden range does not make it dialable.
	p, _ = NewAddressPolicy([]string{"169.254.0.0/16", "0.0.0.0/0"})
	if err := p.Check(netip.MustParseAddr("169.254.169.254")); !errors.Is(err, errForbiddenAddress) {
		t.Errorf("the metadata address stays forbidden even when a network covers it: %v", err)
	}
	if _, err := NewAddressPolicy([]string{"10.0.0.0"}); err == nil {
		t.Error("a bare address is not a CIDR")
	}
	if _, err := NewAddressPolicy([]string{"not-a-cidr"}); err == nil || strings.Contains(err.Error(), "not-a-cidr") {
		t.Errorf("bad CIDRs are rejected without being echoed: %v", err)
	}
}

func TestControlJudgesTheConcreteAddressBeingDialed(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	for addr, wantErr := range map[string]bool{
		"169.254.169.254:80": true, "[::ffff:169.254.169.254]:80": true, "0.0.0.0:80": true, "[fe80::1%eth0]:80": true,
		"[::]:80": true, "224.0.0.1:80": true, "not an address": true, "": true,
		"127.0.0.1:8000": false, "[::1]:8000": false, "10.0.0.5:1": false,
	} {
		if err := p.Control("tcp", addr, nil); (err != nil) != wantErr {
			t.Errorf("%q: err=%v, want error=%v", addr, err, wantErr)
		}
	}
}

func TestTheWorkerTransportRefusesForbiddenTargetsBeforeConnecting(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	client := &http.Client{Transport: newWorkerTransport(p, time.Second), Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, target := range []string{
		"http://169.254.169.254/latest/meta-data/", "http://[::ffff:169.254.169.254]/", "http://0.0.0.0:9/", "http://[fe80::1]:9/",
	} {
		start := time.Now()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			t.Fatalf("%s must not be reachable", target)
		}
		if !strings.Contains(err.Error(), errForbiddenAddress.Error()) {
			t.Errorf("%s: the refusal must come from the policy, not the network: %v", target, err)
		}
		if time.Since(start) > time.Second {
			t.Errorf("%s: a refused address must fail at once, took %v", target, time.Since(start))
		}
	}
}

func TestTheWorkerTransportReachesAllowedAddressesAndNeverFollowsRedirects(t *testing.T) {
	var hits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; _, _ = io.WriteString(w, "ok") }))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	p, _ := NewAddressPolicy(nil)
	client := &http.Client{Transport: newWorkerTransport(p, time.Second), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(target.URL)
	if err != nil {
		t.Fatalf("loopback workers must stay reachable: %v", err)
	}
	_ = resp.Body.Close()
	if hits != 1 {
		t.Fatalf("hits = %d", hits)
	}
	resp, err = client.Get(redirector.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("the redirect must be handed back, not followed: %d", resp.StatusCode)
	}

	// With a network list that excludes loopback, even a live local worker is refused.
	only, _ := NewAddressPolicy([]string{"10.0.0.0/8"})
	client = &http.Client{Transport: newWorkerTransport(only, time.Second)}
	if resp, err := client.Get(target.URL); err == nil {
		_ = resp.Body.Close()
		t.Fatal("an address outside worker_networks must be refused")
	} else if !strings.Contains(err.Error(), errOutsideNetworks.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
	if hits != 1 {
		t.Fatal("the refused request must never have reached the worker")
	}
}

func TestMoreMetadataEndpointsAndWrappedFormsAreRefused(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	for _, bad := range []string{"168.63.129.16:80", "100.100.100.200:80", "[fd00:ec2::254]:80", "[64:ff9b::a9fe:a9fe]:80", "[2002:a9fe:a9fe::1]:80"} {
		if err := p.Control("tcp", bad, nil); !errors.Is(err, errForbiddenAddress) {
			t.Errorf("%s must be refused, got %v", bad, err)
		}
	}
}

func TestTheGatewayRefusesToDialItself(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	if err := p.Control("tcp", "127.0.0.1:8080", nil); err != nil {
		t.Fatalf("before the listener is known nothing is self: %v", err)
	}
	p.SetSelf(&net.TCPAddr{IP: net.IPv4zero, Port: 8080})
	for addr, wantSelf := range map[string]bool{
		"127.0.0.1:8080": true, "[::1]:8080": true, "127.0.0.1:8081": false, "10.99.99.99:8080": false,
	} {
		err := p.Control("tcp", addr, nil)
		if errors.Is(err, errSelfAddress) != wantSelf {
			t.Errorf("%s: err=%v, want self=%v", addr, err, wantSelf)
		}
	}
	// A non-TCP address leaves the policy unchanged.
	p2, _ := NewAddressPolicy(nil)
	p2.SetSelf(&net.UnixAddr{Name: "x", Net: "unix"})
	if err := p2.Control("tcp", "127.0.0.1:8080", nil); err != nil {
		t.Fatal(err)
	}
}

// fakeDNS answers A queries over UDP from a table that tests can change, so name resolution can
// be made to give one answer and then another (DNS rebinding).
type fakeDNS struct {
	mu      sync.Mutex
	answers map[string]net.IP
	conn    net.PacketConn
}

func newFakeDNS(t *testing.T) *fakeDNS {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &fakeDNS{answers: map[string]net.IP{}, conn: pc}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 512)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if resp := d.reply(buf[:n]); resp != nil {
				_, _ = pc.WriteTo(resp, addr)
			}
		}
	}()
	return d
}

func (d *fakeDNS) set(name string, ip string) {
	d.mu.Lock()
	d.answers[name] = net.ParseIP(ip).To4()
	d.mu.Unlock()
}

func (d *fakeDNS) resolver() *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", d.conn.LocalAddr().String())
	}}
}

// reply builds a minimal DNS answer: A records from the table, empty for AAAA, NXDOMAIN otherwise.
func (d *fakeDNS) reply(q []byte) []byte {
	if len(q) < 12 {
		return nil
	}
	i, labels := 12, []string{}
	for i < len(q) && q[i] != 0 {
		l := int(q[i])
		if i+1+l > len(q) {
			return nil
		}
		labels = append(labels, string(q[i+1:i+1+l]))
		i += 1 + l
	}
	if i+5 > len(q) {
		return nil
	}
	qtype := int(q[i+1])<<8 | int(q[i+2])
	question := q[12 : i+5]
	name := strings.Join(labels, ".")
	d.mu.Lock()
	ip := d.answers[name]
	d.mu.Unlock()
	resp := append([]byte{q[0], q[1], 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0}, question...)
	switch {
	case ip == nil:
		resp[3] = 0x83 // NXDOMAIN
	case qtype == 1:
		resp[7] = 1
		resp = append(resp, 0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4)
		resp = append(resp, ip...)
	}
	return resp
}

func TestNamesAreCheckedAfterResolutionSoRebindingCannotGetThroughTheGuard(t *testing.T) {
	var hits int
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; _, _ = io.WriteString(w, "ok") }))
	t.Cleanup(worker.Close)
	port := worker.Listener.Addr().(*net.TCPAddr).Port

	dns := newFakeDNS(t)
	p, _ := NewAddressPolicy(nil)
	tr := newWorkerTransportResolver(p, time.Second, dns.resolver())
	tr.DisableKeepAlives = true // every request resolves and dials again
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
	get := func(host string) error {
		resp, err := client.Get("http://" + host + ":" + strconv.Itoa(port) + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
		return err
	}

	dns.set("worker.test", "127.0.0.1")
	if err := get("worker.test"); err != nil || hits != 1 {
		t.Fatalf("a name resolving to an allowed address works: %v hits=%d", err, hits)
	}
	// The same name now points at the metadata service: it passed any registration-time check, but
	// the address actually dialed is what counts.
	dns.set("worker.test", "169.254.169.254")
	if err := get("worker.test"); err == nil || !strings.Contains(err.Error(), errForbiddenAddress.Error()) {
		t.Fatalf("a rebound name must be refused at connect time: %v", err)
	}
	for name, ip := range map[string]string{"zero.test": "0.0.0.0", "multi.test": "224.0.0.1", "azure.test": "168.63.129.16"} {
		dns.set(name, ip)
		if err := get(name); err == nil || !strings.Contains(err.Error(), errForbiddenAddress.Error()) {
			t.Errorf("%s -> %s must be refused: %v", name, ip, err)
		}
	}
	if hits != 1 {
		t.Fatalf("nothing but the first request may have reached the worker: %d", hits)
	}
	dns.set("worker.test", "127.0.0.1")
	if err := get("worker.test"); err != nil || hits != 2 {
		t.Fatalf("and it recovers when the name points somewhere allowed: %v hits=%d", err, hits)
	}
}

func TestAWildcardListenerIsSelfOnEveryLoopbackAndInterfaceAddress(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	p.SetSelf(&net.TCPAddr{IP: net.IPv6zero, Port: 8080})
	for _, self := range []string{"127.0.0.2:8080", "127.255.255.254:8080", "[::ffff:127.0.0.2]:8080", "[::1]:8080", "127.0.0.1:8080"} {
		if err := p.Control("tcp", self, nil); !errors.Is(err, errSelfAddress) {
			t.Errorf("%s is the gateway itself, got %v", self, err)
		}
	}
	// This host's own non-loopback addresses count too (the verifier hit its LAN address).
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil && !ipn.IP.IsLoopback() && !ipn.IP.IsLinkLocalUnicast() {
				if err := p.Control("tcp", net.JoinHostPort(ipn.IP.String(), "8080"), nil); !errors.Is(err, errSelfAddress) {
					t.Errorf("%s is this host, got %v", ipn.IP, err)
				}
				mapped := "[::ffff:" + ipn.IP.String() + "]:8080"
				if err := p.Control("tcp", mapped, nil); !errors.Is(err, errSelfAddress) {
					t.Errorf("%s is this host in IPv4-mapped form, got %v", mapped, err)
				}
				break
			}
		}
	}
	for _, other := range []string{"127.0.0.2:8081", "10.77.77.77:8080"} {
		if err := p.Control("tcp", other, nil); errors.Is(err, errSelfAddress) {
			t.Errorf("%s is not the gateway", other)
		}
	}
}

func TestASpecificListenerIsOnlySelfOnItsOwnAddress(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	p.SetSelf(&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 8080})
	if err := p.Control("tcp", "10.1.2.3:8080", nil); !errors.Is(err, errSelfAddress) {
		t.Errorf("its own address: %v", err)
	}
	for _, other := range []string{"127.0.0.1:8080", "10.1.2.4:8080", "10.1.2.3:8081"} {
		if err := p.Control("tcp", other, nil); errors.Is(err, errSelfAddress) {
			t.Errorf("%s is not the gateway (it listens on 10.1.2.3 only)", other)
		}
	}
}

func TestZonedAndWrappedMetadataAddressesAreRefusedAtConnectTime(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	for _, bad := range []string{"[fd00:ec2::254%lo0]:80", "[::169.254.169.254]:80", "[::ffff:0:169.254.169.254]:80", "[64:ff9b:1::1]:80"} {
		if err := p.Control("tcp", bad, nil); !errors.Is(err, errForbiddenAddress) {
			t.Errorf("%s must be refused, got %v", bad, err)
		}
	}
}
