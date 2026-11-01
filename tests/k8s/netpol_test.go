package k8s

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// NetworkPolicy (Phase 17, PR 5). The checker below judges the rendered NetworkPolicies against the traffic the manifests
// themselves show: it derives the client edges (who dials whom, on which container port) from the Deployments, Jobs, ConfigMaps,
// Secrets read, the worker's advertised address and the Prometheus scrape configuration, and the probes and NodePort Services
// that need the node as a source. The policies must equal that derivation, in both directions (an egress rule needs the matching
// ingress rule and the reverse, since every pod is default-deny both ways). TestNetworkPolicyCatchesMutations shows each way of
// breaking or loosening them being named.

type npSelector struct {
	MatchLabels      map[string]string `yaml:"matchLabels"`
	MatchExpressions []any             `yaml:"matchExpressions"`
}

type npPeer struct {
	PodSelector       *npSelector `yaml:"podSelector"`
	NamespaceSelector *npSelector `yaml:"namespaceSelector"`
	IPBlock           *struct {
		CIDR   string   `yaml:"cidr"`
		Except []string `yaml:"except"`
	} `yaml:"ipBlock"`
}

type npPort struct {
	Protocol string `yaml:"protocol"`
	Port     any    `yaml:"port"`
}

type npRule struct {
	From  []npPeer `yaml:"from"`
	To    []npPeer `yaml:"to"`
	Ports []npPort `yaml:"ports"`
}

type netpol struct {
	Metadata objMeta `yaml:"metadata"`
	Spec     struct {
		PodSelector npSelector `yaml:"podSelector"`
		PolicyTypes []string   `yaml:"policyTypes"`
		Ingress     []npRule   `yaml:"ingress"`
		Egress      []npRule   `yaml:"egress"`
	} `yaml:"spec"`
}

const componentLabel = "app.kubernetes.io/component"

func parsePolicies(t testing.TB, raw []map[string]any) []netpol {
	t.Helper()
	var out []netpol
	for _, d := range raw {
		if d["kind"] != "NetworkPolicy" {
			continue
		}
		b, err := yaml.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		var n netpol
		if err := yaml.Unmarshal(b, &n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func (s npSelector) matches(l map[string]string) bool {
	for k, v := range s.MatchLabels {
		if l[k] != v {
			return false
		}
	}
	return len(s.MatchExpressions) == 0
}

// The node sources: the pod network's gateway on the node, and the private ranges of the node's Docker network.
var nodeCIDRs = []string{"10.244.0.1/32", "172.16.0.0/12", "192.168.0.0/16"}

// The API server destinations of Prometheus' pod discovery: the node on Docker's network (6443) and the kubernetes Service (443).
var apiRules = map[string]string{"172.16.0.0/12": "6443", "192.168.0.0/16": "6443", "10.96.0.1/32": "443"}

// netInput is everything the checker reads.
type netInput struct {
	Pods     []pod
	Services []service
	Raw      []map[string]any
	Policies []netpol
	Scrapes  [][2]string // (component, port name) pairs of the Prometheus pod-discovery jobs
}

// netInputs builds a fresh input from the real render plus the two one-off pod templates (admin and probe are created by the
// script, not rendered, but they are pods of the namespace and the policies must cover the admin one).
func netInputs(t testing.TB) netInput {
	m := overlayManifests(t)
	in := netInput{Pods: append(append([]pod{}, m.Pods...), adminPod(t, "admin-1-2", `["tenant","list"]`)), Services: m.Services, Raw: m.Raw, Policies: parsePolicies(t, m.Raw)}
	var cfg struct {
		ScrapeConfigs []struct {
			Relabel []struct {
				Source []string `yaml:"source_labels"`
				Regex  string   `yaml:"regex"`
			} `yaml:"relabel_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal([]byte(read(t, "observability/prometheus/kubernetes/prometheus.yml")), &cfg); err != nil {
		t.Fatal(err)
	}
	for _, sc := range cfg.ScrapeConfigs {
		for _, r := range sc.Relabel {
			if len(r.Source) == 2 && strings.HasSuffix(r.Source[0], "pod_label_app_kubernetes_io_component") && strings.HasSuffix(r.Source[1], "pod_container_port_name") {
				c, p, ok := strings.Cut(r.Regex, ";")
				if ok {
					in.Scrapes = append(in.Scrapes, [2]string{c, p})
				}
			}
		}
	}
	return in
}

func podComponent(p pod) string { return p.Labels[componentLabel] }

// containerPorts: component -> port name -> number.
func containerPorts(pods []pod) map[string]map[string]int {
	out := map[string]map[string]int{}
	for _, p := range pods {
		c := podComponent(p)
		if out[c] == nil {
			out[c] = map[string]int{}
		}
		for _, ct := range p.all() {
			for _, cp := range ct.Ports {
				name := cp.Name
				if name == "" {
					name = strconv.Itoa(cp.ContainerPort)
				}
				out[c][name] = cp.ContainerPort
			}
		}
	}
	return out
}

func portNumber(ports map[string]int, v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case string:
		n, ok := ports[x]
		return n, ok
	}
	return 0, false
}

var hostPort = regexp.MustCompile(`\b([a-z][a-z0-9-]*):([0-9]{2,5})\b`)

// secretClient: a pod that reads one of these Secrets as a client dials the Service. The Secret's server (the pod that is the
// Service's own component) reads it to configure itself and is not a client.
var secretClient = map[string]struct {
	Service string
	Port    int
}{"dsn": {"postgres", 5432}, "redis": {"redis", 6379}, "kafka": {"redpanda", 9092}, "controlplane": {"control-plane", 9090}}

// expectedEdges derives the pod-to-pod traffic that exists, as "src>dst:port".
func expectedEdges(in netInput) map[string]bool {
	edges := map[string]bool{}
	ports := containerPorts(in.Pods)
	svcPort := map[string]map[int]int{} // service -> service port -> container port
	svcComp := map[string]string{}
	for _, s := range in.Services {
		var comp string
		for _, p := range in.Pods {
			if p.Kind != "Pod" && len(s.Spec.Selector) > 0 && s.Spec.Selector[componentLabel] == podComponent(p) {
				comp = podComponent(p)
			}
		}
		svcComp[s.Metadata.Name] = comp
		svcPort[s.Metadata.Name] = map[int]int{}
		for _, sp := range s.Spec.Ports {
			if n, ok := portNumber(ports[comp], sp.TargetPort); ok {
				svcPort[s.Metadata.Name][sp.Port] = n
			}
		}
	}
	configs := map[string][]string{} // ConfigMap name -> values
	for _, d := range in.Raw {
		if d["kind"] != "ConfigMap" {
			continue
		}
		name, _ := d["metadata"].(map[string]any)["name"].(string)
		data, _ := d["data"].(map[string]any)
		for _, v := range data {
			if s, ok := v.(string); ok {
				configs[name] = append(configs[name], s)
			}
		}
	}
	registry := false
	for _, vs := range configs {
		for _, v := range vs {
			registry = registry || v == "registry"
		}
	}
	for _, p := range in.Pods {
		src := podComponent(p)
		var texts []string
		for _, c := range p.all() {
			texts = append(texts, c.Command...)
			texts = append(texts, c.Args...)
			for _, e := range c.Env {
				texts = append(texts, e.Value)
			}
			for _, ef := range c.EnvFrom {
				if ef.ConfigMapRef != nil {
					texts = append(texts, configs[ef.ConfigMapRef.Name]...)
				}
				if ef.SecretRef != nil {
					if sc, ok := secretClient[ef.SecretRef.Name]; ok && sc.Service != src {
						edges[fmt.Sprintf("%s>%s:%d", src, sc.Service, sc.Port)] = true
					}
				}
			}
		}
		for _, tx := range texts {
			for _, m := range hostPort.FindAllStringSubmatch(tx, -1) {
				n, _ := strconv.Atoi(m[2])
				if cp, ok := svcPort[m[1]][n]; ok && svcComp[m[1]] != "" {
					edges[fmt.Sprintf("%s>%s:%d", src, svcComp[m[1]], cp)] = true
				}
			}
		}
		// A worker advertises its pod IP and port; the gateway dials whatever the registry returns.
		for _, c := range p.all() {
			for _, e := range c.Env {
				if m := regexp.MustCompile(`\$\(POD_IP\):([0-9]+)`).FindStringSubmatch(e.Value); m != nil && registry {
					edges["gateway>"+src+":"+m[1]] = true
				}
			}
		}
	}
	for _, sc := range in.Scrapes {
		if n, ok := ports[sc[0]][sc[1]]; ok {
			edges[fmt.Sprintf("prometheus>%s:%d", sc[0], n)] = true
		}
	}
	// The broker advertises the Service name redpanda:9092 and so dials itself through it.
	edges["redpanda>redpanda:9092"] = true
	return edges
}

// expectedNodePorts: the container ports the node itself dials: network probes (kubelet) and NodePort Services (kube-proxy).
// The API server's pod proxy (smoke K6, mock worker 9000) is the same port as that pod's probes.
func expectedNodePorts(in netInput) map[string]map[int]bool {
	out := map[string]map[int]bool{}
	add := func(c string, n int) {
		if out[c] == nil {
			out[c] = map[int]bool{}
		}
		out[c][n] = true
	}
	ports := containerPorts(in.Pods)
	for _, p := range in.Pods {
		for _, c := range p.all() {
			for _, pr := range []*probe{c.StartupProbe, c.ReadinessProbe, c.LivenessProbe} {
				if pr == nil {
					continue
				}
				if pr.HTTPGet != nil {
					if n, ok := portNumber(ports[podComponent(p)], pr.HTTPGet.Port); ok {
						add(podComponent(p), n)
					}
				}
				if pr.TCPSocket != nil {
					if n, ok := portNumber(ports[podComponent(p)], pr.TCPSocket.Port); ok {
						add(podComponent(p), n)
					}
				}
			}
		}
	}
	for _, s := range in.Services {
		if s.Spec.Type != "NodePort" {
			continue
		}
		comp := s.Spec.Selector[componentLabel]
		for _, sp := range s.Spec.Ports {
			if n, ok := portNumber(ports[comp], sp.TargetPort); ok {
				add(comp, n)
			}
		}
	}
	return out
}

func peerComponent(p npPeer) (string, bool) {
	if p.PodSelector == nil || p.NamespaceSelector != nil || p.IPBlock != nil || len(p.PodSelector.MatchExpressions) != 0 || len(p.PodSelector.MatchLabels) != 1 {
		return "", false
	}
	c, ok := p.PodSelector.MatchLabels[componentLabel]
	return c, ok
}

func policyComponent(n netpol) (string, bool) {
	l := n.Spec.PodSelector.MatchLabels
	if len(l) != 2 || l["app.kubernetes.io/name"] != "serverflow" || l[componentLabel] == "" || len(n.Spec.PodSelector.MatchExpressions) != 0 {
		return "", false
	}
	return l[componentLabel], true
}

func isNodeBlock(ps []npPeer) bool {
	if len(ps) != len(nodeCIDRs) {
		return false
	}
	have := map[string]bool{}
	for _, p := range ps {
		if p.IPBlock == nil || len(p.IPBlock.Except) != 0 || p.PodSelector != nil || p.NamespaceSelector != nil {
			return false
		}
		have[p.IPBlock.CIDR] = true
	}
	for _, c := range nodeCIDRs {
		if !have[c] {
			return false
		}
	}
	return true
}

// checkNetworkPolicies returns every way the policies fail to be default-deny plus exactly the traffic that exists.
func checkNetworkPolicies(in netInput) []string {
	var v []string
	bad := func(f string, a ...any) { v = append(v, fmt.Sprintf(f, a...)) }
	ports := containerPorts(in.Pods)
	comps := map[string]bool{}
	for _, p := range in.Pods {
		comps[podComponent(p)] = true
	}
	isBoth := func(t []string) bool {
		return len(t) == 2 && ((t[0] == "Ingress" && t[1] == "Egress") || (t[0] == "Egress" && t[1] == "Ingress"))
	}

	// 1. default deny: selects every pod, both directions, allows nothing.
	var deny, dns *netpol
	for i := range in.Policies {
		switch in.Policies[i].Metadata.Name {
		case "default-deny-all":
			deny = &in.Policies[i]
		case "allow-dns-egress":
			dns = &in.Policies[i]
		}
	}
	if deny == nil {
		bad("there is no default-deny-all policy")
	} else if len(deny.Spec.PodSelector.MatchLabels) != 0 || len(deny.Spec.PodSelector.MatchExpressions) != 0 || !isBoth(deny.Spec.PolicyTypes) || len(deny.Spec.Ingress) != 0 || len(deny.Spec.Egress) != 0 {
		bad("default-deny-all must select every pod, list policyTypes Ingress and Egress and allow nothing")
	}
	// 2. DNS: CoreDNS in kube-system on 53 and nothing else.
	if dns == nil {
		bad("there is no allow-dns-egress policy")
	} else {
		ok := len(dns.Spec.PodSelector.MatchLabels) == 0 && len(dns.Spec.PodSelector.MatchExpressions) == 0 && len(dns.Spec.PolicyTypes) == 1 && dns.Spec.PolicyTypes[0] == "Egress" && len(dns.Spec.Ingress) == 0 && len(dns.Spec.Egress) == 1
		if ok {
			r := dns.Spec.Egress[0]
			ok = len(r.To) == 1 && r.To[0].IPBlock == nil && r.To[0].NamespaceSelector != nil && r.To[0].PodSelector != nil &&
				len(r.To[0].NamespaceSelector.MatchLabels) == 1 && r.To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] == "kube-system" &&
				len(r.To[0].PodSelector.MatchLabels) == 1 && r.To[0].PodSelector.MatchLabels["k8s-app"] == "kube-dns" && len(r.Ports) == 2
			for _, pt := range r.Ports {
				ok = ok && pt.Port == 53 && (pt.Protocol == "UDP" || pt.Protocol == "TCP")
			}
		}
		if !ok {
			bad("allow-dns-egress must allow only CoreDNS (kube-system, k8s-app=kube-dns) on port 53")
		}
	}

	// 3. one or more workload policies per component; every rule is narrow and numeric.
	selected := map[string]bool{}
	actualEgress, actualIngress := map[string]bool{}, map[string]bool{}
	nodeIngress := map[string]map[int]bool{}
	apiEgress := map[string]map[string]bool{}
	for _, n := range in.Policies {
		name := n.Metadata.Name
		if n.Metadata.Name == "default-deny-all" || n.Metadata.Name == "allow-dns-egress" {
			continue
		}
		c, ok := policyComponent(n)
		if !ok {
			bad("%s: podSelector must be exactly app.kubernetes.io/name=serverflow and one component", name)
			continue
		}
		if !comps[c] {
			bad("%s: selects the component %s, which no workload has", name, c)
		}
		selected[c] = true
		if !isBoth(n.Spec.PolicyTypes) {
			bad("%s: policyTypes must be Ingress and Egress (a type that is left out is not denied by this policy's pods' selection of rules)", name)
		}
		portsOK := func(r npRule, dest string, side string) bool {
			if len(r.Ports) == 0 {
				bad("%s: a %s rule without ports allows every port", name, side)
				return false
			}
			good := true
			for _, pt := range r.Ports {
				num, isInt := pt.Port.(int)
				if !isInt || pt.Protocol != "TCP" {
					bad("%s: %s ports must be numeric TCP ports, got %v/%v", name, side, pt.Protocol, pt.Port)
					good = false
					continue
				}
				if dest != "" {
					found := false
					for _, p := range ports[dest] {
						found = found || p == num
					}
					if !found {
						bad("%s: %s port %d, but %s declares no containerPort %d", name, side, num, dest, num)
					}
				}
			}
			return good
		}
		for _, r := range n.Spec.Ingress {
			if len(r.From) == 0 {
				bad("%s: an ingress rule without from matches every source", name)
				continue
			}
			if isNodeBlock(r.From) {
				if portsOK(r, c, "ingress") {
					for _, pt := range r.Ports {
						if nodeIngress[c] == nil {
							nodeIngress[c] = map[int]bool{}
						}
						nodeIngress[c][pt.Port.(int)] = true
					}
				}
				continue
			}
			for _, p := range r.From {
				src, ok := peerComponent(p)
				if !ok {
					bad("%s: an ingress peer must be a pod of one component (no namespaceSelector, no expressions, no ipBlock other than the node range)", name)
					continue
				}
				if !comps[src] {
					bad("%s: ingress from %s, which no workload has", name, src)
				}
				if portsOK(r, c, "ingress") {
					for _, pt := range r.Ports {
						actualIngress[fmt.Sprintf("%s>%s:%d", src, c, pt.Port.(int))] = true
					}
				}
			}
		}
		for _, r := range n.Spec.Egress {
			if len(r.To) == 0 {
				bad("%s: an egress rule without to matches every destination", name)
				continue
			}
			for _, p := range r.To {
				if p.IPBlock != nil {
					want, known := apiRules[p.IPBlock.CIDR]
					if c != "prometheus" || !known || len(p.IPBlock.Except) != 0 || len(r.Ports) != 1 || fmt.Sprint(r.Ports[0].Port) != want {
						bad("%s: ipBlock egress to %s is not the API server rule of Prometheus (%v)", name, p.IPBlock.CIDR, apiRules)
						continue
					}
					if apiEgress[c] == nil {
						apiEgress[c] = map[string]bool{}
					}
					apiEgress[c][p.IPBlock.CIDR] = true
					continue
				}
				dst, ok := peerComponent(p)
				if !ok {
					bad("%s: an egress peer must be a pod of one component or the API server ipBlocks of Prometheus", name)
					continue
				}
				if !comps[dst] {
					bad("%s: egress to %s, which no workload has", name, dst)
				}
				if portsOK(r, dst, "egress") {
					for _, pt := range r.Ports {
						actualEgress[fmt.Sprintf("%s>%s:%d", c, dst, pt.Port.(int))] = true
					}
				}
			}
		}
	}
	for c := range comps {
		if !selected[c] {
			bad("no NetworkPolicy selects the component %s: under default-deny it could neither send nor receive", c)
		}
	}

	// 4. the policies equal the derivation, both ways.
	want := expectedEdges(in)
	for e := range want {
		if !actualEgress[e] {
			bad("missing egress %s: the manifests show this traffic and no egress rule allows it", e)
		}
		if !actualIngress[e] {
			bad("missing ingress %s: the manifests show this traffic and no ingress rule allows it", e)
		}
	}
	for e := range actualEgress {
		if !want[e] {
			bad("unexpected egress %s: allowed, but nothing in the manifests dials it", e)
		}
	}
	for e := range actualIngress {
		if !want[e] {
			bad("unexpected ingress %s: allowed, but nothing in the manifests dials it", e)
		}
	}
	// 5. node-sourced traffic exactly where a probe, a NodePort or the API server's pod proxy needs it.
	wantNode := expectedNodePorts(in)
	for c, ps := range wantNode {
		for p := range ps {
			if !nodeIngress[c][p] {
				bad("missing node ingress %s:%d: a network probe or a NodePort Service needs the node to reach it", c, p)
			}
		}
	}
	for c, ps := range nodeIngress {
		for p := range ps {
			if !wantNode[c][p] {
				bad("unexpected node ingress %s:%d: no probe or NodePort Service needs the node to reach it", c, p)
			}
		}
	}
	// 6. Prometheus alone reaches the API server, with all three rules.
	for _, cidr := range sortedKeys(map[string]bool{"172.16.0.0/12": true, "192.168.0.0/16": true, "10.96.0.1/32": true}) {
		if !apiEgress["prometheus"][cidr] {
			bad("prometheus lacks its API server egress rule to %s", cidr)
		}
	}
	return v
}

func TestNetworkPolicyIsDefaultDenyAndEqualsTheTraffic(t *testing.T) {
	in := netInputs(t)
	for _, x := range checkNetworkPolicies(in) {
		t.Error(x)
	}
	// The derivation is not vacuous: it found the traffic the architecture has.
	edges := expectedEdges(in)
	for _, e := range []string{
		"gateway>control-plane:9090", "gateway>mock-worker:9000", "gateway>redis:6379", "gateway>postgres:5432", "gateway>redpanda:9092", "gateway>jaeger:4318",
		"mock-worker>control-plane:9090", "mock-worker>jaeger:4318", "usage-consumer>redpanda:9092", "usage-consumer>postgres:5432",
		"migrate>postgres:5432", "admin>postgres:5432", "redpanda-init>redpanda:9092", "grafana>prometheus:9090",
		"prometheus>gateway:9100", "prometheus>control-plane:9101", "prometheus>mock-worker:9000",
	} {
		if !edges[e] {
			t.Errorf("the derivation missed %s (found %v)", e, sortedKeys(edges))
		}
	}
	if len(edges) != 18 {
		t.Errorf("the derivation found %d edges (%v), expected 18: the 17 above and the broker's own", len(edges), sortedKeys(edges))
	}
	// The Prometheus pod discovery rules and a few absences that are the point of the exercise.
	for _, e := range []string{"control-plane>postgres:5432", "mock-worker>redis:6379", "prometheus>postgres:5432", "grafana>jaeger:16686"} {
		if edges[e] {
			t.Errorf("%s must not exist", e)
		}
	}
}

func TestNetworkPolicyCatchesMutations(t *testing.T) {
	policy := func(in *netInput, name string) *netpol {
		for i := range in.Policies {
			if in.Policies[i].Metadata.Name == name {
				return &in.Policies[i]
			}
		}
		t.Fatalf("no policy %s", name)
		return nil
	}
	remove := func(in *netInput, name string) {
		var keep []netpol
		for _, p := range in.Policies {
			if p.Metadata.Name != name {
				keep = append(keep, p)
			}
		}
		in.Policies = keep
	}
	podSel := func(c string) npPeer {
		return npPeer{PodSelector: &npSelector{MatchLabels: map[string]string{componentLabel: c}}}
	}
	tcp := func(p int) []npPort { return []npPort{{Protocol: "TCP", Port: p}} }
	// dropEgress removes the egress rule whose only peer is dst on port.
	dropEgress := func(in *netInput, from, dst string, port int) {
		p := policy(in, from)
		var keep []npRule
		for _, r := range p.Spec.Egress {
			if c, ok := peerComponent(r.To[0]); ok && len(r.To) == 1 && c == dst && r.Ports[0].Port == port {
				continue
			}
			keep = append(keep, r)
		}
		p.Spec.Egress = keep
	}
	dropIngress := func(in *netInput, to string, port int, src string) {
		p := policy(in, to)
		for i := range p.Spec.Ingress {
			r := &p.Spec.Ingress[i]
			if len(r.Ports) == 1 && r.Ports[0].Port == port {
				var keep []npPeer
				for _, f := range r.From {
					if c, ok := peerComponent(f); !ok || c != src {
						keep = append(keep, f)
					}
				}
				r.From = keep
			}
		}
		var rules []npRule
		for _, r := range p.Spec.Ingress {
			if len(r.From) > 0 {
				rules = append(rules, r)
			}
		}
		p.Spec.Ingress = rules
	}
	first := func(p *pod) *container { return &p.Spec.Containers[0] }
	podOf := func(in *netInput, id string) *pod {
		for i := range in.Pods {
			if in.Pods[i].id() == id {
				return &in.Pods[i]
			}
		}
		t.Fatalf("no pod %s", id)
		return nil
	}
	muts := []struct {
		name, want string
		apply      func(in *netInput)
	}{
		{"default-deny removed", "no default-deny-all", func(in *netInput) { remove(in, "default-deny-all") }},
		{"default-deny ingress only", "default-deny-all must select every pod", func(in *netInput) { policy(in, "default-deny-all").Spec.PolicyTypes = []string{"Ingress"} }},
		{"default-deny egress only", "default-deny-all must select every pod", func(in *netInput) { policy(in, "default-deny-all").Spec.PolicyTypes = []string{"Egress"} }},
		{"default-deny narrowed to one component", "default-deny-all must select every pod", func(in *netInput) {
			policy(in, "default-deny-all").Spec.PodSelector.MatchLabels = map[string]string{componentLabel: "gateway"}
		}},
		{"default-deny allows everything", "default-deny-all must select every pod", func(in *netInput) {
			policy(in, "default-deny-all").Spec.Egress = []npRule{{To: []npPeer{{}}}}
		}},
		{"dns removed", "no allow-dns-egress", func(in *netInput) { remove(in, "allow-dns-egress") }},
		{"dns open to every destination", "allow-dns-egress must allow only CoreDNS", func(in *netInput) { policy(in, "allow-dns-egress").Spec.Egress[0].To = nil }},
		{"dns on every port", "allow-dns-egress must allow only CoreDNS", func(in *netInput) {
			policy(in, "allow-dns-egress").Spec.Egress[0].Ports = append(policy(in, "allow-dns-egress").Spec.Egress[0].Ports, npPort{Protocol: "TCP", Port: 443})
		}},
		{"redis loses its policy (a pod nobody selected)", "no NetworkPolicy selects the component redis", func(in *netInput) { remove(in, "redis") }},
		{"a new workload without a policy", "no NetworkPolicy selects the component newthing", func(in *netInput) {
			p := *podOf(in, "Deployment/gateway")
			p.Name = "newthing"
			p.Labels = map[string]string{componentLabel: "newthing", "app.kubernetes.io/name": "serverflow"}
			in.Pods = append(in.Pods, p)
		}},
		{"gateway may no longer reach redis (egress rule removed)", "missing egress gateway>redis:6379", func(in *netInput) { dropEgress(in, "gateway", "redis", 6379) }},
		{"redis no longer accepts the gateway (ingress rule removed)", "missing ingress gateway>redis:6379", func(in *netInput) { dropIngress(in, "redis", 6379, "gateway") }},
		{"postgres accepts Prometheus too (loosened)", "unexpected ingress prometheus>postgres:5432", func(in *netInput) {
			p := policy(in, "postgres")
			p.Spec.Ingress[0].From = append(p.Spec.Ingress[0].From, podSel("prometheus"))
		}},
		{"control plane may dial postgres (loosened egress)", "unexpected egress control-plane>postgres:5432", func(in *netInput) {
			p := policy(in, "control-plane")
			p.Spec.Egress = append(p.Spec.Egress, npRule{To: []npPeer{podSel("postgres")}, Ports: tcp(5432)})
		}},
		{"every source may reach postgres", "without from matches every source", func(in *netInput) {
			p := policy(in, "postgres")
			p.Spec.Ingress = append(p.Spec.Ingress, npRule{Ports: tcp(5432)})
		}},
		{"every destination from the gateway", "without to matches every destination", func(in *netInput) {
			p := policy(in, "gateway")
			p.Spec.Egress = append(p.Spec.Egress, npRule{Ports: tcp(443)})
		}},
		{"rule without ports", "allows every port", func(in *netInput) { policy(in, "redis").Spec.Ingress[0].Ports = nil }},
		{"0.0.0.0/0 on the jaeger UI", "must be a pod of one component", func(in *netInput) {
			p := policy(in, "jaeger")
			p.Spec.Ingress = append(p.Spec.Ingress, npRule{From: []npPeer{{IPBlock: &struct {
				CIDR   string   `yaml:"cidr"`
				Except []string `yaml:"except"`
			}{CIDR: "0.0.0.0/0"}}}, Ports: tcp(16686)})
		}},
		{"the whole pod CIDR as a node source", "unexpected node ingress", func(in *netInput) {
			p := policy(in, "postgres")
			p.Spec.Ingress = append(p.Spec.Ingress, npRule{From: []npPeer{
				{IPBlock: &struct {
					CIDR   string   `yaml:"cidr"`
					Except []string `yaml:"except"`
				}{CIDR: "10.244.0.1/32"}},
				{IPBlock: &struct {
					CIDR   string   `yaml:"cidr"`
					Except []string `yaml:"except"`
				}{CIDR: "172.16.0.0/12"}},
				{IPBlock: &struct {
					CIDR   string   `yaml:"cidr"`
					Except []string `yaml:"except"`
				}{CIDR: "192.168.0.0/16"}},
			}, Ports: tcp(5432)})
		}},
		{"namespaceSelector peer", "must be a pod of one component", func(in *netInput) {
			p := policy(in, "redis")
			p.Spec.Ingress[0].From = append(p.Spec.Ingress[0].From, npPeer{NamespaceSelector: &npSelector{}})
		}},
		{"undeclared port in an egress rule", "declares no containerPort 9001", func(in *netInput) {
			p := policy(in, "gateway")
			for i := range p.Spec.Egress {
				if c, ok := peerComponent(p.Spec.Egress[i].To[0]); ok && c == "mock-worker" {
					p.Spec.Egress[i].Ports = tcp(9001)
				}
			}
		}},
		{"named port in a rule", "must be numeric TCP ports", func(in *netInput) {
			policy(in, "redis").Spec.Ingress[0].Ports = []npPort{{Protocol: "TCP", Port: "redis"}}
		}},
		{"UDP rule", "must be numeric TCP ports", func(in *netInput) {
			policy(in, "redis").Spec.Ingress[0].Ports = []npPort{{Protocol: "UDP", Port: 6379}}
		}},
		{"a policy that only covers ingress", "policyTypes must be Ingress and Egress", func(in *netInput) { policy(in, "grafana").Spec.PolicyTypes = []string{"Ingress"} }},
		{"the gateway NodePort cannot be reached", "missing node ingress gateway:8080", func(in *netInput) {
			p := policy(in, "gateway")
			p.Spec.Ingress = p.Spec.Ingress[1:]
		}},
		{"a node source on postgres", "unexpected node ingress postgres:5432", func(in *netInput) {
			p := policy(in, "gateway")
			node := p.Spec.Ingress[0]
			node.Ports = tcp(5432)
			q := policy(in, "postgres")
			q.Spec.Ingress = append(q.Spec.Ingress, node)
		}},
		{"Prometheus loses the API server", "prometheus lacks its API server egress rule", func(in *netInput) {
			p := policy(in, "prometheus")
			p.Spec.Egress = p.Spec.Egress[:3]
		}},
		{"the gateway may reach the API server", "is not the API server rule of Prometheus", func(in *netInput) {
			p := policy(in, "gateway")
			p.Spec.Egress = append(p.Spec.Egress, npRule{To: []npPeer{{IPBlock: &struct {
				CIDR   string   `yaml:"cidr"`
				Except []string `yaml:"except"`
			}{CIDR: "172.16.0.0/12"}}}, Ports: tcp(6443)})
		}},
		{"a new dial in the manifests (usage consumer to redis)", "missing egress usage-consumer>redis:6379", func(in *netInput) {
			c := first(podOf(in, "Deployment/usage-consumer"))
			c.Env = append(c.Env, envVar{Name: "SERVERFLOW_EXTRA_ADDR", Value: "redis:6379"})
		}},
		{"a new secret client (mock worker reads the DSN)", "missing egress mock-worker>postgres:5432", func(in *netInput) {
			c := first(podOf(in, "Deployment/mock-worker"))
			c.EnvFrom = append(c.EnvFrom, struct {
				SecretRef    *struct{ Name string } `yaml:"secretRef"`
				ConfigMapRef *struct{ Name string } `yaml:"configMapRef"`
			}{SecretRef: &struct{ Name string }{"dsn"}})
		}},
		{"a policy selecting a component nobody runs", "which no workload has", func(in *netInput) {
			in.Policies = append(in.Policies, *policy(in, "redis"))
			in.Policies[len(in.Policies)-1].Metadata.Name = "ghost"
			in.Policies[len(in.Policies)-1].Spec.PodSelector.MatchLabels = map[string]string{"app.kubernetes.io/name": "serverflow", componentLabel: "ghost"}
		}},
		{"a broad pod selector", "podSelector must be exactly", func(in *netInput) {
			policy(in, "redis").Spec.PodSelector.MatchLabels = map[string]string{componentLabel: "redis"}
		}},
	}
	for _, mu := range muts {
		in := netInputs(t) // a fresh parse so one mutation never leaks into the next
		mu.apply(&in)
		if v := checkNetworkPolicies(in); !contains(v, mu.want) {
			t.Errorf("%s: no violation containing %q; got %v", mu.name, mu.want, v)
		}
	}
	t.Logf("%d mutations caught", len(muts))
}

// probePod fills the probe template the way scripts/k8s-up.sh does.
func probePod(t testing.TB, component string) pod {
	t.Helper()
	text := read(t, "deploy/kubernetes/tools/netpol-probe.yaml")
	text = strings.ReplaceAll(text, "__PROBE_NAME__", "netpol-probe-1-2")
	text = strings.ReplaceAll(text, "__PROBE_COMPONENT__", component)
	text = strings.ReplaceAll(text, "__PROBE_TARGETS__", "10.96.1.1:6379 10.96.1.2:5432")
	var w struct {
		Kind     string  `yaml:"kind"`
		Metadata objMeta `yaml:"metadata"`
		Spec     podSpec `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(text), &w); err != nil {
		t.Fatalf("netpol-probe.yaml: %v", err)
	}
	p := pod{Kind: w.Kind, Name: w.Metadata.Name, Replicas: 1, Labels: w.Metadata.Labels, Spec: w.Spec}
	p.W.Metadata = w.Metadata
	return p
}

// TestProbePodIsHardenedAndOutsideTheAllowList: the probe of K15 satisfies Pod Security `restricted` like every other pod (the
// namespace would reject it otherwise), the label it carries as the outsider is selected by no allow rule (only default-deny and
// DNS), and the label of the control is selected by the admin policy.
func TestProbePodIsHardenedAndOutsideTheAllowList(t *testing.T) {
	created := createdSecrets(t)
	for _, comp := range []string{"netpol-probe", "admin"} {
		p := probePod(t, comp)
		if p.Kind != "Pod" || p.Spec.RestartPolicy != "Never" || p.Spec.ActiveDeadlineSeconds == 0 {
			t.Errorf("%s probe: must be a Pod with restartPolicy Never and an activeDeadlineSeconds", comp)
		}
		var v []string
		for _, f := range []func(pod) []string{checkSecurity, checkResources, checkLabels, checkVolumes} {
			v = append(v, f(p)...)
		}
		v = append(v, checkEnvAndSecrets(p, created)...)
		v = append(v, checkImages(t, p)...)
		for _, x := range v {
			t.Errorf("%s probe: %s", comp, x)
		}
	}
	in := netInputs(t)
	count := func(comp string) []string {
		var names []string
		l := probePod(t, comp).Labels
		for _, n := range in.Policies {
			if n.Spec.PodSelector.matches(l) {
				names = append(names, n.Metadata.Name)
			}
		}
		sort.Strings(names)
		return names
	}
	if got := strings.Join(count("netpol-probe"), ","); got != "allow-dns-egress,default-deny-all" {
		t.Errorf("the outsider probe is selected by %q; it must be selected by default-deny-all and allow-dns-egress only", got)
	}
	if got := strings.Join(count("admin"), ","); got != "admin,allow-dns-egress,default-deny-all" {
		t.Errorf("the control probe is selected by %q; it must have the admin policy besides the two default ones", got)
	}
	if strings.Contains(string(render(t, overlay)), "__PROBE_") || strings.Contains(read(t, "deploy/kubernetes/base/kustomization.yaml"), "netpol-probe") {
		t.Error("the probe template must not be rendered by kustomize")
	}
	// The targets are addresses, never names: a blocked lookup must not be able to fake a blocked connection.
	if !strings.Contains(read(t, "deploy/kubernetes/tools/netpol-probe.yaml"), `nc -w 3 "${t%:*}" "${t##*:}"`) {
		t.Error("the probe must connect with a timeout to the IP and port of each target")
	}
}

// TestK15JudgesEnforcement runs k15_network_policy_enforced against stubs of the probe pod and of the gateway request.
func TestK15JudgesEnforcement(t *testing.T) {
	run := func(outsider, control, code string, withPolicy bool) bool {
		stub := `kc() { case "$*" in *"get networkpolicy"*) ` + map[bool]string{true: "return 0", false: "return 1"}[withPolicy] + `;; esac; }
service_ip() { case "$1" in redis) echo 10.96.0.10;; postgres) echo 10.96.0.11;; redpanda) echo 10.96.0.12;; control-plane) echo 10.96.0.13;; esac; }
netpol_probe() { case "$1" in netpol-probe) printf '%s\n' ` + outsider + `;; admin) printf '%s\n' ` + control + `;; esac; }
gw_curl() { printf '%s' ` + code + `; }
K8S_DIR=/nonexistent NAMESPACE=serverflow GATEWAY_URL=http://g BODY=x
`
		res := runK(t, []string{"k15_network_policy_enforced"}, stub, "k15_network_policy_enforced")
		if !allEqual(res, res[0]) {
			t.Fatalf("bash versions disagree: %v", res)
		}
		return res[0]
	}
	allClosed := `"10.96.0.10:6379 closed" "10.96.0.11:5432 closed" "10.96.0.12:9092 closed" "10.96.0.13:9090 closed"`
	ctlOK := `"10.96.0.11:5432 open" "10.96.0.10:6379 closed"`
	if !run(allClosed, ctlOK, "200", true) {
		t.Fatal("K15 failed on a cluster that enforces the policies")
	}
	cases := map[string]struct {
		outsider, control, code string
		policy                  bool
	}{
		"the CNI does not enforce policy (the outsider reaches redis)":        {`"10.96.0.10:6379 open" "10.96.0.11:5432 closed" "10.96.0.12:9092 closed" "10.96.0.13:9090 closed"`, ctlOK, "200", true},
		"the outsider reaches the control plane":                              {`"10.96.0.10:6379 closed" "10.96.0.11:5432 closed" "10.96.0.12:9092 closed" "10.96.0.13:9090 open"`, ctlOK, "200", true},
		"the outsider's log is empty":                                         {`""`, ctlOK, "200", true},
		"the control cannot reach postgres (the probe or the rule is broken)": {allClosed, `"10.96.0.11:5432 closed" "10.96.0.10:6379 closed"`, "200", true},
		"the control reaches redis (the admin rule is too wide)":              {allClosed, `"10.96.0.11:5432 open" "10.96.0.10:6379 open"`, "200", true},
		"the gateway answers 503 after the policies":                          {allClosed, ctlOK, "503", true},
		"the gateway does not answer":                                         {allClosed, ctlOK, "000", true},
		"there is no default-deny policy":                                     {allClosed, ctlOK, "200", false},
	}
	for name, c := range cases {
		if run(c.outsider, c.control, c.code, c.policy) {
			t.Errorf("K15 passed although %s", name)
		}
	}
}

// TestNetpolProbeRefusesOddInput: the probe pod's placeholders are substituted into YAML, so what goes in is checked first.
func TestNetpolProbeRefusesOddInput(t *testing.T) {
	body := extractFunc(t, "netpol_probe")
	for _, want := range []string{"netpol-probe | admin", `*[!0-9.:\ ]*`, "deadline", "delete pod"} {
		if !strings.Contains(body, want) {
			t.Errorf("netpol_probe lost %q", want)
		}
	}
	for _, bash := range bashes(t) {
		for _, c := range [][2]string{{"evil", "10.0.0.1:80"}, {"admin", "10.0.0.1:80; rm -rf /"}, {"admin", "name:80"}, {"netpol-probe", ""}} {
			prelude := "set -u\nNAMESPACE=serverflow PROBE_POD=/nonexistent\ndie() { echo \"DIE: $*\" >&2; exit 7; }\nkc() { echo \"kc called: $*\" >&2; return 1; }\n"
			out, err := execOut(t, bash, "-c", prelude+body+"\nnetpol_probe '"+c[0]+"' '"+c[1]+"'\n")
			if err == nil || !strings.Contains(out, "DIE: netpol_probe: refusing") || strings.Contains(out, "kc called") {
				t.Errorf("%s: netpol_probe %q %q was not refused before anything ran: %v %s", bash, c[0], c[1], err, out)
			}
		}
	}
}
