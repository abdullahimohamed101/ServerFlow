package k8s

import (
	"net"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type kindConfig struct {
	Kind       string `yaml:"kind"`
	Name       string `yaml:"name"`
	Networking struct {
		APIServerAddress string `yaml:"apiServerAddress"`
		PodSubnet        string `yaml:"podSubnet"`
		ServiceSubnet    string `yaml:"serviceSubnet"`
	} `yaml:"networking"`
	Nodes []struct {
		Role              string `yaml:"role"`
		ExtraMounts       []any  `yaml:"extraMounts"`
		Image             string `yaml:"image"`
		ExtraPortMappings []struct {
			ListenAddress string `yaml:"listenAddress"`
			Protocol      string `yaml:"protocol"`
			HostPort      int    `yaml:"hostPort"`
			ContainerPort int    `yaml:"containerPort"`
		} `yaml:"extraPortMappings"`
	} `yaml:"nodes"`
}

func loadKind(t testing.TB) kindConfig {
	t.Helper()
	var c kindConfig
	if err := yaml.Unmarshal([]byte(read(t, "deploy/kubernetes/kind/cluster.yaml")), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// kindViolations is what is wrong with a kind cluster configuration (D2, D11, D15).
func kindViolations(c kindConfig, nodeImage string) []string {
	var v []string
	if c.Kind != "Cluster" || c.Name != "serverflow" {
		v = append(v, "the cluster must be named serverflow (the context kind-serverflow is the only one the scripts accept)")
	}
	if c.Networking.APIServerAddress != "127.0.0.1" {
		v = append(v, "the API server must listen on 127.0.0.1")
	}
	if c.Networking.PodSubnet != "10.244.0.0/16" {
		v = append(v, "podSubnet must be 10.244.0.0/16 (gateway.worker_networks is that CIDR, D11)")
	}
	if _, _, err := net.ParseCIDR(c.Networking.ServiceSubnet); err != nil {
		v = append(v, "serviceSubnet must be a CIDR")
	}
	if len(c.Nodes) != 1 || c.Nodes[0].Role != "control-plane" {
		return append(v, "exactly one control-plane node (single node, D2)")
	}
	if c.Nodes[0].Image != nodeImage {
		v = append(v, "the node image must equal KIND_NODE_IMAGE of tools.env")
	}
	if !digestRef.MatchString(c.Nodes[0].Image) {
		v = append(v, "the node image must be pinned by tag and digest")
	}
	if len(c.Nodes[0].ExtraMounts) > 0 {
		v = append(v, "no extraMounts: a hostPath into the node would expose the host's files")
	}
	hostPorts := map[int]bool{}
	for _, m := range c.Nodes[0].ExtraPortMappings {
		if m.ListenAddress != "127.0.0.1" {
			v = append(v, "every mapped port must listen on 127.0.0.1")
		}
		if m.Protocol != "TCP" {
			v = append(v, "every mapped port must say protocol TCP")
		}
		if m.ContainerPort < 30000 || m.ContainerPort > 32767 {
			v = append(v, "a mapped container port must be in the NodePort range")
		}
		if hostPorts[m.HostPort] {
			v = append(v, "a host port is mapped twice")
		}
		hostPorts[m.HostPort] = true
		// Compose publishes 18080, 19090, 19091, 13000 and 16686; both stacks must be able to run at once.
		switch m.HostPort {
		case 18080, 19090, 19091, 13000, 16686, 8080, 9090:
			v = append(v, "a host port collides with Compose or the host-process workflow")
		}
	}
	if len(hostPorts) != 5 {
		v = append(v, "five published ports are expected: gateway, control plane, Prometheus, Grafana, Jaeger")
	}
	return v
}

func TestKindConfig(t *testing.T) {
	for _, v := range kindViolations(loadKind(t), pins(t)["KIND_NODE_IMAGE"]) {
		t.Error(v)
	}
}

func TestKindConfigCatchesMistakes(t *testing.T) {
	mut := map[string]func(*kindConfig){
		"public listener":   func(c *kindConfig) { c.Nodes[0].ExtraPortMappings[0].ListenAddress = "0.0.0.0" },
		"no listen address": func(c *kindConfig) { c.Nodes[0].ExtraPortMappings[1].ListenAddress = "" },
		"two nodes":         func(c *kindConfig) { c.Nodes = append(c.Nodes, c.Nodes[0]) },
		"unpinned node":     func(c *kindConfig) { c.Nodes[0].Image = "kindest/node:v1.36.4" },
		"other node digest": func(c *kindConfig) { c.Nodes[0].Image = strings.Replace(c.Nodes[0].Image, "099e", "099f", 1) },
		"wrong pod CIDR":    func(c *kindConfig) { c.Networking.PodSubnet = "10.0.0.0/16" },
		"api on all ifaces": func(c *kindConfig) { c.Networking.APIServerAddress = "0.0.0.0" },
		"compose port":      func(c *kindConfig) { c.Nodes[0].ExtraPortMappings[0].HostPort = 18080 },
		"duplicate hostport": func(c *kindConfig) {
			c.Nodes[0].ExtraPortMappings[1].HostPort = c.Nodes[0].ExtraPortMappings[0].HostPort
		},
		"low container port": func(c *kindConfig) { c.Nodes[0].ExtraPortMappings[2].ContainerPort = 8080 },
		"extra mount of the host": func(c *kindConfig) {
			c.Nodes[0].ExtraMounts = []any{map[string]any{"hostPath": "/", "containerPath": "/host"}}
		},
		"renamed cluster": func(c *kindConfig) { c.Name = "other" },
	}
	for name, f := range mut {
		c := loadKind(t)
		f(&c)
		if len(kindViolations(c, pins(t)["KIND_NODE_IMAGE"])) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
}

// TestToolPins: tools.env is complete, pinned by digest or checksum, consistent with itself, with the cluster
// configuration and with `scripts/k8s-tool.sh pin`.
func TestToolPins(t *testing.T) {
	p := pins(t)
	sha := regexp.MustCompile(`^[0-9a-f]{64}$`)
	for _, k := range []string{"KIND_LINUX_AMD64_SHA256", "KUBECTL_LINUX_AMD64_SHA256", "KUBECONFORM_LINUX_AMD64_SHA256"} {
		if !sha.MatchString(p[k]) {
			t.Errorf("%s is not a sha256", k)
		}
	}
	for _, k := range []string{"KIND_NODE_IMAGE", "KUBECTL_IMAGE", "KUBECONFORM_IMAGE"} {
		if !digestRef.MatchString(p[k]) {
			t.Errorf("%s must be host/repo:tag@sha256:digest, is %q", k, p[k])
		}
	}
	ver := regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)
	for _, k := range []string{"KUBERNETES_VERSION", "KIND_VERSION", "KUBECONFORM_VERSION"} {
		if !ver.MatchString(p[k]) {
			t.Errorf("%s is not a vX.Y.Z version", k)
		}
	}
	if !strings.Contains(p["KIND_NODE_IMAGE"], ":"+p["KUBERNETES_VERSION"]+"@") {
		t.Error("the node image tag is not KUBERNETES_VERSION")
	}
	if !strings.Contains(p["KUBECTL_IMAGE"], ":"+p["KUBERNETES_VERSION"]+"@") {
		t.Error("the kubectl image tag is not KUBERNETES_VERSION")
	}
	if !strings.Contains(p["KUBECONFORM_IMAGE"], ":"+p["KUBECONFORM_VERSION"]+"@") {
		t.Error("the kubeconform image tag is not KUBECONFORM_VERSION")
	}
	// Docker Hub rate-limits shared runners; the images come from mirrors and registries that do not.
	for _, k := range []string{"KIND_NODE_IMAGE", "KUBECTL_IMAGE", "KUBECONFORM_IMAGE"} {
		if strings.HasPrefix(p[k], "docker.io/") || !strings.Contains(strings.SplitN(p[k], "/", 2)[0], ".") {
			t.Errorf("%s must name a registry other than Docker Hub", k)
		}
	}
	// The script and the Go parser read the same file the same way.
	for _, bash := range bashes(t) {
		out, err := execOut(t, bash, "-u", "scripts/k8s-tool.sh", "pin", "KIND_NODE_IMAGE")
		if err != nil || strings.TrimSpace(out) != p["KIND_NODE_IMAGE"] {
			t.Errorf("%s -u k8s-tool.sh pin KIND_NODE_IMAGE: %v %q", bash, err, out)
		}
	}
}
