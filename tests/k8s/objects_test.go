package k8s

import (
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// allowedKinds is the whole vocabulary of the render. A kind that is not here (a ClusterRole, a hostPath PersistentVolume, a
// NetworkPolicy before PR 5) needs a deliberate edit of this list and so a review.
var allowedKinds = map[string]bool{"Namespace": true, "ServiceAccount": true, "Service": true, "StatefulSet": true, "Job": true}

func labelsOf(d map[string]any) map[string]any {
	md, _ := d["metadata"].(map[string]any)
	l, _ := md["labels"].(map[string]any)
	return l
}

func TestNamespaceEnforcesRestricted(t *testing.T) {
	var ns map[string]any
	for _, d := range overlayManifestsRaw(t) {
		if d["kind"] == "Namespace" {
			if ns != nil {
				t.Fatal("more than one Namespace")
			}
			ns = d
		}
	}
	if ns == nil {
		t.Fatal("no Namespace in the render")
	}
	if name := ns["metadata"].(map[string]any)["name"]; name != "serverflow" {
		t.Errorf("namespace is %v, want serverflow (spec 37)", name)
	}
	l := labelsOf(ns)
	for _, mode := range []string{"enforce", "audit", "warn"} {
		if l["pod-security.kubernetes.io/"+mode] != "restricted" {
			t.Errorf("pod-security.kubernetes.io/%s must be restricted, is %v", mode, l["pod-security.kubernetes.io/"+mode])
		}
		want := "v" + strings.Join(strings.Split(strings.TrimPrefix(pins(t)["KUBERNETES_VERSION"], "v"), ".")[:2], ".")
		if l["pod-security.kubernetes.io/"+mode+"-version"] != want {
			t.Errorf("pod-security.kubernetes.io/%s-version must be %s (the Kubernetes minor of tools.env), is %v", mode, want, l["pod-security.kubernetes.io/"+mode+"-version"])
		}
	}
}

func overlayManifestsRaw(t testing.TB) []map[string]any { return overlayManifests(t).Raw }

func TestOnlyAllowedKindsAndNoSecrets(t *testing.T) {
	for _, d := range overlayManifestsRaw(t) {
		kind, _ := d["kind"].(string)
		if kind == "Secret" {
			t.Errorf("the render contains a Secret: Secrets are created by scripts/k8s-up.sh from .dev/, never from git")
		}
		if !allowedKinds[kind] {
			t.Errorf("kind %s is not in the allowed list of this pull request (edit allowedKinds deliberately)", kind)
		}
	}
}

func TestEveryObjectIsInTheNamespaceWithTheLabels(t *testing.T) {
	for _, d := range overlayManifestsRaw(t) {
		md, _ := d["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		kind, _ := d["kind"].(string)
		if kind != "Namespace" && md["namespace"] != "serverflow" {
			t.Errorf("%s/%s is not in the namespace serverflow (%v)", kind, name, md["namespace"])
		}
		l := labelsOf(d)
		for _, k := range []string{"app.kubernetes.io/name", "app.kubernetes.io/component", "app.kubernetes.io/part-of", "serverflow.dev/phase"} {
			if s, _ := l[k].(string); s == "" {
				t.Errorf("%s/%s lacks the label %s", kind, name, k)
			}
		}
		phase, _ := l["serverflow.dev/phase"].(string)
		if phase != "foundation" && phase != "data" && phase != "jobs" {
			t.Errorf("%s/%s has the phase %q; scripts/k8s-up.sh applies foundation, data and jobs", kind, name, phase)
		}
	}
}

// TestServicesSelectPodsAndTargetDeclaredPorts: a Service must select some pod template, its targetPort names must exist in
// that pod, nothing is a NodePort or LoadBalancer yet (PR 3 adds the NodePorts, bound by kind to 127.0.0.1).
func TestServicesSelectPodsAndTargetDeclaredPorts(t *testing.T) {
	m := overlayManifests(t)
	if len(m.Services) < 3 {
		t.Fatalf("expected the three data-tier Services, found %d", len(m.Services))
	}
	for _, s := range m.Services {
		name := s.Metadata.Name
		for _, sp := range s.Spec.Ports {
			if sp.NodePort != 0 {
				t.Errorf("service %s port %s sets nodePort %d; there are no NodePorts until PR 3", name, sp.Name, sp.NodePort)
			}
		}
		if s.Spec.Type != "ClusterIP" {
			t.Errorf("service %s is %q; only ClusterIP exists until PR 3", name, s.Spec.Type)
		}
		matched := false
		for _, p := range m.Pods {
			ok := len(s.Spec.Selector) > 0
			for k, v := range s.Spec.Selector {
				if p.Labels[k] != v {
					ok = false
				}
			}
			if !ok {
				continue
			}
			matched = true
			for _, sp := range s.Spec.Ports {
				tp, isName := sp.TargetPort.(string)
				if !isName {
					t.Errorf("service %s port %s: targetPort must be a port name", name, sp.Name)
					continue
				}
				found := false
				for _, c := range p.all() {
					for _, cp := range c.Ports {
						found = found || cp.Name == tp
					}
				}
				if !found {
					t.Errorf("service %s targets the port name %q, which %s does not declare", name, tp, p.id())
				}
			}
		}
		if !matched {
			t.Errorf("service %s selects no pod", name)
		}
	}
	// A StatefulSet's serviceName is a Service of the render.
	for _, p := range m.Pods {
		if p.Kind != "StatefulSet" {
			continue
		}
		found := false
		for _, s := range m.Services {
			found = found || s.Metadata.Name == p.W.Spec.ServiceName
		}
		if !found {
			t.Errorf("%s: serviceName %q is not a Service", p.id(), p.W.Spec.ServiceName)
		}
	}
	// Service names equal Compose's (D21): DSNs and env files are reused unchanged.
	want := map[string]bool{"postgres": false, "redis": false, "redpanda": false}
	for _, s := range m.Services {
		if _, ok := want[s.Metadata.Name]; ok {
			want[s.Metadata.Name] = true
		}
	}
	for n, ok := range want {
		if !ok {
			t.Errorf("no Service named %s (the Compose name)", n)
		}
	}
}

// TestNoSecretsCommitted: no file under deploy/kubernetes holds a password, token or key literal (generated secrets live in
// .dev/, gitignored), and the render holds none either.
func TestNoSecretsCommitted(t *testing.T) {
	allowed := regexp.MustCompile(`\$|_file\b|credentials_file`)
	var files []string
	err := filepath.Walk(filepath.Join(root(t), "deploy/kubernetes"), func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			files = append(files, p)
		}
		return err
	})
	if err != nil || len(files) < 8 {
		t.Fatalf("expected the files of deploy/kubernetes (%v), found %d", err, len(files))
	}
	scan := func(name, text string) {
		for i, line := range strings.Split(text, "\n") {
			if secretLiteral.MatchString(line) && !allowed.MatchString(line) {
				t.Errorf("%s:%d looks like a secret literal", name, i+1) // the line itself is not printed
			}
			if regexp.MustCompile(`://[^/\s:@]+:[^/\s@$]+@`).MatchString(line) {
				t.Errorf("%s:%d holds a URL with a password", name, i+1)
			}
		}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root(t), f)
		scan(rel, string(b))
		if strings.HasSuffix(f, ".env") && strings.Contains(string(b), "SECRET") {
			t.Errorf("%s: tools.env holds pins, not secrets", rel)
		}
	}
	scan("the rendered overlay", string(render(t, overlay)))
	// Files that must never exist in the tree.
	for _, bad := range []string{"secret.yaml", "secrets.yaml", ".env"} {
		if _, err := os.Stat(filepath.Join(root(t), "deploy/kubernetes/base", bad)); err == nil {
			t.Errorf("deploy/kubernetes/base/%s must not exist", bad)
		}
	}
}

func TestBaseRendersToo(t *testing.T) {
	base := parse(t, render(t, "deploy/kubernetes/base"))
	over := overlayManifests(t)
	if len(base.Raw) != len(over.Raw) {
		t.Errorf("base renders %d objects and the overlay %d: the overlay only sets values (images)", len(base.Raw), len(over.Raw))
	}
	if !strings.Contains(string(render(t, overlay)), "image: serverflow/admin:dev") {
		t.Error("the overlay's images: transformer did not set the admin tag")
	}
	if strings.Contains(string(render(t, "deploy/kubernetes/base")), "serverflow/admin:") {
		t.Error("the base must leave the tag to the overlay")
	}
}

// TestOverlayImageTagsAreExplicit reads the overlay file itself (no render needed): a ServerFlow image gets a tag that is not
// latest, so a mutation of it fails even where no container runtime is available.
func TestOverlayImageTagsAreExplicit(t *testing.T) {
	var k struct {
		Images []struct {
			Name   string `yaml:"name"`
			NewTag string `yaml:"newTag"`
		} `yaml:"images"`
	}
	if err := yaml.Unmarshal([]byte(read(t, overlay+"/kustomization.yaml")), &k); err != nil {
		t.Fatal(err)
	}
	if len(k.Images) == 0 {
		t.Fatal("the overlay sets no image tags")
	}
	for _, im := range k.Images {
		if !strings.HasPrefix(im.Name, "serverflow/") || im.NewTag == "" || im.NewTag == "latest" || !regexp.MustCompile(`^[A-Za-z0-9._-]+$`).MatchString(im.NewTag) {
			t.Errorf("overlay image %s: tag %q must be explicit and not latest", im.Name, im.NewTag)
		}
	}
}
