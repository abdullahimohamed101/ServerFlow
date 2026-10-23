package k8s

import (
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var (
	digestRef      = regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)+(:[0-9]+)?/[a-z0-9._/-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$`)
	composeDefault = regexp.MustCompile(`^\$\{[A-Z_]+:-(.+)\}$`)
)

// repoOf strips the tag and the digest: mirror.gcr.io/library/postgres:16-alpine@sha256:... -> mirror.gcr.io/library/postgres.
func repoOf(ref string) string {
	ref, _, _ = strings.Cut(ref, "@")
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return ref
}

// composeImages returns repo -> the full pinned reference docker-compose.yml defaults to (ServerFlow's own images excluded).
func composeImages(t testing.TB) map[string]string {
	t.Helper()
	var doc struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(read(t, "docker-compose.yml")), &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for name, s := range doc.Services {
		ref := s.Image
		if m := composeDefault.FindStringSubmatch(ref); m != nil {
			ref = m[1]
		}
		if ref == "" || strings.HasPrefix(ref, "serverflow/") {
			continue
		}
		if prev, dup := out[repoOf(ref)]; dup && prev != ref {
			t.Fatalf("docker-compose.yml pins %s twice with different references (service %s)", repoOf(ref), name)
		}
		out[repoOf(ref)] = ref
	}
	if len(out) < 6 {
		t.Fatalf("expected the third-party images of docker-compose.yml, found %d", len(out))
	}
	return out
}

var serverflowImages = map[string]bool{"gateway": true, "control-plane": true, "worker-agent": true, "mock-worker": true, "usage-consumer": true, "admin": true}

// checkImages: a third-party image is pinned by tag and digest and is the one docker-compose.yml pins; a ServerFlow image is
// one of the six, tagged (not latest) and built and loaded by scripts/k8s-up.sh.
func checkImages(t testing.TB, p pod) []string {
	var v []string
	compose := composeImages(t)
	built := map[string]bool{}
	for _, n := range scriptWords(t, "IMAGES") {
		built[n] = true
	}
	for _, c := range p.all() {
		who := p.id() + ": container " + c.Name + " image " + c.Image
		if strings.HasPrefix(c.Image, "serverflow/") {
			name, tag, _ := strings.Cut(strings.TrimPrefix(c.Image, "serverflow/"), ":")
			switch {
			case !serverflowImages[name]:
				v = append(v, who+": not one of the six ServerFlow images")
			case tag == "" || tag == "latest":
				v = append(v, who+": needs a tag other than latest (the overlay's images: transformer sets it)")
			case !built[name]:
				v = append(v, who+": scripts/k8s-up.sh does not build and load it (IMAGES)")
			}
		} else {
			if !digestRef.MatchString(c.Image) {
				v = append(v, who+": a third-party image must be pinned as host/repo:tag@sha256:digest")
			}
			if want, ok := compose[repoOf(c.Image)]; !ok {
				v = append(v, who+": docker-compose.yml does not pin this repository (one set of pins)")
			} else if want != c.Image {
				v = append(v, who+": differs from docker-compose.yml, which pins "+want)
			}
			if strings.HasPrefix(c.Image, "docker.io/") || !strings.Contains(strings.SplitN(c.Image, "/", 2)[0], ".") {
				v = append(v, who+": Docker Hub is avoided (shared-runner rate limits); use mirror.gcr.io or quay.io")
			}
		}
		if c.ImagePullPolicy != "IfNotPresent" {
			v = append(v, who+": imagePullPolicy must be IfNotPresent (images are loaded or pinned by digest)")
		}
	}
	return v
}

// TestThirdPartyImagesEqualCompose is the drift test of D5: the data tier runs the images docker-compose.yml pins, and every
// data-tier image of Compose is used (later pull requests extend dataTierRepos with their images).
func TestThirdPartyImagesEqualCompose(t *testing.T) {
	m := overlayManifests(t)
	used := map[string]bool{}
	for _, p := range m.Pods {
		for _, v := range checkImages(t, p) {
			t.Error(v)
		}
		for _, c := range p.all() {
			used[repoOf(c.Image)] = true
		}
	}
	for _, repo := range []string{"mirror.gcr.io/library/postgres", "mirror.gcr.io/library/redis", "mirror.gcr.io/redpandadata/redpanda"} {
		if !used[repo] {
			t.Errorf("no workload uses %s, which docker-compose.yml runs", repo)
		}
		if composeImages(t)[repo] == "" {
			t.Errorf("docker-compose.yml does not pin %s", repo)
		}
	}
}

// TestEveryServerFlowImageIsBuiltByTheScript: IMAGES in k8s-up.sh lists exactly the ServerFlow images the render uses.
func TestEveryServerFlowImageIsBuiltByTheScript(t *testing.T) {
	used := map[string]bool{}
	for _, p := range overlayManifests(t).Pods {
		for _, c := range p.all() {
			if n, ok := strings.CutPrefix(c.Image, "serverflow/"); ok {
				used[strings.SplitN(n, ":", 2)[0]] = true
			}
		}
	}
	for _, n := range scriptWords(t, "IMAGES") {
		if !serverflowImages[n] {
			t.Errorf("k8s-up.sh builds %q, which is not a ServerFlow image target", n)
		}
		if !used[n] {
			t.Errorf("k8s-up.sh builds and loads %s, but no workload uses it", n)
		}
		delete(used, n)
	}
	for n := range used {
		t.Errorf("a workload uses serverflow/%s, which k8s-up.sh does not build and load", n)
	}
}
