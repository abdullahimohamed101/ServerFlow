// Package ciguard holds a guard over the CI workflows: an image from a registry must be pulled through
// scripts/ci-docker-pull.sh (retries with backoff, optional cache tarball), never by a bare `docker run` or
// `docker pull`, and never as a `services:` container, which the runner pulls without retries. An anonymous
// pull from a shared runner is rate limited ("toomanyrequests: Rate exceeded") and one failed pull would
// otherwise fail the whole job.
package ciguard

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// imageRef matches a registry image reference such as public.ecr.aws/docker/library/redis:7 or
// ghcr.io/org/app@sha256:...; a bare local tag like serverflow/gateway:dev has no registry host.
var imageRef = regexp.MustCompile(`[a-z0-9-]+(?:\.[a-z0-9-]+)+(?::[0-9]+)?/[a-z0-9._/-]+(?::[A-Za-z0-9._-]+)?(?:@sha256:[0-9a-f]+)?`)

// violations returns what is wrong with one workflow file's text.
func violations(name, text string) []string {
	var out []string
	for i, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		where := name + ":" + strconv.Itoa(i+1)
		if trimmed == "services:" {
			out = append(out, where+": a services: container is pulled by the runner without retries; start it with scripts/ci-docker-pull.sh and docker run")
		}
		if strings.Contains(line, "docker pull") {
			out = append(out, where+": use scripts/ci-docker-pull.sh instead of docker pull")
		}
		if !strings.Contains(line, "docker run") && !strings.Contains(line, "docker create") {
			continue
		}
		for _, img := range imageRef.FindAllString(line, -1) {
			if !strings.Contains(text, "scripts/ci-docker-pull.sh "+img) {
				out = append(out, where+": docker run of "+img+" without scripts/ci-docker-pull.sh "+img+" before it")
			}
		}
	}
	return out
}

func TestWorkflowsPullRegistryImagesWithRetries(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", ".github", "workflows", "*.yml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no workflow files found (%v): the guard would silently stop covering them", err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range violations(filepath.Base(f), string(b)) {
			t.Error(v)
		}
	}
}

func TestGuardCatchesTheBadPatterns(t *testing.T) {
	bad := map[string]string{
		"bare docker run":    "      run: docker run -d public.ecr.aws/docker/library/redis:7 redis-server\n",
		"docker pull":        "      run: docker pull ghcr.io/org/app:1\n",
		"services container": "    services:\n      postgres:\n        image: public.ecr.aws/docker/library/postgres:16\n",
		"other image":        "      run: |\n        scripts/ci-docker-pull.sh public.ecr.aws/docker/library/redis:7\n        docker run -d quay.io/org/other:1\n",
	}
	for name, text := range bad {
		if len(violations("x.yml", text)) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
	good := "      run: |\n        scripts/ci-docker-pull.sh public.ecr.aws/docker/library/redis:7 \"$T/r.tar\"\n        docker run -d --name redis public.ecr.aws/docker/library/redis:7 redis-server\n        docker run --rm serverflow/gateway:dev\n"
	if v := violations("x.yml", good); len(v) != 0 {
		t.Errorf("a compliant workflow was flagged: %v", v)
	}
}
