package k8s

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"gopkg.in/yaml.v3"
)

// The subset of the Kubernetes API the policies read. Unknown fields are ignored; kubeconform checks the schema.

type objMeta struct {
	Name      string            `yaml:"name"`
	Namespace string            `yaml:"namespace"`
	Labels    map[string]string `yaml:"labels"`
}

type httpGet struct {
	Path string `yaml:"path"`
	Port any    `yaml:"port"`
}

type probe struct {
	Exec *struct {
		Command []string `yaml:"command"`
	} `yaml:"exec"`
	HTTPGet          *httpGet            `yaml:"httpGet"`
	TCPSocket        *struct{ Port any } `yaml:"tcpSocket"`
	PeriodSeconds    int                 `yaml:"periodSeconds"`
	TimeoutSeconds   int                 `yaml:"timeoutSeconds"`
	FailureThreshold int                 `yaml:"failureThreshold"`
}

type envVar struct {
	Name      string `yaml:"name"`
	Value     string `yaml:"value"`
	ValueFrom *struct {
		SecretKeyRef *struct{ Name, Key string } `yaml:"secretKeyRef"`
	} `yaml:"valueFrom"`
}

type secCtx struct {
	Privileged               *bool                  `yaml:"privileged"`
	AllowPrivilegeEscalation *bool                  `yaml:"allowPrivilegeEscalation"`
	ReadOnlyRootFilesystem   *bool                  `yaml:"readOnlyRootFilesystem"`
	RunAsNonRoot             *bool                  `yaml:"runAsNonRoot"`
	RunAsUser                *int64                 `yaml:"runAsUser"`
	RunAsGroup               *int64                 `yaml:"runAsGroup"`
	FSGroup                  *int64                 `yaml:"fsGroup"`
	SeccompProfile           *struct{ Type string } `yaml:"seccompProfile"`
	Capabilities             *struct {
		Add  []string `yaml:"add"`
		Drop []string `yaml:"drop"`
	} `yaml:"capabilities"`
}

type container struct {
	Name            string   `yaml:"name"`
	Image           string   `yaml:"image"`
	ImagePullPolicy string   `yaml:"imagePullPolicy"`
	Command         []string `yaml:"command"`
	Args            []string `yaml:"args"`
	Ports           []struct {
		Name          string `yaml:"name"`
		ContainerPort int    `yaml:"containerPort"`
	} `yaml:"ports"`
	Env     []envVar `yaml:"env"`
	EnvFrom []struct {
		SecretRef    *struct{ Name string } `yaml:"secretRef"`
		ConfigMapRef *struct{ Name string } `yaml:"configMapRef"`
	} `yaml:"envFrom"`
	SecurityContext *secCtx `yaml:"securityContext"`
	Resources       struct {
		Requests map[string]string `yaml:"requests"`
		Limits   map[string]string `yaml:"limits"`
	} `yaml:"resources"`
	StartupProbe   *probe `yaml:"startupProbe"`
	ReadinessProbe *probe `yaml:"readinessProbe"`
	LivenessProbe  *probe `yaml:"livenessProbe"`
	VolumeMounts   []struct {
		Name      string `yaml:"name"`
		MountPath string `yaml:"mountPath"`
	} `yaml:"volumeMounts"`
}

type volume struct {
	Name     string `yaml:"name"`
	EmptyDir *struct {
		Medium    string `yaml:"medium"`
		SizeLimit string `yaml:"sizeLimit"`
	} `yaml:"emptyDir"`
	HostPath *struct{ Path string } `yaml:"hostPath"`
	Secret   *struct {
		SecretName string `yaml:"secretName"`
	} `yaml:"secret"`
}

type podSpec struct {
	ServiceAccountName            string      `yaml:"serviceAccountName"`
	AutomountServiceAccountToken  *bool       `yaml:"automountServiceAccountToken"`
	HostNetwork                   bool        `yaml:"hostNetwork"`
	HostPID                       bool        `yaml:"hostPID"`
	HostIPC                       bool        `yaml:"hostIPC"`
	ShareProcessNamespace         *bool       `yaml:"shareProcessNamespace"`
	TerminationGracePeriodSeconds *int64      `yaml:"terminationGracePeriodSeconds"`
	RestartPolicy                 string      `yaml:"restartPolicy"`
	SecurityContext               *secCtx     `yaml:"securityContext"`
	InitContainers                []container `yaml:"initContainers"`
	Containers                    []container `yaml:"containers"`
	Volumes                       []volume    `yaml:"volumes"`
}

type workload struct {
	APIVersion string  `yaml:"apiVersion"`
	Kind       string  `yaml:"kind"`
	Metadata   objMeta `yaml:"metadata"`
	Spec       struct {
		Replicas              *int   `yaml:"replicas"`
		ServiceName           string `yaml:"serviceName"`
		BackoffLimit          *int   `yaml:"backoffLimit"`
		ActiveDeadlineSeconds *int   `yaml:"activeDeadlineSeconds"`
		Selector              struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"selector"`
		Template struct {
			Metadata objMeta `yaml:"metadata"`
			Spec     podSpec `yaml:"spec"`
		} `yaml:"template"`
		VolumeClaimTemplates []struct {
			Metadata objMeta `yaml:"metadata"`
			Spec     struct {
				AccessModes []string `yaml:"accessModes"`
				Resources   struct {
					Requests map[string]string `yaml:"requests"`
				} `yaml:"resources"`
			} `yaml:"spec"`
		} `yaml:"volumeClaimTemplates"`
	} `yaml:"spec"`
}

type service struct {
	Metadata objMeta `yaml:"metadata"`
	Spec     struct {
		Type     string            `yaml:"type"`
		Selector map[string]string `yaml:"selector"`
		Ports    []struct {
			Name       string `yaml:"name"`
			Port       int    `yaml:"port"`
			TargetPort any    `yaml:"targetPort"`
			NodePort   int    `yaml:"nodePort"`
		} `yaml:"ports"`
	} `yaml:"spec"`
}

// pod is one pod template with the object that owns it.
type pod struct {
	Kind     string // Deployment, StatefulSet, Job
	Name     string
	Replicas int
	Labels   map[string]string
	Spec     podSpec
	W        workload
}

func (p pod) id() string { return p.Kind + "/" + p.Name }

// longRunning pods must have probes; a Job's pod runs to completion.
func (p pod) longRunning() bool { return p.Kind != "Job" && p.Kind != "CronJob" }

func (p pod) all() []container {
	return append(append([]container{}, p.Spec.InitContainers...), p.Spec.Containers...)
}

// manifests is the parsed render of one kustomize directory.
type manifests struct {
	Raw      []map[string]any
	Pods     []pod
	Services []service
}

var podKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "Job": true}

func parse(t testing.TB, rendered []byte) manifests {
	t.Helper()
	var m manifests
	m.Raw = decodeAll(t, rendered)
	for _, d := range m.Raw {
		b, err := yaml.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		kind, _ := d["kind"].(string)
		switch {
		case podKinds[kind]:
			var w workload
			if err := yaml.Unmarshal(b, &w); err != nil {
				t.Fatalf("%s: %v", kind, err)
			}
			n := 1
			if w.Spec.Replicas != nil {
				n = *w.Spec.Replicas
			}
			if kind == "Job" {
				n = 1
			}
			m.Pods = append(m.Pods, pod{Kind: kind, Name: w.Metadata.Name, Replicas: n, Labels: w.Spec.Template.Metadata.Labels, Spec: w.Spec.Template.Spec, W: w})
		case kind == "Service":
			var s service
			if err := yaml.Unmarshal(b, &s); err != nil {
				t.Fatal(err)
			}
			m.Services = append(m.Services, s)
		}
	}
	return m
}

func overlayManifests(t testing.TB) manifests { return parse(t, render(t, overlay)) }

func (m manifests) pod(t testing.TB, id string) pod {
	t.Helper()
	for _, p := range m.Pods {
		if p.id() == id {
			return p
		}
	}
	t.Fatalf("no %s in the render", id)
	return pod{}
}

var quantity = regexp.MustCompile(`^([0-9]+)(Mi|Gi|Ki|m)?$`)

// memMiB returns a memory quantity in MiB (Mi and Gi only); ok is false for anything else.
func memMiB(q string) (int, bool) {
	m := quantity.FindStringSubmatch(q)
	if m == nil {
		return 0, false
	}
	n, _ := strconv.Atoi(m[1])
	switch m[2] {
	case "Mi":
		return n, true
	case "Gi":
		return n * 1024, true
	}
	return 0, false
}

func boolIs(p *bool, want bool) bool { return p != nil && *p == want }

func fmtv(format string, a ...any) string { return fmt.Sprintf(format, a...) }
