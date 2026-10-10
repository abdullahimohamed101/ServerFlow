package observability

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// root finds the repository root (the directory holding go.mod).
func root(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func read(t testing.TB, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// --- dashboards -------------------------------------------------------------------------------------------------

type dashboard struct {
	File       string
	UID        string `json:"uid"`
	Title      string `json:"title"`
	Editable   bool   `json:"editable"`
	Schema     int    `json:"schemaVersion"`
	Templating struct {
		List []variable `json:"list"`
	} `json:"templating"`
	Panels []panel `json:"panels"`
}

type variable struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Query   any    `json:"query"`
	Current struct {
		Value any `json:"value"`
	} `json:"current"`
	Datasource *datasource `json:"datasource"`
}

type datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type panel struct {
	ID          int         `json:"id"`
	Type        string      `json:"type"`
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Datasource  *datasource `json:"datasource"`
	Targets     []struct {
		Expr       string      `json:"expr"`
		Datasource *datasource `json:"datasource"`
	} `json:"targets"`
}

func dashboards(t testing.TB) []dashboard {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root(t), "observability/grafana/dashboards/*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards found: %v", err)
	}
	sort.Strings(files)
	var out []dashboard
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d dashboard
		dec := json.NewDecoder(strings.NewReader(string(b)))
		if err := dec.Decode(&d); err != nil {
			t.Fatalf("%s is not valid JSON for a dashboard: %v", filepath.Base(f), err)
		}
		d.File = filepath.Base(f)
		out = append(out, d)
	}
	return out
}

// dashboardExprs returns every PromQL expression of every panel, with where it came from.
func dashboardExprs(t testing.TB) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, d := range dashboards(t) {
		for _, p := range d.Panels {
			for i, tg := range p.Targets {
				out[fmt.Sprintf("%s panel %d %q target %d", d.File, p.ID, p.Title, i)] = tg.Expr
			}
		}
	}
	return out
}

// The panels spec section 30 asks for, by dashboard uid. A panel may be renamed only by changing this list.
var requiredPanels = map[string][]string{
	"sf-cluster-overview": {"Requests per second", "Tokens per second", "Request latency p50 / p95 / p99", "Time to first token p95", "Error rate (5xx)", "Healthy workers", "GPU utilisation"},
	"sf-worker":           {"Queue depth", "Active requests", "GPU utilisation", "Tokens per second", "Request latency p95", "Time to first token p95", "Error rate"},
	"sf-scheduler":        {"Routed requests per second", "Queue imbalance", "Scheduling decisions per second", "Selection distribution", "No-capacity events"},
	"sf-model":            {"Requests per second", "Tokens per second", "Active replicas", "Queue depth", "Request latency p50 / p95 / p99", "Time to first token p95"},
}

func TestFourDashboardsWithTheSpecPanels(t *testing.T) {
	ds := dashboards(t)
	if len(ds) != 4 {
		t.Fatalf("%d dashboards, want the four of spec section 30", len(ds))
	}
	uids := map[string]string{}
	for _, d := range ds {
		if prev, dup := uids[d.UID]; dup {
			t.Errorf("uid %q in both %s and %s", d.UID, prev, d.File)
		}
		uids[d.UID] = d.File
		want, ok := requiredPanels[d.UID]
		if !ok {
			t.Errorf("%s: unexpected uid %q", d.File, d.UID)
			continue
		}
		have := map[string]bool{}
		for _, p := range d.Panels {
			have[p.Title] = true
		}
		for _, title := range want {
			if !have[title] {
				t.Errorf("%s lacks the panel %q (spec section 30)", d.File, title)
			}
		}
	}
}

func TestDashboardStructure(t *testing.T) {
	for _, d := range dashboards(t) {
		if d.Title == "" || d.UID == "" || !strings.HasPrefix(d.UID, "sf-") {
			t.Errorf("%s: title %q uid %q", d.File, d.Title, d.UID)
		}
		if d.Editable {
			t.Errorf("%s: dashboards are provisioned from the repository and must not be editable in Grafana", d.File)
		}
		if d.Schema != 39 {
			t.Errorf("%s: schemaVersion %d is not the pinned 39", d.File, d.Schema)
		}
		ids := map[int]bool{}
		defined := map[string]bool{"__range": true, "__rate_interval": true, "__interval": true}
		for _, v := range d.Templating.List {
			defined[v.Name] = true
			switch v.Name {
			case "datasource":
				if v.Type != "datasource" || v.Current.Value != "serverflow-prometheus" {
					t.Errorf("%s: the datasource variable must be a prometheus datasource variable fixed to serverflow-prometheus, got %+v", d.File, v)
				}
			default:
				if v.Type != "query" || v.Datasource == nil || v.Datasource.UID != "${datasource}" {
					t.Errorf("%s: variable %s must be a query variable on ${datasource}", d.File, v.Name)
				}
			}
		}
		if !defined["datasource"] {
			t.Errorf("%s: no datasource variable", d.File)
		}
		for _, p := range d.Panels {
			if ids[p.ID] || p.ID == 0 {
				t.Errorf("%s: panel id %d is zero or repeated", d.File, p.ID)
			}
			ids[p.ID] = true
			if p.Title == "" || p.Description == "" {
				t.Errorf("%s panel %d: title and description are required (a panel whose source is missing must say so)", d.File, p.ID)
			}
			if p.Datasource == nil || p.Datasource.UID != "${datasource}" || p.Datasource.Type != "prometheus" {
				t.Errorf("%s panel %d: datasource must be the ${datasource} variable, got %+v", d.File, p.ID, p.Datasource)
			}
			for i, tg := range p.Targets {
				if tg.Expr == "" {
					t.Errorf("%s panel %d target %d: empty expression", d.File, p.ID, i)
				}
				if tg.Datasource == nil || tg.Datasource.UID != "${datasource}" {
					t.Errorf("%s panel %d target %d: datasource must be ${datasource}", d.File, p.ID, i)
				}
				for _, u := range Variables(tg.Expr) {
					if !defined[u] {
						t.Errorf("%s panel %d target %d: variable $%s is not defined by the dashboard", d.File, p.ID, i, u)
					}
				}
			}
		}
	}
	// No dashboard may name a datasource by its display name or point at another uid.
	for _, f := range []string{"cluster-overview", "worker", "scheduler", "model"} {
		raw := string(read(t, "observability/grafana/dashboards/"+f+".json"))
		if strings.Contains(raw, "ServerFlow Prometheus\"") && !strings.Contains(raw, `"regex": "/^ServerFlow Prometheus$/"`) {
			t.Errorf("%s names the datasource inline", f)
		}
		for _, m := range regexp.MustCompile(`"uid":\s*"([^"]+)"`).FindAllStringSubmatch(raw, -1) {
			if !strings.HasPrefix(m[1], "sf-") && m[1] != "${datasource}" {
				t.Errorf("%s refers to the uid %q", f, m[1])
			}
		}
	}
}

// --- provisioning and compose ------------------------------------------------------------------------------------

func TestProvisioningMatchesTheDashboards(t *testing.T) {
	var prov struct {
		Datasources []struct {
			Name, UID, Type, URL string
			Editable             bool
		} `yaml:"datasources"`
	}
	if err := yaml.Unmarshal(read(t, "observability/grafana/provisioning/datasources/prometheus.yml"), &prov); err != nil {
		t.Fatal(err)
	}
	if len(prov.Datasources) != 1 || prov.Datasources[0].UID != "serverflow-prometheus" || prov.Datasources[0].Type != "prometheus" {
		t.Fatalf("datasource provisioning: %+v", prov.Datasources)
	}
	if prov.Datasources[0].Editable {
		t.Error("the datasource must not be editable")
	}
	// The dashboards' datasource variable selects it by display name through a regex.
	if !strings.Contains(string(read(t, "observability/grafana/dashboards/worker.json")), "/^"+prov.Datasources[0].Name+"$/") {
		t.Errorf("the dashboards' datasource regex does not select %q", prov.Datasources[0].Name)
	}

	var dprov struct {
		Providers []struct {
			Options map[string]any `yaml:"options"`
		} `yaml:"providers"`
	}
	if err := yaml.Unmarshal(read(t, "observability/grafana/provisioning/dashboards/serverflow.yml"), &dprov); err != nil {
		t.Fatal(err)
	}
	if len(dprov.Providers) != 1 || dprov.Providers[0].Options["path"] != "/var/lib/grafana/dashboards" {
		t.Fatalf("dashboard provisioning: %+v", dprov.Providers)
	}

	compose := string(read(t, "observability/docker-compose.yml"))
	for _, want := range []string{
		"./grafana/dashboards:/var/lib/grafana/dashboards:ro", "./grafana/provisioning:/etc/grafana/provisioning:ro",
		"./prometheus:/etc/prometheus:ro",
	} {
		if !strings.Contains(compose, want) {
			t.Errorf("compose does not mount %s", want)
		}
	}
}

func TestComposeIsClosedByDefault(t *testing.T) {
	var c struct {
		Services map[string]struct {
			Image       string
			Ports       []string
			Environment map[string]string
		} `yaml:"services"`
	}
	raw := read(t, "observability/docker-compose.yml")
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Services) != 2 {
		t.Fatalf("services: %v", c.Services)
	}
	for name, s := range c.Services {
		for _, p := range s.Ports {
			if !strings.HasPrefix(p, "127.0.0.1:") {
				t.Errorf("%s publishes %q beyond loopback", name, p)
			}
		}
		if !regexp.MustCompile(`:v?\d+\.\d+\.\d+$`).MatchString(s.Image) {
			t.Errorf("%s image %q is not pinned to a version", name, s.Image)
		}
	}
	g := c.Services["grafana"].Environment
	if !strings.Contains(g["GF_SECURITY_ADMIN_PASSWORD"], ":?") {
		t.Errorf("the Grafana admin password must be required from the environment (${VAR:?...}), got %q", g["GF_SECURITY_ADMIN_PASSWORD"])
	}
	if g["GF_AUTH_ANONYMOUS_ENABLED"] != "false" {
		t.Error("anonymous Grafana access must be off")
	}
	// Nothing tracked may carry a password: the example sets none, and .env is ignored by git.
	if ex := string(read(t, "observability/.env.example")); regexp.MustCompile(`(?m)^GRAFANA_ADMIN_PASSWORD=\S`).MatchString(ex) {
		t.Error(".env.example commits a password")
	}
	if out, err := exec.Command("git", "-C", root(t), "check-ignore", "-q", "observability/.env").CombinedOutput(); err != nil {
		t.Errorf("observability/.env is not git-ignored: %v %s", err, out)
	}
}

// --- rules ---------------------------------------------------------------------------------------------------------

type ruleFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Record      string            `yaml:"record"`
			Alert       string            `yaml:"alert"`
			Expr        string            `yaml:"expr"`
			For         string            `yaml:"for"`
			Labels      map[string]string `yaml:"labels"`
			Annotations map[string]string `yaml:"annotations"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func loadRules(t testing.TB, rel string) ruleFile {
	t.Helper()
	var r ruleFile
	if err := yaml.Unmarshal(read(t, rel), &r); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return r
}

// recordingNames and alertNames list what the rule files define.
func recordingNames(t testing.TB) []string {
	var out []string
	for _, g := range loadRules(t, "observability/prometheus/rules/recording.yml").Groups {
		for _, r := range g.Rules {
			out = append(out, r.Record)
		}
	}
	return out
}

func alertNames(t testing.TB) []string {
	var out []string
	for _, g := range loadRules(t, "observability/prometheus/rules/alerts.yml").Groups {
		for _, r := range g.Rules {
			out = append(out, r.Alert)
		}
	}
	return out
}

func TestEveryAlertHasSummarySeverityAndARunbookThatExists(t *testing.T) {
	doc := string(read(t, "docs/operations/observability.md"))
	for _, g := range loadRules(t, "observability/prometheus/rules/alerts.yml").Groups {
		for _, r := range g.Rules {
			if r.Annotations["summary"] == "" || r.Labels["severity"] == "" {
				t.Errorf("%s lacks a summary or a severity", r.Alert)
			}
			rb := r.Annotations["runbook"]
			file, anchor, ok := strings.Cut(rb, "#")
			if !ok || file != "docs/operations/observability.md" {
				t.Errorf("%s: runbook %q must point into docs/operations/observability.md#anchor", r.Alert, rb)
				continue
			}
			if !strings.Contains(doc, `<a id="`+anchor+`"></a>`) {
				t.Errorf("%s: the operations doc has no <a id=%q></a> section", r.Alert, anchor)
			}
		}
	}
}

// Every recording rule needs a test sample, and every alert a firing case and a non-firing one.
func TestEveryRuleHasATest(t *testing.T) {
	rec := string(read(t, "observability/prometheus/tests/recording.test.yml"))
	for _, n := range recordingNames(t) {
		if !strings.Contains(rec, "- expr: "+n+"\n") {
			t.Errorf("recording rule %s has no promql_expr_test", n)
		}
	}
	var tests struct {
		Tests []struct {
			Name           string `yaml:"name"`
			AlertRuleTests []struct {
				Alertname string           `yaml:"alertname"`
				ExpAlerts []map[string]any `yaml:"exp_alerts"`
			} `yaml:"alert_rule_test"`
		} `yaml:"tests"`
	}
	if err := yaml.Unmarshal(read(t, "observability/prometheus/tests/alerts.test.yml"), &tests); err != nil {
		t.Fatal(err)
	}
	fires, quiet := map[string]int{}, map[string]int{}
	for _, tc := range tests.Tests {
		for _, a := range tc.AlertRuleTests {
			if len(a.ExpAlerts) > 0 {
				fires[a.Alertname]++
			} else {
				quiet[a.Alertname]++
			}
		}
	}
	for _, n := range alertNames(t) {
		if fires[n] == 0 || quiet[n] == 0 {
			t.Errorf("alert %s needs a firing case and a non-firing case (has %d and %d)", n, fires[n], quiet[n])
		}
	}
}

func TestScrapeJobsLimitSamples(t *testing.T) {
	var cfg struct {
		ScrapeConfigs []struct {
			JobName     string `yaml:"job_name"`
			SampleLimit int    `yaml:"sample_limit"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(read(t, "observability/prometheus/prometheus.yml"), &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.ScrapeConfigs) != 3 {
		t.Fatalf("scrape jobs: %+v", cfg.ScrapeConfigs)
	}
	for _, j := range cfg.ScrapeConfigs {
		if j.SampleLimit <= 0 || j.SampleLimit > 20000 {
			t.Errorf("%s: sample_limit %d (want 1-20000, the last line of defence in ADR-017)", j.JobName, j.SampleLimit)
		}
	}
}
