package observability

import (
	"reflect"
	"testing"
)

func TestMetricNames(t *testing.T) {
	for expr, want := range map[string][]string{
		`up`: {"up"},
		`sum(rate(inference_requests_total{status=~"5..", model=~"$model"}[5m]))`:                                   {"inference_requests_total"},
		`histogram_quantile(0.95, sum by (model, le) (rate(inference_ttft_seconds_bucket[$__rate_interval])))`:      {"inference_ttft_seconds_bucket"},
		`sum(a_total) / ignoring(worker_id) group_left sum(b_total)`:                                                {"a_total", "b_total"},
		`max by (model) (worker_queue_depth) - min by (model) (worker_queue_depth)`:                                 {"worker_queue_depth"},
		`worker_queue_depth > on(worker_id) worker_queue_capacity`:                                                  {"worker_queue_capacity", "worker_queue_depth"},
		`cluster:inference_errors:ratio_rate5m > (14.4 * 0.001) and cluster:inference_errors:ratio_rate1h > 0.0144`: {"cluster:inference_errors:ratio_rate1h", "cluster:inference_errors:ratio_rate5m"},
		`increase(x_total[5m]) > 0 or vector(0)`:                                                                    {"x_total"},
		`sum(increase(scheduler_ineligible_selections_total[$__range]))`:                                            {"scheduler_ineligible_selections_total"},
		`label_replace(up{job="a by (x)"}, "x", "$1", "job", "(.*)")`:                                               {"up"},
		`sum by (alertname, severity) (ALERTS{alertstate="firing", alertname=~"ServerFlow.*"})`:                     {"ALERTS"},
	} {
		if got := MetricNames(expr); !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n got  %v\n want %v", expr, got, want)
		}
	}
}

func TestVariables(t *testing.T) {
	got := Variables(`rate(x{a=~"$model", w=~"${worker_id}", z=~"$model"}[$__rate_interval]) + sum(y[$__range])`)
	if want := []string{"__range", "__rate_interval", "model", "worker_id"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
	if got := Variables(`rate(x[5m])`); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestFamily(t *testing.T) {
	fam := map[string]bool{"h_seconds": true, "c_total": true, "odd_count": true}
	for in, want := range map[string]string{"h_seconds_bucket": "h_seconds", "h_seconds_sum": "h_seconds", "c_total": "c_total", "odd_count": "odd_count"} {
		if got, ok := Family(in, fam); !ok || got != want {
			t.Errorf("%s -> %s %v", in, got, ok)
		}
	}
	if _, ok := Family("nope_bucket", fam); ok {
		t.Error("an unknown family matched")
	}
}

func TestLabelNames(t *testing.T) {
	for expr, want := range map[string][]string{
		`sum by (worker_id, le) (rate(x_bucket{model=~"$model", result="failed"}[5m]))`: {"le", "model", "result", "worker_id"},
		`a / ignoring(worker_id) group_left(model) b`:                                   {"model", "worker_id"},
		`max by (model) (q) - min by (model) (q)`:                                       {"model"},
		`ALERTS{alertstate="firing", alertname=~"ServerFlow.*"}`:                        {"alertname", "alertstate"},
		`rate(x{status=~"5..", note="a by (zz)"}[1m])`:                                  {"note", "status"},
		`sum without (instance) (up{job!="x"})`:                                         {"instance", "job"},
		`sum(rate(x[5m]))`:                                                              {},
	} {
		got := LabelNames(expr)
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n got  %v\n want %v", expr, got, want)
		}
	}
}
