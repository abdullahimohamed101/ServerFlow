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

func TestUnresolvedVariables(t *testing.T) {
	if got := UnresolvedVariables(`rate(x{a=~"$model", w=~"$worker_id"}[$__rate_interval])`); len(got) != 0 {
		t.Fatal(got)
	}
	if got := UnresolvedVariables(`rate(x{a=~"$mdoel"}[5m])`); !reflect.DeepEqual(got, []string{"$mdoel"}) {
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
