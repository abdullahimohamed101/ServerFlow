package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCacheHitsAndMissesAreCounted(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	key, _ := st.add(1)
	for i := 0; i < 10; i++ {
		if _, err := a.Authenticate(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	if h, m := a.CacheStats(); h != 9 || m != 1 {
		t.Fatalf("hits %d misses %d, want 9 and 1 (one cold lookup)", h, m)
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(a.Collector())
	want := `# HELP cache_hits_total Cache lookups answered from the cache, by cache.
# TYPE cache_hits_total counter
cache_hits_total{cache="auth_key"} 9
# HELP cache_misses_total Cache lookups that needed the backing store, by cache.
# TYPE cache_misses_total counter
cache_misses_total{cache="auth_key"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
