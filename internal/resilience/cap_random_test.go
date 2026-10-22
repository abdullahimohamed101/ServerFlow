package resilience

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Per-model ejection cap under random operations: Sync with eligibility flips and model moves, Prune, faults and clock
// advance. After every operation the open plus half-open breakers of each model are at most MaxOpen(0.5, eligible workers of
// that model), so a model with one eligible worker never has one open.
func TestRandomPerModelCapInvariant(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		r := newRig(t, capParams())
		nw := 3 + rng.Intn(10)
		nm := 1 + rng.Intn(4)
		mem := make([]Member, nw)
		for i := range mem {
			mem[i] = Member{Key: key(fmt.Sprintf("w%d", i)), Model: fmt.Sprintf("m%d", rng.Intn(nm)), Eligible: rng.Intn(5) > 0}
		}
		r.bs.Sync(mem)
		check := func(op string) {
			el := map[string]int{}
			open := map[string]int{}
			info := map[string]Info{}
			for _, i := range r.bs.Snapshot() {
				info[i.Key.ID] = i
			}
			for _, m := range mem {
				if m.Eligible {
					el[m.Model]++
					if r.bs.State(m.Key) != Closed {
						open[m.Model]++
					}
				}
			}
			for model, n := range open {
				if n > MaxOpen(0.5, el[model]) {
					t.Fatalf("seed %d after %s: model %s has %d open of %d eligible", seed, op, model, n, el[model])
				}
			}
		}
		for step := 0; step < 400 && len(mem) > 0; step++ {
			switch rng.Intn(6) {
			case 0, 1, 2:
				m := mem[rng.Intn(len(mem))]
				if tk, ok := r.bs.Acquire(m.Key); ok {
					v := Fault
					if rng.Intn(6) == 0 {
						v = OK
					}
					r.bs.Record(tk, v)
				}
				check("send")
			case 3:
				r.clk.Advance(time.Duration(rng.Intn(15)) * time.Second)
				check("advance")
			case 4:
				i := rng.Intn(len(mem))
				mem[i].Eligible = !mem[i].Eligible
				if rng.Intn(4) == 0 {
					mem[i].Model = fmt.Sprintf("m%d", rng.Intn(nm))
				}
				r.bs.Sync(mem)
				check("sync")
			case 5:
				if rng.Intn(3) == 0 {
					k := rng.Intn(len(mem))
					keep := []Key{}
					for i, m := range mem {
						if i != k {
							keep = append(keep, m.Key)
						}
					}
					mem = append(mem[:k:k], mem[k+1:]...)
					r.bs.Prune(keep)
					check("prune")
				}
			}
		}
	}
}
