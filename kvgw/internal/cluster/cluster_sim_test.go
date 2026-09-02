package cluster

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// A deterministic simulation: drive the real coordinator through thousands of
// randomized put/get/del operations while continuously failing a node, and
// check every successful read against a single-source-of-truth model. This is
// the "prove it, don't just demo it" layer — a tiny linearizability checker.
//
// Fault model: at most one node is down at any step. With RF=3 and R=W=2, every
// replica set still has a quorum, so every operation must succeed and every read
// must return exactly the last committed write. (A stronger fault model would
// have to reason about indeterminate failed writes; capping at one failure keeps
// the invariant exact while still exercising failover + read-repair on every
// single step.)

type modelVal struct {
	present bool
	value   string
}

func TestSimulationStaysLinearizable(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSim(t, seed)
		})
	}
}

func runSim(t *testing.T, seed int64) {
	const (
		n     = 5
		rf    = 3
		r     = 2
		w     = 2
		steps = 250
	)
	rng := rand.New(rand.NewSource(seed))
	c, fakes := build(n, rf, r, w)
	byAddr := map[string]*fakeNode{}
	for _, f := range fakes {
		byAddr[f.addr] = f
	}
	allUp := func() {
		for _, f := range fakes {
			f.setDown(false)
		}
	}

	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5"}
	model := map[string]modelVal{}

	for step := 0; step < steps; step++ {
		// Churn: at most one node down, so every quorum is still reachable.
		allUp()
		if rng.Intn(3) != 0 {
			fakes[rng.Intn(n)].setDown(true)
		}

		key := keys[rng.Intn(len(keys))]
		switch roll := rng.Intn(10); {
		case roll < 6: // put
			val := fmt.Sprintf("v%d", step)
			if _, err := c.Put([]byte(key), []byte(val)); err != nil {
				t.Fatalf("seed %d step %d: put %q failed with a single node down: %v", seed, step, key, err)
			}
			model[key] = modelVal{present: true, value: val}

		case roll < 8: // get
			got, found, meta, err := c.Get([]byte(key))
			if err != nil {
				t.Fatalf("seed %d step %d: get %q failed: %v", seed, step, key, err)
			}
			m := model[key]
			if found != m.present {
				t.Fatalf("seed %d step %d: get %q found=%v want present=%v (v%d)",
					seed, step, key, found, m.present, meta.Version)
			}
			if found && string(got) != m.value {
				t.Fatalf("seed %d step %d: stale read of %q: got %q want %q",
					seed, step, key, got, m.value)
			}

		default: // del
			if _, _, err := c.Del([]byte(key)); err != nil {
				t.Fatalf("seed %d step %d: del %q failed: %v", seed, step, key, err)
			}
			model[key] = modelVal{present: false}
		}
	}

	// Bring everyone back and read each key once to drive read-repair, then
	// assert the whole replica set has converged to the committed state.
	allUp()
	for _, k := range keys {
		if _, _, _, err := c.Get([]byte(k)); err != nil {
			t.Fatalf("seed %d: convergence read of %q failed: %v", seed, k, err)
		}
	}
	for _, k := range keys {
		m := model[k]
		for _, addr := range c.Route([]byte(k)) {
			res, err := byAddr[addr].Get([]byte(k))
			if err != nil {
				t.Fatalf("seed %d: convergence get on %s failed: %v", seed, addr, err)
			}
			if m.present {
				if res.State != kv.Present || string(res.Value) != m.value {
					t.Fatalf("seed %d: replica %s for %q did not converge: state=%v value=%q want present %q",
						seed, addr, k, res.State, res.Value, m.value)
				}
			} else if res.State == kv.Present {
				t.Fatalf("seed %d: replica %s still holds deleted key %q as present (%q)",
					seed, addr, k, res.Value)
			}
		}
	}
}
