package cluster

import (
	"fmt"
	"math/rand"
	"testing"
)

// A chaos test: drive reads and writes while a malicious scheduler randomly
// fails/heals nodes AND grows/shrinks the cluster at runtime — then asserts no
// committed write was ever lost. It exercises fault tolerance, live membership,
// and anti-entropy together.
//
// The scheduler keeps the cluster in a recoverable regime (it heals everyone and
// runs a repair sweep before each membership change, and never lets more than
// one node be down at a time), which is exactly the contract under which the
// store promises not to lose data. Within that regime, every committed value
// must stay readable across all the churn.
func TestChaosKeepsData(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runChaos(t, seed)
		})
	}
}

func runChaos(t *testing.T, seed int64) {
	const (
		rf    = 3
		rq    = 2
		wq    = 2
		minN  = 4
		maxN  = 7
		steps = 400
	)
	rng := rand.New(rand.NewSource(seed))

	c, fakes := build(5, rf, rq, wq)
	addrs := []string{}
	byAddr := map[string]*fakeNode{}
	for _, f := range fakes {
		addrs = append(addrs, f.addr)
		byAddr[f.addr] = f
	}
	nextID := len(addrs)
	down := "" // at most one node down at a time
	keys := []string{"a", "b", "c", "d", "e", "f"}
	model := map[string]string{} // committed value per key ("" = absent)

	heal := func() {
		if down != "" {
			byAddr[down].setDown(false)
			down = ""
		}
	}

	for step := 0; step < steps; step++ {
		switch roll := rng.Intn(100); {
		case roll < 60: // write
			k := keys[rng.Intn(len(keys))]
			v := fmt.Sprintf("v%d", step)
			if _, err := c.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("seed %d step %d: put %q failed: %v (down=%s, nodes=%v)",
					seed, step, k, err, down, addrs)
			}
			model[k] = v

		case roll < 85: // read
			k := keys[rng.Intn(len(keys))]
			val, found, _, err := c.Get([]byte(k))
			if err != nil {
				t.Fatalf("seed %d step %d: get %q failed: %v", seed, step, k, err)
			}
			want, has := model[k]
			if found != has || (found && string(val) != want) {
				t.Fatalf("seed %d step %d: get %q = (%q,%v), want (%q,%v)",
					seed, step, k, val, found, want, has)
			}

		default: // chaos — stabilise, then perturb membership/availability
			heal()
			c.RepairSweep() // every key now fully RF-replicated

			switch action := rng.Intn(3); {
			case action == 0 && len(addrs) < maxN: // a node joins
				addr := fmt.Sprintf("n%d", nextID)
				nextID++
				f := newFake(addr)
				if !c.AddNode(addr, f) {
					t.Fatalf("seed %d: AddNode(%s) failed", seed, addr)
				}
				addrs = append(addrs, addr)
				byAddr[addr] = f

			case action == 1 && len(addrs) > minN: // a node leaves
				victim := addrs[rng.Intn(len(addrs))]
				if !c.RemoveNode(victim) {
					t.Fatalf("seed %d: RemoveNode(%s) failed", seed, victim)
				}
				out := addrs[:0]
				for _, a := range addrs {
					if a != victim {
						out = append(out, a)
					}
				}
				addrs = out
				delete(byAddr, victim)

			default: // a node fails (heals at the next chaos step)
				victim := addrs[rng.Intn(len(addrs))]
				byAddr[victim].setDown(true)
				down = victim
			}
		}
	}

	// Settle and verify every committed value survived the mayhem.
	heal()
	c.RepairSweep()
	for _, k := range keys {
		val, found, _, err := c.Get([]byte(k))
		if err != nil {
			t.Fatalf("seed %d: final get %q failed: %v", seed, k, err)
		}
		want, has := model[k]
		if found != has || (found && string(val) != want) {
			t.Fatalf("seed %d: DATA LOST — final get %q = (%q,%v), want (%q,%v)",
				seed, k, val, found, want, has)
		}
	}
}
