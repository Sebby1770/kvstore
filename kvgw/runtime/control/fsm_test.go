package control

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

// applier feeds commands to an FSM at consecutive indices and checks the
// internal invariants after every one.
type applier struct {
	t   *testing.T
	f   *FSM
	idx uint64
}

func newApplier(t *testing.T) *applier { return &applier{t: t, f: NewFSM()} }

func (a *applier) do(cmd []byte) Result {
	a.t.Helper()
	a.idx++
	r := a.f.Apply(a.idx, cmd)
	a.f.mu.RLock()
	err := a.f.checkInvariants()
	a.f.mu.RUnlock()
	if err != nil {
		a.t.Fatalf("invariant broken after index %d (%s): %v", a.idx, cmd, err)
	}
	return r
}

func at(d time.Duration) time.Time { return t0.Add(d) }

func TestRingValidationAndEpochs(t *testing.T) {
	a := newApplier(t)
	for _, bad := range [][]byte{
		SetRingCmd(nil, 1, 1, 1, t0),
		SetRingCmd([]string{"a", "a"}, 1, 1, 1, t0),
		SetRingCmd([]string{"a", "b"}, 3, 1, 1, t0),
		SetRingCmd([]string{"a", "b"}, 2, 3, 1, t0),
		SetRingCmd([]string{"a", "b"}, 2, 1, 0, t0),
	} {
		if r := a.do(bad); !errors.Is(r.Err(), ErrInvalid) {
			t.Fatalf("%s: want invalid, got %+v", bad, r)
		}
	}
	r := a.do(SetRingCmd([]string{"a", "b", "c"}, 3, 2, 2, t0))
	if !r.OK() || r.Ring.Epoch != 1 || r.Ring.Index != a.idx {
		t.Fatalf("first ring: %+v", r)
	}
	if r := a.do(SetRingIfCmd([]string{"a", "b"}, 2, 1, 2, 0, t0)); !errors.Is(r.Err(), ErrConflict) || r.Ring.Epoch != 1 {
		t.Fatalf("stale epoch CAS should conflict and report the current ring: %+v", r)
	}
	if r := a.do(SetRingIfCmd([]string{"a", "b"}, 2, 1, 2, 1, t0)); !r.OK() || r.Ring.Epoch != 2 {
		t.Fatalf("matching CAS: %+v", r)
	}
	if got := a.f.Ring(); got.Epoch != 2 || len(got.Nodes) != 2 {
		t.Fatalf("Ring() = %+v", got)
	}
}

func TestLeaseLifecycleAndFencing(t *testing.T) {
	a := newApplier(t)
	g := a.do(AcquireLeaseCmd("lock", "alice", 10*time.Second, at(0)))
	l, err := g.LeaseGrant()
	if err != nil || l.Holder != "alice" || l.Token != a.idx {
		t.Fatalf("grant: %+v %v", g, err)
	}
	if r := a.do(AcquireLeaseCmd("lock", "bob", 10*time.Second, at(5*time.Second))); !errors.Is(r.Err(), ErrConflict) || r.Lease.Holder != "alice" {
		t.Fatalf("bob must conflict while alice holds it: %+v", r)
	}
	// Re-acquire by the holder extends and keeps the token.
	re := a.do(AcquireLeaseCmd("lock", "alice", 10*time.Second, at(6*time.Second)))
	if !re.OK() || !re.Existing || re.Lease.Token != l.Token || re.Lease.ExpiresAt != at(16*time.Second).UnixNano() {
		t.Fatalf("re-acquire: %+v", re)
	}
	if r := a.do(RenewLeaseCmd("lock", "bob", l.Token, 0, at(7*time.Second))); !errors.Is(r.Err(), ErrNotHolder) {
		t.Fatalf("renew by non-holder: %+v", r)
	}
	if r := a.do(RenewLeaseCmd("lock", "alice", l.Token-1, 0, at(7*time.Second))); !errors.Is(r.Err(), ErrStaleToken) {
		t.Fatalf("renew with stale token: %+v", r)
	}
	// Expiry is judged by the command's own time.
	if r := a.do(RenewLeaseCmd("lock", "alice", l.Token, 0, at(17*time.Second))); !errors.Is(r.Err(), ErrExpired) {
		t.Fatalf("renew after expiry: %+v", r)
	}
	b := a.do(AcquireLeaseCmd("lock", "bob", time.Second, at(17*time.Second)))
	if !b.OK() || b.Lease.Token <= l.Token {
		t.Fatalf("bob after expiry must get a larger token: %+v (alice had %d)", b, l.Token)
	}
	if r := a.do(ReleaseLeaseCmd("lock", "alice", l.Token, at(17*time.Second))); !errors.Is(r.Err(), ErrNotHolder) {
		t.Fatalf("old holder release: %+v", r)
	}
	if r := a.do(ReleaseLeaseCmd("lock", "bob", b.Lease.Token, at(17*time.Second))); !r.OK() {
		t.Fatalf("release: %+v", r)
	}
	if _, ok := a.f.Lease("lock"); ok {
		t.Fatal("released lease still present")
	}
}

// Model-based: random lease commands against a tiny reference model. Checks
// mutual exclusion (at most one unexpired holder) and that tokens of
// successive grants of a name strictly increase.
func TestLeaseModel(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 7))
			a := newApplier(t)
			type model struct {
				holder  string
				token   uint64
				expires int64
			}
			ms := map[string]*model{}
			lastToken := map[string]uint64{}
			now := t0
			names := []string{"x", "y"}
			holders := []string{"h1", "h2", "h3"}
			for step := 0; step < 400; step++ {
				now = now.Add(time.Duration(rng.IntN(3000)) * time.Millisecond)
				name, holder := names[rng.IntN(2)], holders[rng.IntN(3)]
				ttl := time.Duration(1+rng.IntN(5)) * time.Second
				m := ms[name]
				held := m != nil && now.UnixNano() < m.expires
				switch rng.IntN(3) {
				case 0:
					r := a.do(AcquireLeaseCmd(name, holder, ttl, now))
					switch {
					case held && m.holder != holder:
						if !errors.Is(r.Err(), ErrConflict) {
							t.Fatalf("step %d: acquire of held lease succeeded: %+v", step, r)
						}
					case held:
						if !r.OK() || r.Lease.Token != m.token {
							t.Fatalf("step %d: re-acquire changed token: %+v", step, r)
						}
						m.expires = now.Add(ttl).UnixNano()
					default:
						if !r.OK() {
							t.Fatalf("step %d: free lease not granted: %+v", step, r)
						}
						if r.Lease.Token <= lastToken[name] {
							t.Fatalf("step %d: token %d not above previous grant %d", step, r.Lease.Token, lastToken[name])
						}
						lastToken[name] = r.Lease.Token
						ms[name] = &model{holder: holder, token: r.Lease.Token, expires: now.Add(ttl).UnixNano()}
					}
				case 1:
					tok := uint64(0)
					if m != nil {
						tok = m.token
					}
					r := a.do(RenewLeaseCmd(name, holder, tok, ttl, now))
					if r.OK() != (held && m.holder == holder) {
						t.Fatalf("step %d: renew ok=%v held=%v: %+v", step, r.OK(), held, r)
					}
					if r.OK() {
						m.expires = now.Add(ttl).UnixNano()
					}
				case 2:
					tok := uint64(0)
					if m != nil {
						tok = m.token
					}
					r := a.do(ReleaseLeaseCmd(name, holder, tok, now))
					if r.OK() != (m != nil && m.holder == holder) {
						t.Fatalf("step %d: release ok=%v: %+v", step, r.OK(), r)
					}
					if r.OK() {
						delete(ms, name)
					}
				}
			}
		})
	}
}

func TestRegistryCAS(t *testing.T) {
	a := newApplier(t)
	if r := a.do(RegCASCmd("k", []byte("v1"), 0, t0)); !r.OK() || r.Entry.Version != a.idx {
		t.Fatalf("create: %+v", r)
	}
	v1 := a.idx
	if r := a.do(RegCASCmd("k", []byte("x"), 0, t0)); !errors.Is(r.Err(), ErrConflict) {
		t.Fatalf("create-if-absent on existing key: %+v", r)
	}
	if r := a.do(RegCASCmd("k", []byte("v2"), v1, t0)); !r.OK() || r.Entry.CreateIndex != v1 {
		t.Fatalf("CAS: %+v", r)
	}
	if r := a.do(RegDeleteIfCmd("k", v1, t0)); !errors.Is(r.Err(), ErrConflict) {
		t.Fatalf("stale delete: %+v", r)
	}
	if r := a.do(RegDeleteCmd("k", t0)); !r.OK() {
		t.Fatalf("delete: %+v", r)
	}
	if r := a.do(RegDeleteCmd("k", t0)); !errors.Is(r.Err(), ErrNotFound) {
		t.Fatalf("double delete: %+v", r)
	}
}

func TestJobLifecycle(t *testing.T) {
	a := newApplier(t)
	if r := a.do(EnqueueJobCmd("q", "j1", []byte("one"), 2, 0, at(0))); !r.OK() || r.Existing {
		t.Fatalf("enqueue: %+v", r)
	}
	if r := a.do(EnqueueJobCmd("q", "j1", []byte("dup"), 2, 0, at(0))); !r.Existing || string(r.Job.Payload) != "one" {
		t.Fatalf("enqueue must be idempotent by id: %+v", r)
	}
	a.do(EnqueueJobCmd("q", "j2", []byte("two"), 1, 0, at(0)))
	a.do(EnqueueJobCmd("q", "later", nil, 1, time.Hour, at(0)))

	c1 := a.do(ClaimJobCmd("q", "w1", 10*time.Second, at(time.Second)))
	j1, ok, err := c1.ClaimedJob()
	if err != nil || !ok || j1.ID != "j1" || j1.Attempts != 1 {
		t.Fatalf("FIFO claim: %+v", c1)
	}
	c2 := a.do(ClaimJobCmd("q", "w2", time.Minute, at(time.Second)))
	if c2.Job == nil || c2.Job.ID != "j2" {
		t.Fatalf("second claim: %+v", c2)
	}
	if r := a.do(ClaimJobCmd("q", "w3", 10*time.Second, at(2*time.Second))); r.Job != nil {
		t.Fatalf("nothing should be ready (delayed job, two claimed): %+v", r)
	}
	// w1 stalls past its visibility timeout; w3 reclaims; w1's token is dead.
	c3 := a.do(ClaimJobCmd("q", "w3", 10*time.Second, at(12*time.Second)))
	if c3.Job == nil || c3.Job.ID != "j1" || c3.Job.Token <= j1.Token || c3.Job.Attempts != 2 {
		t.Fatalf("reclaim after visibility timeout: %+v", c3)
	}
	if r := a.do(CompleteJobCmd("q", "j1", j1.Token, at(13*time.Second))); !errors.Is(r.Err(), ErrStaleToken) {
		t.Fatalf("stale worker completion must be rejected: %+v", r)
	}
	if r := a.do(CompleteJobCmd("q", "j1", c3.Job.Token, at(13*time.Second))); !r.OK() {
		t.Fatalf("complete: %+v", r)
	}
	if r := a.do(CompleteJobCmd("q", "j1", c3.Job.Token, at(14*time.Second))); !r.OK() || !r.Duplicate {
		t.Fatalf("repeated completion is a duplicate: %+v", r)
	}
	// j2 has one attempt: failing it dead-letters it.
	if r := a.do(FailJobCmd("q", "j2", c2.Job.Token, "boom", 0, at(15*time.Second))); !r.DeadLettered {
		t.Fatalf("fail on last attempt should dead-letter: %+v", r)
	}
	if dl := a.f.DeadLetters("q"); len(dl) != 1 || dl[0].LastError != "boom" {
		t.Fatalf("dead letters: %+v", dl)
	}
	if r := a.do(RedriveJobCmd("q", "j2", at(16*time.Second))); !r.OK() || r.Job.State != JobPending {
		t.Fatalf("redrive: %+v", r)
	}
	st := a.f.Queue("q")
	if st.Pending != 2 || st.Completed != 1 || st.Reclaims != 1 || st.DeadLettered != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestJobRetryBackoff(t *testing.T) {
	a := newApplier(t)
	a.do(EnqueueJobCmd("q", "j", nil, 3, 0, at(0)))
	c := a.do(ClaimJobCmd("q", "w", time.Minute, at(0)))
	if r := a.do(FailJobCmd("q", "j", c.Job.Token, "x", 0, at(time.Second))); !r.OK() || r.Job.State != JobPending {
		t.Fatalf("fail: %+v", r)
	}
	// Default backoff for the first failure is RetryBackoffBase.
	if r := a.do(ClaimJobCmd("q", "w", time.Minute, at(time.Second+RetryBackoffBase/2))); r.Job != nil {
		t.Fatalf("claimed during backoff: %+v", r)
	}
	if r := a.do(ClaimJobCmd("q", "w", time.Minute, at(time.Second+RetryBackoffBase))); r.Job == nil || r.Job.Attempts != 2 {
		t.Fatalf("not claimable after backoff: %+v", r)
	}
}

// A worker that keeps crashing mid-job must not loop the job forever: a lapsed
// claim on the final attempt dead-letters it.
func TestLapsedFinalAttemptDeadLetters(t *testing.T) {
	a := newApplier(t)
	a.do(EnqueueJobCmd("q", "j", nil, 1, 0, at(0)))
	a.do(ClaimJobCmd("q", "w", time.Second, at(0)))
	if r := a.do(ClaimJobCmd("q", "w2", time.Second, at(5*time.Second))); r.Job != nil {
		t.Fatalf("exhausted job re-claimed: %+v", r)
	}
	if st := a.f.Queue("q"); st.Dead != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

// randomCommand draws one plausible command; the mix exercises every op.
func randomCommand(rng *rand.Rand, now time.Time) []byte {
	name := func(p string, n int) string { return fmt.Sprintf("%s%d", p, rng.IntN(n)) }
	switch rng.IntN(13) {
	case 0:
		return SetRingCmd([]string{"a", "b", "c"}[:1+rng.IntN(3)], 1, 1, 1, now)
	case 1, 2:
		return AcquireLeaseCmd(name("l", 3), name("h", 3), time.Duration(1+rng.IntN(5))*time.Second, now)
	case 3:
		return RenewLeaseCmd(name("l", 3), name("h", 3), uint64(rng.IntN(200)), 0, now)
	case 4:
		return ReleaseLeaseCmd(name("l", 3), name("h", 3), uint64(rng.IntN(200)), now)
	case 5:
		return RegPutCmd(name("k", 5), []byte(name("v", 9)), now)
	case 6:
		return RegCASCmd(name("k", 5), []byte("c"), uint64(rng.IntN(100)), now)
	case 7:
		return RegDeleteCmd(name("k", 5), now)
	case 8, 9:
		return EnqueueJobCmd(name("q", 2), name("j", 20), []byte("p"), 1+rng.IntN(3), time.Duration(rng.IntN(3))*time.Second, now)
	case 10:
		return ClaimJobCmd(name("q", 2), name("w", 3), time.Duration(1+rng.IntN(4))*time.Second, now)
	case 11:
		return CompleteJobCmd(name("q", 2), name("j", 20), uint64(rng.IntN(200)), now)
	default:
		return FailJobCmd(name("q", 2), name("j", 20), uint64(rng.IntN(200)), "e", 0, now)
	}
}

// Determinism: replicas fed the same log end in byte-identical states, and a
// replica restored from a snapshot taken mid-log and then fed the rest ends in
// the same state as one that applied everything -- the property Raft log
// compaction relies on.
func TestDeterminismAndSnapshotEquivalence(t *testing.T) {
	for seed := uint64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		var log [][]byte
		now := t0
		for i := 0; i < 600; i++ {
			now = now.Add(time.Duration(rng.IntN(800)) * time.Millisecond)
			log = append(log, randomCommand(rng, now))
		}
		cut := 1 + rng.IntN(len(log)-1)

		full, other := newApplier(t), newApplier(t)
		var restored *FSM
		for i, c := range log {
			ra, rb := full.do(c), other.do(c)
			if !bytes.Equal(ra.Encode(), rb.Encode()) {
				t.Fatalf("seed %d entry %d: results differ\n%s\n%s", seed, i, ra.Encode(), rb.Encode())
			}
			if i+1 == cut {
				snap, err := full.f.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				restored = NewFSM()
				if err := restored.Restore(snap); err != nil {
					t.Fatalf("seed %d: restore: %v", seed, err)
				}
			}
		}
		for i := cut; i < len(log); i++ {
			restored.Apply(uint64(i+1), log[i])
		}
		sa, _ := full.f.Snapshot()
		sb, _ := other.f.Snapshot()
		sc, _ := restored.Snapshot()
		if !bytes.Equal(sa, sb) || !bytes.Equal(sa, sc) {
			t.Fatalf("seed %d: snapshots diverge (cut at %d)", seed, cut)
		}
	}
}

func TestApplyRejectsStaleIndexAndGarbage(t *testing.T) {
	f := NewFSM()
	if r := f.Apply(5, AcquireLeaseCmd("l", "h", time.Second, t0)); !r.OK() {
		t.Fatal(r)
	}
	if r := f.Apply(5, AcquireLeaseCmd("l", "x", time.Second, t0)); !errors.Is(r.Err(), ErrStaleIndex) {
		t.Fatalf("re-apply: %+v", r)
	}
	if r := f.Apply(6, []byte("{not json")); !errors.Is(r.Err(), ErrInvalid) || f.Applied() != 6 {
		t.Fatalf("garbage: %+v applied=%d", r, f.Applied())
	}
	if r := f.Apply(7, nil); !r.OK() || r.Op != OpNoop {
		t.Fatalf("empty entry is a no-op: %+v", r)
	}
	if err := f.Restore([]byte(`{"version":1,"applied":1,"leases":[{"name":"l","holder":"h","token":9,"ttl_ms":5}]}`)); err == nil {
		t.Fatal("restore accepted a token from the future")
	}
	if l, ok := f.Lease("l"); !ok || l.Holder != "h" {
		t.Fatal("failed restore modified state")
	}
}

func FuzzApply(f *testing.F) {
	f.Add(AcquireLeaseCmd("l", "h", time.Second, t0))
	f.Add(EnqueueJobCmd("q", "j", []byte("x"), 2, 0, t0))
	f.Add(SetRingCmd([]string{"a"}, 1, 1, 1, t0))
	f.Add([]byte(`{"op":"job_claim","now":1,"queue":"q","worker":"w","visibility_ms":-1}`))
	f.Fuzz(func(t *testing.T, cmd []byte) {
		fsm := NewFSM()
		fsm.Apply(1, EnqueueJobCmd("q", "j", nil, 1, 0, t0))
		fsm.Apply(2, cmd)
		fsm.Apply(3, cmd)
		fsm.mu.RLock()
		err := fsm.checkInvariants()
		fsm.mu.RUnlock()
		if err != nil {
			t.Fatalf("invariants broken by %q: %v", cmd, err)
		}
		snap, err := fsm.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if err := NewFSM().Restore(snap); err != nil {
			t.Fatalf("own snapshot does not restore: %v", err)
		}
	})
}
