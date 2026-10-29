package node

import (
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/ring"
	"github.com/Sebby1770/kvstore/kvgw/runtime/actor"
	"github.com/Sebby1770/kvstore/kvgw/runtime/swim"
)

// memberView is the node's picture of the live mesh, fed by SWIM. It is also
// the actor Placement: rendezvous hashing over alive members, the same
// algorithm the KV ring uses, so an actor moves only when its owner leaves
// or a new member out-scores it.
type memberView struct {
	self string
	mu   sync.RWMutex
	all  map[string]swim.Member
	ring *ring.Ring
}

func newMemberView(self string) *memberView {
	return &memberView{self: self, all: map[string]swim.Member{}, ring: ring.New([]string{self})}
}

func (v *memberView) set(ms []swim.Member) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.all = map[string]swim.Member{}
	var live []string
	for _, m := range ms {
		v.all[m.Name] = m
		// Suspects are excluded from placement. SWIM keeps a suspect in the
		// membership until the suspicion times out (seconds), but an actor
		// whose owner is probably dead should fail over now; if the
		// suspicion was false, the CAS fence makes the brief double
		// activation harmless and the actor moves back.
		if m.State == swim.StateAlive {
			live = append(live, m.Name)
		}
	}
	sort.Strings(live)
	v.ring = ring.New(live)
}

func (v *memberView) get(name string) (swim.Member, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	m, ok := v.all[name]
	return m, ok
}

func (v *memberView) alive() []swim.Member {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var out []swim.Member
	for _, m := range v.all {
		if m.State == swim.StateAlive {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Owner implements actor.Placement.
func (v *memberView) Owner(id actor.ID) (string, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	o := v.ring.Pick([]byte(id.String()))
	return o, o != ""
}

// swimDriver runs the sans-IO SWIM node: one goroutine owns it, fed by
// Raw datagrams from the endpoint and by its own deadline timer.
type swimDriver struct {
	n    *Node
	mu   sync.Mutex
	sw   *swim.Node
	wake chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func newSwimDriver(n *Node) *swimDriver {
	cfg := swim.Config{Name: n.cfg.Name, Addr: n.cfg.Advertise, Meta: n.swimMeta(),
		ProbeInterval: n.cfg.ProbeInterval, ProbeTimeout: n.cfg.ProbeInterval / 2,
		GossipInterval: n.cfg.ProbeInterval / 5, PushPullInterval: 30 * n.cfg.ProbeInterval,
		MaxMessageSize: 1100, Rand: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))}
	d := &swimDriver{n: n, sw: swim.New(cfg, time.Now()), wake: make(chan struct{}, 1), done: make(chan struct{})}
	n.members.set(d.sw.Members())
	return d
}

func (n *Node) swimMeta() map[string]string {
	return map[string]string{"http": n.cfg.HTTPAdvertise, "identity": Fingerprint(n.identity.Public().(ed25519PublicKey))}
}

func (d *swimDriver) start() {
	d.mu.Lock()
	d.sw.Join(d.n.cfg.Join, time.Now())
	n := d.n
	n.mu.Lock()
	var boot []string
	for name, addr := range n.peers {
		if name != n.cfg.Name {
			boot = append(boot, addr)
		}
	}
	n.mu.Unlock()
	sort.Strings(boot)
	d.sw.Join(boot, time.Now())
	d.flushLocked()
	d.mu.Unlock()
	d.wg.Add(2)
	go d.recvLoop()
	go d.timerLoop()
}

func (d *swimDriver) kick() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *swimDriver) recvLoop() {
	defer d.wg.Done()
	for {
		p, err := d.n.ep.RecvRaw(d.n.ctx)
		if err != nil {
			return
		}
		d.mu.Lock()
		d.sw.Handle(p.From, p.Payload, time.Now())
		d.flushLocked()
		d.mu.Unlock()
	}
}

func (d *swimDriver) timerLoop() {
	defer d.wg.Done()
	t := time.NewTimer(time.Second)
	defer t.Stop()
	for {
		d.mu.Lock()
		wait := time.Until(d.sw.NextDeadline())
		d.mu.Unlock()
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(max(wait, 0))
		select {
		case <-d.done:
			return
		case <-d.n.ctx.Done():
			return
		case <-d.wake:
		case <-t.C:
			d.mu.Lock()
			d.sw.Tick(time.Now())
			d.flushLocked()
			d.mu.Unlock()
		}
	}
}

// flushLocked sends SWIM's outbox and processes its events.
func (d *swimDriver) flushLocked() {
	for _, m := range d.sw.Outbox() {
		d.n.ep.SendRaw(m.To, m.Payload)
	}
	evs := d.sw.Events()
	if len(evs) == 0 {
		return
	}
	d.n.members.set(d.sw.Members())
	for _, e := range evs {
		d.n.events.add("swim."+e.Kind.String(), e.Member.Name+" ("+e.Member.Addr+")")
	}
	// Placement changed: hand actors this node no longer owns to their new
	// owners (asynchronously: deactivation persists state).
	go d.n.actors.Rebalance()
	d.kick()
}

func (d *swimDriver) updateMeta() {
	d.mu.Lock()
	d.sw.UpdateMeta(d.n.swimMeta(), time.Now())
	d.flushLocked()
	d.mu.Unlock()
}

func (d *swimDriver) members() []swim.Member {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sw.Members()
}

func (d *swimDriver) localHealth() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sw.LocalHealth()
}

func (d *swimDriver) leave() {
	d.mu.Lock()
	d.sw.Leave(time.Now())
	d.flushLocked()
	d.mu.Unlock()
}

func (d *swimDriver) stop() {
	d.once.Do(func() { close(d.done) })
	d.wg.Wait()
}
