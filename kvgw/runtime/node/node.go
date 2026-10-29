// Package node is the KVStore mesh daemon assembled from the runtime and
// networking stack: one process, one UDP port, and every layer wired
// together.
//
//	              HTTP (KV API, /mesh, /lease, /jobs, /actor)
//	                              │
//	KV coordinator ── ring from ──┤── control plane (Raft FSM: ring, leases, jobs, registry)
//	actor host ── placement from ─┤── SWIM membership (gossip over KTP Raw datagrams)
//	                              │
//	       rpc over KTP streams: raft.step · actor.deliver · control.join
//	                              │
//	                     endpoint (one UDP socket)
//
// Every node is a full peer: any node serves any request. The storage ring
// comes from the control plane, so all coordinators serve the same ring and
// quorum overlap holds across them -- the property 1.x lost as soon as two
// gateways disagreed about membership.
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/client"
	"github.com/Sebby1770/kvstore/kvgw/internal/cluster"
	"github.com/Sebby1770/kvstore/kvgw/internal/httpapi"
	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
	"github.com/Sebby1770/kvstore/kvgw/runtime/actor"
	"github.com/Sebby1770/kvstore/kvgw/runtime/control"
	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
	"github.com/Sebby1770/kvstore/kvgw/runtime/swim"
)

// Config configures a node.
type Config struct {
	Name          string // unique node name (also its Raft ID)
	Listen        string // UDP address for KTP + SWIM
	Advertise     string // address peers dial (default Listen)
	HTTP          string // HTTP listen address ("" = no HTTP server; use Handler)
	HTTPAdvertise string // HTTP address shown to peers (default HTTP)
	DataDir       string // identity key + Raft WAL ("" = ephemeral, in memory)

	ClusterKey []byte
	Insecure   bool   // KTP without AEAD (debugging only)
	Congestion string // KTP congestion controller: bbr, cubic, newreno [cubic]

	// Bootstrap lists the founding control-plane voters as "name=addr".
	// Every founding node must be started with the same list. Nodes not in
	// it join as learners and are promoted up to MaxVoters.
	Bootstrap []string
	// Join lists addresses of existing members to gossip with first.
	Join []string
	// MaxVoters caps automatic promotion of joining nodes [5].
	MaxVoters int

	// KVNodes is the storage ring installed if the control plane has none.
	KVNodes     []string
	RF, R, W    int
	Heartbeat   time.Duration // kvnode heartbeat [2s]
	AntiEntropy time.Duration // repair sweep (0 = off)
	KVTimeout   time.Duration // per kvnode call [2s]

	// Timing (tests shrink these).
	TickInterval  time.Duration // Raft tick [100ms]
	ProbeInterval time.Duration // SWIM [1s]

	Logf func(format string, args ...any)

	// Test hooks: a PacketConn instead of a UDP socket, its resolver, and a
	// factory for storage-node clients.
	PacketConn net.PacketConn
	Resolve    func(addr string) (net.Addr, error)
	KVNode     func(addr string) kv.Node
}

// Node is a running mesh node.
type Node struct {
	cfg      Config
	identity ed25519.PrivateKey
	clock    *hlc.Clock
	events   *eventLog

	ep      *endpoint.Endpoint
	rpcSrv  *rpc.Server
	rpcCli  *rpc.Client
	ctl     *control.Service
	raftTx  *raftTransport
	kv      *cluster.Cluster
	api     *httpapi.API
	actors  *actor.Host
	members *memberView
	swim    *swimDriver
	http    *http.Server

	mu      sync.Mutex
	kvNodes map[string]kv.Node // reused clients across ring epochs
	peers   map[string]string  // raft ID -> address (bootstrap + gossip)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (c *Config) defaults() error {
	if c.Name == "" {
		return errors.New("node: Name is required")
	}
	if c.Listen == "" && c.PacketConn == nil {
		return errors.New("node: Listen (or PacketConn) is required")
	}
	if c.Advertise == "" {
		if c.PacketConn != nil {
			c.Advertise = c.PacketConn.LocalAddr().String()
		} else {
			c.Advertise = c.Listen
		}
	}
	if c.HTTPAdvertise == "" {
		c.HTTPAdvertise = c.HTTP
	}
	if c.Congestion == "" {
		c.Congestion = "cubic"
	}
	if c.MaxVoters <= 0 {
		c.MaxVoters = 5
	}
	if c.RF <= 0 {
		c.RF = 3
	}
	if c.R <= 0 {
		c.R = 2
	}
	if c.W <= 0 {
		c.W = 2
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = 2 * time.Second
	}
	if c.KVTimeout <= 0 {
		c.KVTimeout = 2 * time.Second
	}
	if c.TickInterval <= 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = time.Second
	}
	if c.KVNode == nil {
		timeout := c.KVTimeout
		c.KVNode = func(addr string) kv.Node { return client.New(addr, timeout) }
	}
	return nil
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logf != nil {
		n.cfg.Logf("["+n.cfg.Name+"] "+format, args...)
	}
}

// parseBootstrap splits "name=addr" pairs.
func parseBootstrap(list []string) (names []string, addrs map[string]string, err error) {
	addrs = map[string]string{}
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, addr, ok := strings.Cut(item, "=")
		if !ok || name == "" || addr == "" {
			return nil, nil, fmt.Errorf("node: bootstrap entry %q is not name=addr", item)
		}
		names = append(names, name)
		addrs[name] = addr
	}
	sort.Strings(names)
	return names, addrs, nil
}

// Start brings the node up.
func Start(cfg Config) (*Node, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	bootNames, bootAddrs, err := parseBootstrap(cfg.Bootstrap)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, kvNodes: map[string]kv.Node{}, peers: bootAddrs}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.clock = hlc.New(time.Now, 500*time.Millisecond)
	n.events = newEventLog(n.clock, 256)

	// Identity.
	if cfg.DataDir != "" {
		n.identity, err = loadOrCreateIdentity(filepath.Join(cfg.DataDir, "identity.key"))
	} else {
		_, n.identity, err = ed25519.GenerateKey(rand.Reader)
	}
	if err != nil {
		return nil, err
	}

	// Transport.
	kcfg := &ktp.Config{Identity: n.identity, ClusterKey: cfg.ClusterKey, Insecure: cfg.Insecure, Congestion: cfg.Congestion,
		KeepAlive: 5 * time.Second, IdleTimeout: 30 * time.Second, HandshakeTimeout: 3 * time.Second}
	eopts := endpoint.Options{Resolve: cfg.Resolve, Logf: n.logf, AcceptDialed: true}
	if cfg.PacketConn != nil {
		n.ep, err = endpoint.New(cfg.PacketConn, kcfg, eopts)
	} else {
		n.ep, err = endpoint.Listen(cfg.Listen, kcfg, eopts)
		if err == nil && strings.HasSuffix(n.cfg.Advertise, ":0") {
			n.cfg.Advertise = n.ep.LocalAddr().String()
		}
	}
	if err != nil {
		return nil, err
	}

	// RPC, with hybrid-logical-clock propagation in metadata.
	n.rpcSrv = rpc.NewServer()
	n.rpcSrv.Use(func(ctx context.Context, method string, req []byte, next rpc.Handler) ([]byte, error) {
		if ts, err := hlc.Parse(rpc.Metadata(ctx)["hlc"]); err == nil {
			n.clock.Update(ts)
		}
		return next(ctx, req)
	})
	n.rpcCli = rpc.NewClient(n.ep, rpc.ClientOptions{
		Metadata: func(context.Context) map[string]string { return map[string]string{"hlc": n.clock.Now().String()} },
	})

	// Control plane.
	n.raftTx = newRaftTransport(n)
	var peers []string
	for _, name := range bootNames {
		if name == cfg.Name {
			peers = bootNames
		}
	}
	var raftDir string
	if cfg.DataDir != "" {
		raftDir = filepath.Join(cfg.DataDir, "raft")
	}
	n.ctl, err = control.NewService(control.ServiceConfig{ID: cfg.Name, Peers: peers, DataDir: raftDir,
		Transport: n.raftTx, TickInterval: cfg.TickInterval, Logf: n.logf})
	if err != nil {
		n.ep.Close()
		return nil, err
	}

	// KV coordinator: the ring arrives from the control plane.
	n.kv = cluster.New(map[string]kv.Node{}, nil, cfg.RF, cfg.R, cfg.W)
	n.api = httpapi.New(n.kv, httpapi.Options{Timeout: cfg.KVTimeout, Membership: &ringMembership{n: n},
		TagPrefix: cfg.Name, Brand: "KVStore mesh node " + cfg.Name})
	n.ctl.OnRing(n.adoptRing)
	n.ctl.OnConf(func(cs raftConf) {
		n.events.add("raft.conf", fmt.Sprintf("voters=%v learners=%v", cs.Voters, cs.Learners))
	})

	// Membership and actors.
	n.members = newMemberView(cfg.Name)
	n.actors = actor.NewHost(actor.HostConfig{Node: cfg.Name, Placement: n.members,
		Transport: &actorTransport{n: n}, Store: &registryStore{n: n}, IdleTimeout: 2 * time.Minute})
	registerBuiltinActors(n.actors)
	n.swim = newSwimDriver(n)

	n.registerRPC()
	n.routes()

	n.ctl.Start()
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.rpcSrv.Serve(n.ctx, n.ep) }()
	n.swim.start()
	n.wg.Add(2)
	go n.housekeeping()
	go n.kvLoops()

	if cfg.HTTP != "" {
		ln, err := net.Listen("tcp", cfg.HTTP)
		if err != nil {
			n.Close()
			return nil, err
		}
		if n.cfg.HTTPAdvertise == "" || strings.HasSuffix(n.cfg.HTTPAdvertise, ":0") {
			n.cfg.HTTPAdvertise = ln.Addr().String()
			n.swim.updateMeta()
		}
		n.http = &http.Server{Handler: n.api.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go n.http.Serve(ln)
	}
	n.events.add("node.start", fmt.Sprintf("%s at %s (identity %s)", cfg.Name, n.cfg.Advertise, Fingerprint(n.identity.Public().(ed25519.PublicKey))))
	n.logf("started at %s (raft %s, %d bootstrap voters)", n.cfg.Advertise, map[bool]string{true: "founder", false: "joiner"}[len(peers) > 0], len(bootNames))
	return n, nil
}

// Handler serves the node's HTTP API (also when Config.HTTP is empty).
func (n *Node) Handler() http.Handler { return n.api.Handler() }

// Name is the node's name.
func (n *Node) Name() string { return n.cfg.Name }

// Addr is the node's advertised mesh address.
func (n *Node) Addr() string { return n.cfg.Advertise }

// HTTPAddr is the node's HTTP address.
func (n *Node) HTTPAddr() string { return n.cfg.HTTPAdvertise }

// Control exposes the control-plane replica.
func (n *Node) Control() *control.Service { return n.ctl }

// Actors exposes the actor host.
func (n *Node) Actors() *actor.Host { return n.actors }

// KV exposes the quorum coordinator.
func (n *Node) KV() *cluster.Cluster { return n.kv }

// Close leaves the cluster gracefully and shuts every layer down.
func (n *Node) Close() error {
	n.swim.leave()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n.actors.Close(ctx)
	if n.http != nil {
		n.http.Shutdown(ctx)
	}
	n.cancel()
	n.swim.stop()
	n.ctl.Stop()
	n.raftTx.close()
	err := n.ep.Close()
	n.wg.Wait()
	return err
}

// Crash stops the node abruptly, without telling anyone (tests and demos).
func (n *Node) Crash() {
	n.cancel()
	// Activations die with the process; their state is whatever they last
	// persisted (the control plane is already stopping, so nothing new is
	// written).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n.ctl.Stop()
	n.actors.Close(ctx)
	n.swim.stop()
	n.raftTx.close()
	n.ep.Close()
	n.wg.Wait()
}

// peerAddr resolves a Raft ID to a mesh address.
func (n *Node) peerAddr(id string) (string, bool) {
	if m, ok := n.members.get(id); ok {
		return m.Addr, true
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	a, ok := n.peers[id]
	return a, ok
}

// adoptRing installs a ring committed by the control plane.
func (n *Node) adoptRing(r control.RingConfig) {
	n.mu.Lock()
	nodes := make(map[string]kv.Node, len(r.Nodes))
	for _, addr := range r.Nodes {
		c, ok := n.kvNodes[addr]
		if !ok {
			c = n.cfg.KVNode(addr)
			n.kvNodes[addr] = c
		}
		nodes[addr] = c
	}
	n.mu.Unlock()
	if n.kv.Reconfigure(r.Epoch, r.Nodes, nodes, r.RF, r.R, r.W) {
		n.events.add("ring.epoch", fmt.Sprintf("epoch %d: %d storage nodes, RF=%d R=%d W=%d", r.Epoch, len(r.Nodes), r.RF, r.R, r.W))
	}
}

// housekeeping installs the initial ring, promotes joiners, and records
// leadership changes.
func (n *Node) housekeeping() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.TickInterval * 5)
	defer t.Stop()
	var lastLead string
	var lastTerm uint64
	joined := false
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		st := n.ctl.Status()
		if st.Lead != lastLead || st.Term != lastTerm {
			if st.Lead != "" {
				n.events.add("raft.leader", fmt.Sprintf("%s leads term %d", st.Lead, st.Term))
			}
			lastLead, lastTerm = st.Lead, st.Term
		}
		if n.ctl.IsLeader() {
			n.installInitialRing()
			n.promoteLearners()
		}
		if !joined && !n.inConf(st) {
			joined = n.requestJoin()
		} else if n.inConf(st) {
			joined = true
		}
	}
}

func (n *Node) inConf(st raftStatus) bool {
	for _, v := range append(append([]string{}, st.Voters...), st.Learners...) {
		if v == n.cfg.Name {
			return true
		}
	}
	return false
}

func (n *Node) installInitialRing() {
	if len(n.cfg.KVNodes) == 0 || n.ctl.FSM().Ring().Epoch > 0 {
		return
	}
	rf := min(n.cfg.RF, len(n.cfg.KVNodes))
	r, w := min(n.cfg.R, rf), min(n.cfg.W, rf)
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	if _, err := n.ctl.Apply(ctx, control.SetRingIfCmd(n.cfg.KVNodes, rf, r, w, 0, n.ctl.Now())); err == nil {
		n.logf("installed storage ring %v", n.cfg.KVNodes)
	}
}

// promoteLearners turns caught-up learners into voters, up to MaxVoters.
func (n *Node) promoteLearners() {
	st := n.ctl.Status()
	if len(st.Voters) >= n.cfg.MaxVoters || len(st.Learners) == 0 {
		return
	}
	for _, l := range st.Learners {
		p, ok := st.Progress[l]
		if !ok || st.Commit == 0 || p.Match+64 < st.Commit {
			continue // not caught up yet
		}
		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		err := n.ctl.AddVoter(ctx, l)
		cancel()
		if err == nil {
			n.events.add("raft.promote", l+" promoted to voter")
		}
		return // one change at a time
	}
}

// requestJoin asks the cluster to add this node as a learner.
func (n *Node) requestJoin() bool {
	var targets []string
	for _, m := range n.members.alive() {
		if m.Name != n.cfg.Name {
			targets = append(targets, m.Addr)
		}
	}
	targets = append(targets, n.cfg.Join...)
	for _, addr := range targets {
		ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
		_, err := n.rpcCli.Call(ctx, addr, "control.join", []byte(n.cfg.Name+"="+n.cfg.Advertise))
		cancel()
		if err == nil {
			n.events.add("raft.join", "added as a learner via "+addr)
			return true
		}
	}
	return false
}

func (n *Node) kvLoops() {
	defer n.wg.Done()
	hb := time.NewTicker(max(n.cfg.Heartbeat, 10*time.Millisecond))
	defer hb.Stop()
	var ae <-chan time.Time
	if n.cfg.AntiEntropy > 0 {
		t := time.NewTicker(n.cfg.AntiEntropy)
		defer t.Stop()
		ae = t.C
	}
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-hb.C:
			if n.cfg.Heartbeat > 0 && len(n.kv.Addrs()) > 0 {
				n.kv.Heartbeat()
			}
		case <-ae:
			if len(n.kv.Addrs()) > 0 {
				n.kv.RepairSweep()
			}
		}
	}
}

// ringMembership routes /admin/nodes changes through the control plane, so
// every coordinator adopts them.
type ringMembership struct{ n *Node }

func (m *ringMembership) change(add, remove string) error {
	n := m.n
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		if err := n.ctl.ReadBarrier(ctx); err != nil {
			return err
		}
		cur := n.ctl.FSM().Ring()
		var nodes []string
		found := false
		for _, a := range cur.Nodes {
			if a == remove {
				found = true
				continue
			}
			if a == add {
				return httpapi.ErrMembershipConflict
			}
			nodes = append(nodes, a)
		}
		if remove != "" && (!found || len(nodes) == 0) {
			return httpapi.ErrMembershipConflict
		}
		if add != "" {
			nodes = append(nodes, add)
		}
		rf := cur.RF
		if rf == 0 {
			rf = n.cfg.RF
		}
		rf = min(max(rf, 1), len(nodes))
		r := min(max(cur.R, 1), rf)
		w := min(max(cur.W, 1), rf)
		if cur.Epoch == 0 {
			r, w = min(n.cfg.R, rf), min(n.cfg.W, rf)
		}
		res, err := n.ctl.Propose(ctx, control.SetRingIfCmd(nodes, rf, r, w, cur.Epoch, n.ctl.Now()))
		if err != nil {
			return err
		}
		if res.OK() {
			return nil
		}
		if !errors.Is(res.Err(), control.ErrConflict) {
			return res.Err()
		}
	}
	return errors.New("node: ring changed concurrently too many times")
}

func (m *ringMembership) AddNode(addr string) error    { return m.change(addr, "") }
func (m *ringMembership) RemoveNode(addr string) error { return m.change("", addr) }

// Members returns the SWIM view.
func (n *Node) Members() []swim.Member { return n.swim.members() }
