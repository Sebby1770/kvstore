package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
	"github.com/Sebby1770/kvstore/kvgw/runtime/swim"
)

// memKV is an in-memory storage node with the C engine's LWW semantics.
type memKV struct {
	addr string
	mu   sync.Mutex
	data map[string]kv.ReadResult
}

func (m *memKV) Addr() string { return m.addr }
func (m *memKV) Ping() error  { return nil }
func (m *memKV) Get(key []byte) (kv.ReadResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.data[string(key)]; ok {
		return r, nil
	}
	return kv.ReadResult{State: kv.Missing}, nil
}
func (m *memKV) Put(key, val []byte, v uint64) error { return m.PutX(key, val, v, 0) }
func (m *memKV) PutX(key, val []byte, v, exp uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.data[string(key)]; ok && cur.Version > v {
		return nil
	}
	m.data[string(key)] = kv.ReadResult{State: kv.Present, Value: append([]byte(nil), val...), Version: v, ExpiresAt: exp}
	return nil
}
func (m *memKV) Del(key []byte, v uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.data[string(key)]
	if ok && cur.Version > v {
		return cur.State == kv.Present, nil
	}
	m.data[string(key)] = kv.ReadResult{State: kv.Deleted, Version: v}
	return ok && cur.State == kv.Present, nil
}
func (m *memKV) scan(all bool) []kv.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []kv.Entry
	for k, r := range m.data {
		if r.State == kv.Present || all {
			out = append(out, kv.Entry{Key: []byte(k), Value: r.Value, Version: r.Version, ExpiresAt: r.ExpiresAt, Tombstone: r.State == kv.Deleted})
		}
	}
	return out
}
func (m *memKV) Scan() ([]kv.Entry, error)    { return m.scan(false), nil }
func (m *memKV) ScanAll() ([]kv.Entry, error) { return m.scan(true), nil }
func (m *memKV) Stat() (kv.Stat, error) {
	return kv.Stat{Capacity: 1024, Count: uint32(len(m.scan(false)))}, nil
}
func (m *memKV) Compact() error { return nil }

type mesh struct {
	t     *testing.T
	net   *simnet.Network
	kv    map[string]*memKV
	nodes map[string]*Node
	boot  []string
}

func newMesh(t *testing.T, n int) *mesh {
	m := &mesh{t: t, net: simnet.New(1), kv: map[string]*memKV{}, nodes: map[string]*Node{}}
	m.net.SetDefaultLink(simnet.Link{Latency: 2 * time.Millisecond, Jitter: time.Millisecond})
	for i := 1; i <= 3; i++ {
		a := fmt.Sprintf("kv%d", i)
		m.kv[a] = &memKV{addr: a, data: map[string]kv.ReadResult{}}
	}
	for i := 1; i <= n; i++ {
		m.boot = append(m.boot, fmt.Sprintf("n%d=n%d:7000", i, i))
	}
	for i := 1; i <= n; i++ {
		m.start(fmt.Sprintf("n%d", i), m.boot, nil)
	}
	return m
}

func (m *mesh) start(name string, boot, join []string) *Node {
	m.t.Helper()
	pc, err := m.net.Listen(name + ":7000")
	if err != nil {
		m.t.Fatal(err)
	}
	nd, err := Start(Config{Name: name, PacketConn: pc, Resolve: simnet.Resolve, ClusterKey: []byte("mesh-test"),
		Bootstrap: boot, Join: join, KVNodes: []string{"kv1", "kv2", "kv3"}, RF: 3, R: 2, W: 2,
		Heartbeat: -1, TickInterval: 50 * time.Millisecond, ProbeInterval: 200 * time.Millisecond,
		KVNode: func(a string) kv.Node { return m.kv[a] }})
	if err != nil {
		m.t.Fatalf("start %s: %v", name, err)
	}
	m.nodes[name] = nd
	return nd
}

func (m *mesh) do(node, method, path, body string) (int, string) {
	m.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	m.nodes[node].Handler().ServeHTTP(rec, req)
	b, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(b)
}

func (m *mesh) closeAll() {
	for _, nd := range m.nodes {
		nd.Close()
	}
	time.Sleep(100 * time.Millisecond)
}

func (m *mesh) leader() *Node {
	for i := 0; i < 100; i++ {
		for _, nd := range m.nodes {
			if nd.Control().IsLeader() {
				return nd
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	m.t.Fatal("no control-plane leader")
	return nil
}

func TestMeshEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newMesh(t, 3)
		defer m.closeAll()
		time.Sleep(3 * time.Second)

		// Gossip converged and the ring was installed everywhere.
		for name, nd := range m.nodes {
			alive := 0
			for _, mem := range nd.Members() {
				if mem.State == swim.StateAlive {
					alive++
				}
			}
			if alive != 3 {
				t.Fatalf("%s sees %d alive members", name, alive)
			}
			if nd.KV().Epoch() != 1 || len(nd.KV().Addrs()) != 3 {
				t.Fatalf("%s ring epoch %d addrs %v", name, nd.KV().Epoch(), nd.KV().Addrs())
			}
		}

		// KV through any node.
		if code, body := m.do("n1", "PUT", "/kv/hello", "world"); code != 204 {
			t.Fatalf("put: %d %s", code, body)
		}
		if code, body := m.do("n3", "GET", "/kv/hello", ""); code != 200 || body != "world" {
			t.Fatalf("get via another node: %d %q", code, body)
		}

		// Leases with fencing tokens.
		code, body := m.do("n2", "POST", "/lease/db-primary?holder=alice&ttl=60s", "")
		if code != 200 || !strings.Contains(body, `"token"`) {
			t.Fatalf("acquire: %d %s", code, body)
		}
		if code, _ := m.do("n3", "POST", "/lease/db-primary?holder=bob&ttl=5s", ""); code != 409 {
			t.Fatalf("second holder got %d, want 409", code)
		}

		// Jobs: enqueue on one node, claim on another, complete.
		if code, body := m.do("n1", "POST", "/jobs/emails?id=welcome-1", "hi"); code != 200 {
			t.Fatalf("enqueue: %d %s", code, body)
		}
		code, body = m.do("n3", "POST", "/jobs/emails/claim?worker=w1&visibility=30s", "")
		var job struct {
			ID    string `json:"id"`
			Token uint64 `json:"token"`
		}
		json.Unmarshal([]byte(body), &job)
		if code != 200 || job.ID != "welcome-1" {
			t.Fatalf("claim: %d %s", code, body)
		}
		if code, body := m.do("n2", "POST", fmt.Sprintf("/jobs/emails/welcome-1/complete?token=%d", job.Token), ""); code != 200 {
			t.Fatalf("complete: %d %s", code, body)
		}

		// Virtual actors: one counter, asked from every node.
		var owner string
		for i, from := range []string{"n1", "n2", "n3", "n1"} {
			code, body := m.do(from, "POST", "/actor/counter/visits", "inc")
			var r struct{ Reply, Node string }
			json.Unmarshal([]byte(body), &r)
			if code != 200 || r.Reply != fmt.Sprint(i+1) {
				t.Fatalf("actor ask %d via %s: %d %s", i, from, code, body)
			}
			owner = r.Node
		}

		// The status endpoint reflects the mesh.
		code, body = m.do("n1", "GET", "/mesh/status", "")
		var st MeshStatus
		if err := json.Unmarshal([]byte(body), &st); code != 200 || err != nil || len(st.Members) != 3 || len(st.Transport) == 0 {
			t.Fatalf("status: %d %v members=%d transport=%d", code, err, len(st.Members), len(st.Transport))
		}

		// Crash the actor's owner (if it's not alone): SWIM declares it dead,
		// the actor re-activates elsewhere from its persisted state, and if
		// it led the control plane a new leader takes over.
		m.nodes[owner].Crash()
		delete(m.nodes, owner)
		time.Sleep(8 * time.Second)
		var survivor string
		for name := range m.nodes {
			survivor = name
		}
		code, body = m.do(survivor, "POST", "/actor/counter/visits", "inc")
		var r struct{ Reply, Node string }
		json.Unmarshal([]byte(body), &r)
		if code != 200 || r.Reply != "5" || r.Node == owner {
			t.Fatalf("actor after owner crash: %d %s (old owner %s)", code, body, owner)
		}
		if code, body := m.do(survivor, "GET", "/kv/hello", ""); code != 200 || body != "world" {
			t.Fatalf("kv after crash: %d %q", code, body)
		}
		l := m.leader()
		if l.Name() == owner {
			t.Fatal("crashed node still leads")
		}
		if code, _ := m.do(survivor, "POST", "/lease/db-primary?holder=bob&ttl=5s", ""); code != 409 {
			t.Fatalf("lease state lost across failover (got %d)", code)
		}
	})
}

func TestMeshJoinPromotesLearner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newMesh(t, 3)
		defer m.closeAll()
		time.Sleep(3 * time.Second)
		m.start("n4", nil, []string{"n1:7000"})
		time.Sleep(6 * time.Second)
		l := m.leader()
		st := l.Control().Status()
		found := false
		for _, v := range st.Voters {
			if v == "n4" {
				found = true
			}
		}
		if !found {
			t.Fatalf("n4 not promoted: voters %v learners %v", st.Voters, st.Learners)
		}
		if m.nodes["n4"].KV().Epoch() != 1 {
			t.Fatal("joiner did not adopt the ring")
		}
		if code, body := m.do("n4", "GET", "/leases", ""); code != 200 {
			t.Fatalf("linearizable read on the joiner: %d %s", code, body)
		}
	})
}

func TestRingAdminGoesThroughConsensus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newMesh(t, 3)
		defer m.closeAll()
		time.Sleep(3 * time.Second)
		m.kv["kv4"] = &memKV{addr: "kv4", data: map[string]kv.ReadResult{}}
		if code, body := m.do("n2", "POST", "/admin/nodes?addr=kv4", ""); code != 200 {
			t.Fatalf("add node: %d %s", code, body)
		}
		time.Sleep(time.Second)
		for name, nd := range m.nodes {
			if nd.KV().Epoch() != 2 || len(nd.KV().Addrs()) != 4 {
				t.Fatalf("%s did not adopt epoch 2: %d %v", name, nd.KV().Epoch(), nd.KV().Addrs())
			}
		}
		if code, _ := m.do("n1", "POST", "/admin/nodes?addr=kv4", ""); code != http.StatusConflict {
			t.Fatalf("duplicate add: %d", code)
		}
	})
}
