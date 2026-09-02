package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/cluster"
	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// memNode is a tiny last-writer-wins store so HTTP tests don't need kvnode.
type memNode struct {
	addr string
	mu   sync.Mutex
	data map[string]kv.ReadResult
}

func newMem(addr string) *memNode {
	return &memNode{addr: addr, data: map[string]kv.ReadResult{}}
}

func (m *memNode) Addr() string { return m.addr }
func (m *memNode) Ping() error  { return nil }

func (m *memNode) Stat() (kv.Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := kv.Stat{Capacity: 64}
	for _, r := range m.data {
		if r.State == kv.Present {
			st.Count++
		} else if r.State == kv.Deleted {
			st.Tombstones++
		}
		if r.Version > st.MaxVersion {
			st.MaxVersion = r.Version
		}
	}
	return st, nil
}

func (m *memNode) Get(key []byte) (kv.ReadResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.data[string(key)]; ok {
		return r, nil
	}
	return kv.ReadResult{State: kv.Missing}, nil
}

func (m *memNode) Put(key, val []byte, version uint64) error {
	return m.PutX(key, val, version, 0)
}

func (m *memNode) PutX(key, val []byte, version, expiresAt uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.data[string(key)]; ok && cur.Version > version {
		return nil
	}
	m.data[string(key)] = kv.ReadResult{
		State:     kv.Present,
		Value:     append([]byte(nil), val...),
		Version:   version,
		ExpiresAt: expiresAt,
	}
	return nil
}

func (m *memNode) Del(key []byte, version uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.data[string(key)]
	if ok && cur.Version > version {
		return cur.State == kv.Present, nil
	}
	existed := ok && cur.State == kv.Present
	m.data[string(key)] = kv.ReadResult{State: kv.Deleted, Version: version}
	return existed, nil
}

func (m *memNode) Scan() ([]kv.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []kv.Entry
	for k, r := range m.data {
		if r.State == kv.Present {
			out = append(out, kv.Entry{Key: []byte(k), Value: r.Value, Version: r.Version, ExpiresAt: r.ExpiresAt})
		}
	}
	return out, nil
}

func (m *memNode) ScanAll() ([]kv.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []kv.Entry
	for k, r := range m.data {
		if r.State == kv.Missing {
			continue
		}
		out = append(out, kv.Entry{
			Key:       []byte(k),
			Value:     r.Value,
			Version:   r.Version,
			ExpiresAt: r.ExpiresAt,
			Tombstone: r.State == kv.Deleted,
		})
	}
	return out, nil
}

func testGateway() (*httptest.Server, *cluster.Cluster) {
	addrs := []string{"n0", "n1", "n2"}
	nodes := map[string]kv.Node{}
	for _, a := range addrs {
		nodes[a] = newMem(a)
	}
	cl := cluster.New(nodes, addrs, 3, 2, 2)
	g := &gateway{cluster: cl, timeout: time.Second}
	return httptest.NewServer(g.routes()), cl
}

func TestHTTPCompareAndSet(t *testing.T) {
	srv, _ := testGateway()
	defer srv.Close()

	req, _ := http.NewRequest("PUT", srv.URL+"/kv/k", strings.NewReader("one"))
	req.Header.Set("If-Match", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("create CAS: %v %v", err, resp)
	}
	ver := resp.Header.Get("X-KV-Version")
	if ver == "" {
		ver = resp.Header.Get("X-Triglot-Version")
	}
	resp.Body.Close()
	if ver == "" || ver == "0" {
		t.Fatal("missing version")
	}

	req, _ = http.NewRequest("PUT", srv.URL+"/kv/k", strings.NewReader("stale"))
	req.Header.Set("X-KV-CAS", "1")
	if ver == "1" {
		req.Header.Set("X-KV-CAS", "999")
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("want 409, got %d %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	req, _ = http.NewRequest("PUT", srv.URL+"/kv/k", strings.NewReader("two"))
	req.Header.Set("If-Match", ver)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("update CAS: %v status=%v", err, resp.StatusCode)
	}
	resp.Body.Close()

	got, err := http.Get(srv.URL + "/kv/k")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if got.StatusCode != 200 || string(body) != "two" {
		t.Fatalf("get = %d %q", got.StatusCode, body)
	}
}

func TestHTTPPrefixScanExportImportAndTTLHeader(t *testing.T) {
	srv, cl := testGateway()
	defer srv.Close()
	now := time.Unix(1_900_000_000, 0)
	cl.SetNow(func() time.Time { return now })

	put := func(k, v, ttl string) {
		t.Helper()
		req, _ := http.NewRequest("PUT", srv.URL+"/kv/"+k, strings.NewReader(v))
		if ttl != "" {
			req.Header.Set("X-KV-TTL", ttl)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusNoContent {
			t.Fatalf("put %s: %v %v", k, err, resp)
		}
		resp.Body.Close()
	}
	put("foo", "1", "")
	put("foobar", "2", "")
	put("bar", "3", "")
	put("ephem", "soon", "30")

	resp, err := http.Get(srv.URL + "/scan?prefix=foo")
	if err != nil {
		t.Fatal(err)
	}
	var scan struct {
		Count   int              `json:"count"`
		Entries []map[string]any `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&scan); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if scan.Count != 2 {
		t.Fatalf("prefix scan count=%d entries=%v", scan.Count, scan.Entries)
	}

	resp, err = http.Get(srv.URL + "/export")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if bytes.Contains(raw, []byte("TGTTL1")) || bytes.Contains(raw, []byte("KVTTL1")) {
		t.Fatalf("export leaked envelope: %s", raw)
	}
	var doc cluster.ExportDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Entries) != 4 {
		t.Fatalf("export entries=%d", len(doc.Entries))
	}

	// Fresh gateway; import should restore live keys.
	srv2, cl2 := testGateway()
	defer srv2.Close()
	cl2.SetNow(func() time.Time { return now })
	resp, err = http.Post(srv2.URL+"/import", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	var ir struct {
		Imported int `json:"imported"`
		Skipped  int `json:"skipped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ir); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if ir.Imported != 4 || ir.Skipped != 0 {
		t.Fatalf("import %+v", ir)
	}
	got, err := http.Get(srv2.URL + "/kv/ephem")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if got.StatusCode != 200 || string(body) != "soon" {
		t.Fatalf("imported ttl get = %d %q", got.StatusCode, body)
	}
	if got.Header.Get("X-KV-Expires-At") == "" && got.Header.Get("X-Triglot-Expires-At") == "" {
		t.Fatal("missing X-KV-Expires-At on TTL get")
	}
}

func TestDashboardKVStore(t *testing.T) {
	if !strings.Contains(dashboardHTML, "KVStore") {
		t.Fatal("dashboard missing KVStore title")
	}
	if !strings.Contains(dashboardHTML, "#0b0d10") || !strings.Contains(dashboardHTML, "#9ae6b4") {
		t.Fatal("dashboard missing control-room palette")
	}
	srv, _ := testGateway()
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(body, []byte("KVStore")) {
		t.Fatalf("GET /dashboard = %d", resp.StatusCode)
	}
}

func TestHTTPAcceptsLegacyTriglotHeaders(t *testing.T) {
	srv, cl := testGateway()
	defer srv.Close()
	now := time.Unix(1_900_000_000, 0)
	cl.SetNow(func() time.Time { return now })

	req, _ := http.NewRequest("PUT", srv.URL+"/kv/legacy", strings.NewReader("x"))
	req.Header.Set("X-Triglot-TTL", "15")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("legacy TTL header: %v %v", err, resp)
	}
	resp.Body.Close()

	got, err := http.Get(srv.URL + "/kv/legacy")
	if err != nil {
		t.Fatal(err)
	}
	got.Body.Close()
	if got.Header.Get("X-KV-Expires-At") == "" {
		t.Fatal("native expiry missing after legacy TTL header")
	}
}
