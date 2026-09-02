// gateway — HTTP front door for a fleet of kvnodes. Each key is replicated to
// RF nodes chosen by rendezvous hashing; reads and writes use quorums (R, W).
//
//	gateway --listen :8080 --nodes n0,n1,n2 --rf 3 --read 2 --write 2
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/client"
	"github.com/Sebby1770/kvstore/kvgw/internal/cluster"
	"github.com/Sebby1770/kvstore/kvgw/internal/crdt"
	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

type gateway struct {
	cluster *cluster.Cluster
	timeout time.Duration // dial/read timeout for nodes added at runtime
	tag     uint64        // monotonic source of unique OR-Set tags
}

func kvReqHeader(r *http.Request, name string) string {
	if v := strings.TrimSpace(r.Header.Get("X-KV-" + name)); v != "" {
		return v
	}
	return strings.TrimSpace(r.Header.Get("X-Triglot-" + name))
}

func setKVHeader(w http.ResponseWriter, name, value string) {
	w.Header().Set("X-KV-"+name, value)
	w.Header().Set("X-Triglot-"+name, value) // alias for one release
}

func (g *gateway) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"nodes": g.cluster.Addrs(),
		"rf":    g.cluster.RF(),
		"read":  g.cluster.R(),
		"write": g.cluster.W(),
	})
}

func (g *gateway) handleAddNode(w http.ResponseWriter, r *http.Request) {
	addr := strings.TrimSpace(r.URL.Query().Get("addr"))
	if addr == "" {
		http.Error(w, "addr query parameter required", http.StatusBadRequest)
		return
	}
	if !g.cluster.AddNode(addr, client.New(addr, g.timeout)) {
		http.Error(w, "node already present", http.StatusConflict)
		return
	}
	log.Printf("membership: added %s -> %v", addr, g.cluster.Addrs())
	writeJSON(w, map[string]any{"added": addr, "nodes": g.cluster.Addrs(), "rf": g.cluster.RF()})
}

func (g *gateway) handleCompact(w http.ResponseWriter, r *http.Request) {
	n, errs := g.cluster.CompactAll()
	writeJSON(w, map[string]any{"compacted": n, "errors": errs})
}

func (g *gateway) handleDeletePrefix(w http.ResponseWriter, r *http.Request) {
	n, err := g.cluster.DeletePrefix(r.URL.Query().Get("prefix"))
	out := map[string]any{"deleted": n}
	if err != nil {
		out["error"] = err.Error()
	}
	writeJSON(w, out)
}

func (g *gateway) handleFault(w http.ResponseWriter, r *http.Request) {
	addr := strings.TrimSpace(r.URL.Query().Get("addr"))
	if addr == "" {
		http.Error(w, "addr query parameter required", http.StatusBadRequest)
		return
	}
	raw := strings.TrimSpace(r.URL.Query().Get("down"))
	var down bool
	switch raw {
	case "1", "true", "on":
		down = true
	case "0", "false", "off":
		down = false
	default:
		http.Error(w, "down must be 1 or 0", http.StatusBadRequest)
		return
	}
	g.cluster.ForceDown(addr, down)
	writeJSON(w, map[string]any{"addr": addr, "down": down, "forced": down})
}

func (g *gateway) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	addr := r.PathValue("addr")
	if !g.cluster.RemoveNode(addr) {
		http.Error(w, "node not present, or it is the last node", http.StatusConflict)
		return
	}
	log.Printf("membership: removed %s -> %v", addr, g.cluster.Addrs())
	writeJSON(w, map[string]any{"removed": addr, "nodes": g.cluster.Addrs(), "rf": g.cluster.RF()})
}

func (g *gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	val, found, meta, err := g.cluster.Get(key)
	setKVHeader(w, "Replicas", strings.Join(meta.Replicas, ","))
	setKVHeader(w, "Responders", strings.Join(meta.Responders, ","))
	if len(meta.Repaired) > 0 {
		setKVHeader(w, "Repaired", strings.Join(meta.Repaired, ","))
	}
	if errors.Is(err, cluster.ErrQuorum) {
		http.Error(w, "read quorum not reached", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	setKVHeader(w, "Version", strconv.FormatUint(meta.Version, 10))
	if meta.ExpiresAt > 0 {
		setKVHeader(w, "Expires-At", strconv.FormatInt(meta.ExpiresAt, 10))
	}
	w.Write(val)
}

func (g *gateway) handlePut(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var ttl time.Duration
	if raw := kvReqHeader(r, "TTL"); raw != "" {
		secs, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || secs < 0 {
			http.Error(w, "X-KV-TTL must be a non-negative integer (seconds)", http.StatusBadRequest)
			return
		}
		if secs > 0 {
			ttl = time.Duration(secs) * time.Second
		}
	}
	casVer, hasCAS, err := parseCASHeader(r)
	if err != nil {
		http.Error(w, "If-Match/X-KV-CAS must be a version number", http.StatusBadRequest)
		return
	}

	var meta cluster.WriteMeta
	if hasCAS {
		meta, err = g.cluster.CASTTL(key, casVer, body, ttl)
	} else if ttl > 0 {
		meta, err = g.cluster.PutTTL(key, body, ttl)
	} else {
		meta, err = g.cluster.Put(key, body)
	}
	g.writeMeta(w, meta)
	if errors.Is(err, cluster.ErrConflict) {
		http.Error(w, "version conflict", http.StatusConflict)
		return
	}
	if errors.Is(err, cluster.ErrQuorum) {
		http.Error(w, fmt.Sprintf("write quorum not reached (%d/%d acks)", meta.Acks, meta.Required), http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseCASHeader(r *http.Request) (uint64, bool, error) {
	raw := kvReqHeader(r, "CAS")
	if raw == "" {
		raw = strings.TrimSpace(r.Header.Get("If-Match"))
	}
	if raw == "" {
		return 0, false, nil
	}
	raw = strings.Trim(raw, `"`)
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

func (g *gateway) handleDel(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	existed, meta, err := g.cluster.Del(key)
	g.writeMeta(w, meta)
	if errors.Is(err, cluster.ErrQuorum) {
		http.Error(w, fmt.Sprintf("write quorum not reached (%d/%d acks)", meta.Acks, meta.Required), http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if !existed {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *gateway) writeMeta(w http.ResponseWriter, meta cluster.WriteMeta) {
	setKVHeader(w, "Replicas", strings.Join(meta.Replicas, ","))
	setKVHeader(w, "Version", strconv.FormatUint(meta.Version, 10))
	setKVHeader(w, "Acks", fmt.Sprintf("%d/%d", meta.Acks, meta.Required))
}

func (g *gateway) handleScan(w http.ResponseWriter, r *http.Request) {
	rows, errs := g.cluster.ScanPrefix(r.URL.Query().Get("prefix"))
	w.Header().Set("Content-Type", "application/json")
	if len(errs) > 0 {
		setKVHeader(w, "Errors", strings.Join(errs, "; "))
	}
	json.NewEncoder(w).Encode(map[string]any{
		"count":   len(rows),
		"entries": rows,
		"errors":  errs,
	})
}

func (g *gateway) handleExport(w http.ResponseWriter, r *http.Request) {
	doc, errs := g.cluster.Export()
	if len(errs) > 0 {
		setKVHeader(w, "Errors", strings.Join(errs, "; "))
	}
	writeJSON(w, doc)
}

func (g *gateway) handleImport(w http.ResponseWriter, r *http.Request) {
	var doc cluster.ExportDoc
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&doc); err != nil {
		http.Error(w, "invalid export JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	imported, skipped, errs := g.cluster.Import(doc)
	writeJSON(w, map[string]any{
		"imported": imported,
		"skipped":  skipped,
		"errors":   errs,
	})
}

func (g *gateway) handleRoute(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"key":      string(key),
		"replicas": g.cluster.Route(key),
	})
}

// handleCounter is a PN-Counter: GET returns the value, POST?delta=N merges in
// an increment (default +1). Concurrent increments all count.
func (g *gateway) handleCounter(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	var delta int64
	if r.Method == http.MethodPost {
		delta = 1
		if d := r.URL.Query().Get("delta"); d != "" {
			n, err := strconv.ParseInt(d, 10, 64)
			if err != nil {
				http.Error(w, "delta must be an integer", http.StatusBadRequest)
				return
			}
			delta = n
		}
	}
	newVal, meta, err := g.cluster.Mutate([]byte(key), func(states [][]byte) ([]byte, error) {
		ctr := crdt.NewPNCounter()
		for _, s := range states {
			if o, e := crdt.ParsePNCounter(s); e == nil {
				ctr.Merge(o)
			}
		}
		if delta != 0 {
			ctr.Add("gw", delta)
		}
		return ctr.Bytes(), nil
	})
	g.writeMeta(w, meta)
	if err != nil {
		g.crdtError(w, err)
		return
	}
	ctr, _ := crdt.ParsePNCounter(newVal)
	writeJSON(w, map[string]any{"key": key, "type": "counter", "value": ctr.Value()})
}

// handleSet is an observed-remove set: GET returns members, POST?add=x / ?remove=x
// mutate it. Concurrent adds and removes merge (add-wins).
func (g *gateway) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	add := r.URL.Query().Get("add")
	remove := r.URL.Query().Get("remove")
	tag := fmt.Sprintf("gw:%d", atomic.AddUint64(&g.tag, 1))
	newVal, meta, err := g.cluster.Mutate([]byte(key), func(states [][]byte) ([]byte, error) {
		set := crdt.NewORSet()
		for _, s := range states {
			if o, e := crdt.ParseORSet(s); e == nil {
				set.Merge(o)
			}
		}
		if add != "" {
			set.Add(add, tag)
		}
		if remove != "" {
			set.Remove(remove)
		}
		return set.Bytes(), nil
	})
	g.writeMeta(w, meta)
	if err != nil {
		g.crdtError(w, err)
		return
	}
	set, _ := crdt.ParseORSet(newVal)
	writeJSON(w, map[string]any{"key": key, "type": "set", "members": set.Members()})
}

func (g *gateway) crdtError(w http.ResponseWriter, err error) {
	if errors.Is(err, cluster.ErrQuorum) {
		http.Error(w, "quorum not reached", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, err.Error(), http.StatusBadGateway)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (g *gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", g.handleGet)
	mux.HandleFunc("PUT /kv/{key}", g.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", g.handleDel)
	mux.HandleFunc("GET /scan", g.handleScan)
	mux.HandleFunc("DELETE /scan", g.handleDeletePrefix)
	mux.HandleFunc("GET /export", g.handleExport)
	mux.HandleFunc("POST /import", g.handleImport)
	mux.HandleFunc("GET /route/{key}", g.handleRoute)
	mux.HandleFunc("GET /crdt/counter/{key}", g.handleCounter)
	mux.HandleFunc("POST /crdt/counter/{key}", g.handleCounter)
	mux.HandleFunc("GET /crdt/set/{key}", g.handleSet)
	mux.HandleFunc("POST /crdt/set/{key}", g.handleSet)
	mux.HandleFunc("GET /health", g.handleHealth)
	mux.HandleFunc("GET /metrics", g.handleMetrics)
	mux.HandleFunc("GET /admin/nodes", g.handleListNodes)
	mux.HandleFunc("POST /admin/nodes", g.handleAddNode)
	mux.HandleFunc("DELETE /admin/nodes/{addr}", g.handleRemoveNode)
	mux.HandleFunc("POST /admin/compact", g.handleCompact)
	mux.HandleFunc("POST /admin/fault", g.handleFault)
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "KVStore gateway\n")
		fmt.Fprintf(w, "nodes: %s\n", strings.Join(g.cluster.Addrs(), ", "))
		fmt.Fprintf(w, "RF=%d R=%d W=%d  (R+W>RF: %v)\n", g.cluster.RF(), g.cluster.R(), g.cluster.W(), g.cluster.Consistent())
		fmt.Fprintf(w, "live dashboard: GET /dashboard   metrics: GET /metrics\n")
		fmt.Fprintf(w, "endpoints: GET|PUT|DELETE /kv/{key}, GET|DELETE /scan?prefix=, GET /route/{key}, GET /health\n")
		fmt.Fprintf(w, "cas: PUT /kv/{key} with If-Match or X-KV-CAS: <version>  (409 on conflict)\n")
		fmt.Fprintf(w, "ttl: PUT /kv/{key} with X-KV-TTL: <seconds>  (native engine expires_at)\n")
		fmt.Fprintf(w, "snapshot: GET /export, POST /import\n")
		fmt.Fprintf(w, "crdt: GET|POST /crdt/counter/{key}?delta=N, GET|POST /crdt/set/{key}?add=x&remove=y\n")
		fmt.Fprintf(w, "admin: GET /admin/nodes, POST /admin/nodes?addr=host:port, DELETE /admin/nodes/{addr}\n")
		fmt.Fprintf(w, "compact: POST /admin/compact  (GC tombstones on every live node)\n")
		fmt.Fprintf(w, "prefix delete: DELETE /scan?prefix=  fault inject: POST /admin/fault?addr=&down=1|0\n")
	})
	return mux
}

func (g *gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	nodes, allUp := g.cluster.Health()
	w.Header().Set("Content-Type", "application/json")
	if !allUp {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]any{
		"healthy":    allUp,
		"rf":         g.cluster.RF(),
		"read":       g.cluster.R(),
		"write":      g.cluster.W(),
		"consistent": g.cluster.Consistent(),
		"nodes":      nodes,
		"metrics":    g.cluster.Metrics(),
	})
}

// handleMetrics exposes the coordinator's counters in Prometheus text format.
func (g *gateway) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := g.cluster.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	counters := []struct {
		name, help string
		val        uint64
	}{
		{"kvstore_gets_total", "quorum reads", m.Gets},
		{"kvstore_puts_total", "quorum writes", m.Puts},
		{"kvstore_dels_total", "quorum deletes", m.Dels},
		{"kvstore_mutations_total", "CRDT read-merge-writes", m.Mutations},
		{"kvstore_quorum_failures_total", "operations that missed quorum", m.QuorumFailures},
		{"kvstore_read_repairs_total", "replicas healed on the read path", m.ReadRepairs},
		{"kvstore_sweep_repairs_total", "replicas healed by anti-entropy", m.SweepRepairs},
		{"kvstore_hints_stored_total", "hinted-handoff records stored", m.HintsStored},
		{"kvstore_hints_replayed_total", "hinted-handoff records replayed", m.HintsReplayed},
		{"kvstore_compacts_total", "cluster compact (tombstone GC) operations", m.Compacts},
		{"kvstore_prefix_deletes_total", "prefix-delete operations", m.PrefixDeletes},
	}
	for _, c := range counters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", c.name, c.help, c.name, c.name, c.val)
	}
	fmt.Fprintf(w, "# TYPE kvstore_nodes gauge\nkvstore_nodes %d\n", len(g.cluster.Addrs()))
	fmt.Fprintf(w, "# TYPE kvstore_rf gauge\nkvstore_rf %d\n", g.cluster.RF())
	fmt.Fprintf(w, "# TYPE kvstore_latency_get_ms gauge\nkvstore_latency_get_ms %g\n", m.LatencyGetMs)
	fmt.Fprintf(w, "# TYPE kvstore_latency_put_ms gauge\nkvstore_latency_put_ms %g\n", m.LatencyPutMs)
}

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	nodesCSV := flag.String("nodes", "127.0.0.1:7070", "comma-separated kvnode addresses")
	timeout := flag.Duration("timeout", 2*time.Second, "per-call dial/read timeout")
	rf := flag.Int("rf", 3, "replication factor (clamped to node count)")
	rq := flag.Int("read", 2, "read quorum R")
	wq := flag.Int("write", 2, "write quorum W")
	ae := flag.Duration("anti-entropy", 0, "background repair sweep interval (0 = off)")
	hb := flag.Duration("heartbeat", 2*time.Second, "failure-detection ping interval (0 = off)")
	flag.Parse()

	addrs := strings.Split(*nodesCSV, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	if len(addrs) == 0 || addrs[0] == "" {
		log.Fatal("--nodes is required")
	}

	nodes := make(map[string]kv.Node, len(addrs))
	for _, a := range addrs {
		nodes[a] = client.New(a, *timeout)
	}
	cl := cluster.New(nodes, addrs, *rf, *rq, *wq)

	g := &gateway{cluster: cl, timeout: *timeout}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           g.routes(),
		ReadHeaderTimeout: 3 * time.Second,
	}

	stopBG := make(chan struct{})
	defer close(stopBG)

	if *ae > 0 {
		go func() {
			tk := time.NewTicker(*ae)
			defer tk.Stop()
			for {
				select {
				case <-stopBG:
					return
				case <-tk.C:
					if n, errs := cl.RepairSweep(); n > 0 || len(errs) > 0 {
						log.Printf("anti-entropy: %d repairs, errs=%v", n, errs)
					}
				}
			}
		}()
		log.Printf("anti-entropy: sweeping every %s", *ae)
	}

	if *hb > 0 {
		go cl.RunHeartbeat(*hb, stopBG)
		log.Printf("heartbeat: pinging every %s", *hb)
	}

	go func() {
		log.Printf("gateway: listening on %s, nodes=%v, RF=%d R=%d W=%d (consistent=%v)",
			*listen, addrs, cl.RF(), cl.R(), cl.W(), cl.Consistent())
		if !cl.Consistent() {
			log.Printf("gateway: warning — R+W <= RF, reads may miss recent writes")
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("gateway: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
