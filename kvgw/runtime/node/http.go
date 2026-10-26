package node

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/runtime/actor"
	"github.com/Sebby1770/kvstore/kvgw/runtime/control"
	"github.com/Sebby1770/kvstore/kvgw/runtime/swim"
)

type ed25519PublicKey = ed25519.PublicKey

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func errJSON(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

// controlStatus maps control-plane failures to HTTP statuses.
func controlStatus(err error) int {
	switch {
	case errors.Is(err, control.ErrConflict), errors.Is(err, control.ErrNotHolder),
		errors.Is(err, control.ErrStaleToken), errors.Is(err, control.ErrExpired):
		return http.StatusConflict
	case errors.Is(err, control.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, control.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, control.ErrFull):
		return http.StatusInsufficientStorage
	}
	return http.StatusServiceUnavailable
}

func duration(r *http.Request, name string, def time.Duration) (time.Duration, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	secs, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration", name, s)
	}
	return time.Duration(secs * float64(time.Second)), nil
}

func (n *Node) reqCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 8*time.Second)
}

func (n *Node) routes() {
	mux := n.api.Mux()
	mux.HandleFunc("GET /mesh", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, meshboardHTML)
	})
	mux.HandleFunc("GET /mesh/status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, n.Status()) })
	mux.HandleFunc("GET /mesh/metrics", n.handleMeshMetrics)
	mux.HandleFunc("POST /mesh/raft/transfer", func(w http.ResponseWriter, r *http.Request) {
		to := r.URL.Query().Get("to")
		if to == "" {
			errJSON(w, 400, errors.New("to= is required"))
			return
		}
		n.ctl.TransferLeadership(to)
		writeJSON(w, 202, map[string]any{"transfer_to": to})
	})

	// Leases with fencing tokens.
	mux.HandleFunc("POST /lease/{name}", n.handleLease)
	mux.HandleFunc("DELETE /lease/{name}", n.handleLease)
	mux.HandleFunc("GET /leases", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		if err := n.ctl.ReadBarrier(ctx); err != nil {
			errJSON(w, 503, err)
			return
		}
		writeJSON(w, 200, map[string]any{"leases": n.ctl.FSM().Leases()})
	})

	// Job queues.
	mux.HandleFunc("POST /jobs/{queue}", n.handleEnqueue)
	mux.HandleFunc("POST /jobs/{queue}/claim", n.handleClaim)
	mux.HandleFunc("POST /jobs/{queue}/{id}/{op}", n.handleJobOp)
	mux.HandleFunc("GET /jobs/{queue}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		if err := n.ctl.ReadBarrier(ctx); err != nil {
			errJSON(w, 503, err)
			return
		}
		q := r.PathValue("queue")
		writeJSON(w, 200, map[string]any{"stats": n.ctl.FSM().Queue(q), "jobs": n.ctl.FSM().Jobs(q)})
	})

	// Registry.
	mux.HandleFunc("GET /registry/{key}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		if err := n.ctl.ReadBarrier(ctx); err != nil {
			errJSON(w, 503, err)
			return
		}
		e, ok := n.ctl.FSM().Get(r.PathValue("key"))
		if !ok {
			errJSON(w, 404, errors.New("not found"))
			return
		}
		writeJSON(w, 200, map[string]any{"key": e.Key, "value": string(e.Value), "version": e.Version})
	})
	mux.HandleFunc("PUT /registry/{key}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		body, _ := io.ReadAll(io.LimitReader(r.Body, control.MaxValueLen+1))
		cmd := control.RegPutCmd(r.PathValue("key"), body, n.ctl.Now())
		if v := r.Header.Get("If-Match"); v != "" {
			ev, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				errJSON(w, 400, err)
				return
			}
			cmd = control.RegCASCmd(r.PathValue("key"), body, ev, n.ctl.Now())
		}
		res, err := n.ctl.Apply(ctx, cmd)
		if err != nil {
			errJSON(w, controlStatus(err), err)
			return
		}
		writeJSON(w, 200, map[string]any{"key": res.Entry.Key, "version": res.Entry.Version})
	})

	// Virtual actors.
	mux.HandleFunc("POST /actor/{type}/{key}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		id := actor.ID{Type: r.PathValue("type"), Key: r.PathValue("key")}
		owner, _ := n.members.Owner(id)
		reply, err := n.actors.Ask(ctx, id, body)
		if err != nil {
			status := 503
			if errors.Is(err, actor.ErrUnknownType) {
				status = 404
			} else if errors.Is(err, actor.ErrStaleActivation) {
				status = 409
			}
			writeJSON(w, status, map[string]any{"error": err.Error(), "node": owner})
			return
		}
		writeJSON(w, 200, map[string]any{"reply": string(reply), "node": owner})
	})
	mux.HandleFunc("GET /mesh/actors", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"activations": n.actors.Activations(), "stats": n.actors.Stats(), "types": n.actors.Types()})
	})
}

func (n *Node) handleLease(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := n.reqCtx(r)
	defer cancel()
	q := r.URL.Query()
	name, holder := r.PathValue("name"), q.Get("holder")
	token, _ := strconv.ParseUint(q.Get("token"), 10, 64)
	var cmd []byte
	switch {
	case r.Method == http.MethodDelete:
		cmd = control.ReleaseLeaseCmd(name, holder, token, n.ctl.Now())
	case q.Get("renew") != "":
		ttl, err := duration(r, "ttl", 0)
		if err != nil {
			errJSON(w, 400, err)
			return
		}
		cmd = control.RenewLeaseCmd(name, holder, token, ttl, n.ctl.Now())
	default:
		ttl, err := duration(r, "ttl", 10*time.Second)
		if err != nil {
			errJSON(w, 400, err)
			return
		}
		cmd = control.AcquireLeaseCmd(name, holder, ttl, n.ctl.Now())
	}
	res, err := n.ctl.Propose(ctx, cmd)
	if err != nil {
		errJSON(w, 503, err)
		return
	}
	out := map[string]any{"name": name}
	if res.Lease != nil {
		out["holder"], out["token"], out["expires_at"] = res.Lease.Holder, res.Lease.Token, res.Lease.ExpiresAt
	}
	if err := res.Err(); err != nil {
		out["error"], out["code"] = res.Error, string(res.Code)
		writeJSON(w, controlStatus(err), out)
		return
	}
	writeJSON(w, 200, out)
}

func (n *Node) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := n.reqCtx(r)
	defer cancel()
	q := r.URL.Query()
	payload, _ := io.ReadAll(io.LimitReader(r.Body, control.MaxPayloadLen+1))
	id := q.Get("id")
	if id == "" {
		id = fmt.Sprintf("%s-%s", n.cfg.Name, n.clock.Now().String())
	}
	attempts, _ := strconv.Atoi(q.Get("max_attempts"))
	delay, err := duration(r, "delay", 0)
	if err != nil {
		errJSON(w, 400, err)
		return
	}
	res, err := n.ctl.Apply(ctx, control.EnqueueJobCmd(r.PathValue("queue"), id, payload, attempts, delay, n.ctl.Now()))
	if err != nil {
		errJSON(w, controlStatus(err), err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": res.Job.ID, "existing": res.Existing, "queue": res.Job.Queue})
}

func jobJSON(j *control.Job) map[string]any {
	return map[string]any{"id": j.ID, "payload": string(j.Payload), "token": j.Token, "attempt": j.Attempts,
		"state": j.State, "worker": j.Worker, "visible_until": j.VisibleUntil}
}

func (n *Node) handleClaim(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := n.reqCtx(r)
	defer cancel()
	q := r.URL.Query()
	worker := q.Get("worker")
	if worker == "" {
		worker = "anonymous"
	}
	vis, err := duration(r, "visibility", control.DefaultVisibility)
	if err != nil {
		errJSON(w, 400, err)
		return
	}
	res, err := n.ctl.Apply(ctx, control.ClaimJobCmd(r.PathValue("queue"), worker, vis, n.ctl.Now()))
	if err != nil {
		errJSON(w, controlStatus(err), err)
		return
	}
	if res.Job == nil {
		writeJSON(w, http.StatusNoContent, nil)
		return
	}
	writeJSON(w, 200, jobJSON(res.Job))
}

func (n *Node) handleJobOp(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := n.reqCtx(r)
	defer cancel()
	q := r.URL.Query()
	queue, id, op := r.PathValue("queue"), r.PathValue("id"), r.PathValue("op")
	token, _ := strconv.ParseUint(q.Get("token"), 10, 64)
	var cmd []byte
	switch op {
	case "complete":
		cmd = control.CompleteJobCmd(queue, id, token, n.ctl.Now())
	case "fail":
		backoff, err := duration(r, "backoff", 0)
		if err != nil {
			errJSON(w, 400, err)
			return
		}
		cmd = control.FailJobCmd(queue, id, token, q.Get("error"), backoff, n.ctl.Now())
	case "extend":
		vis, err := duration(r, "visibility", control.DefaultVisibility)
		if err != nil {
			errJSON(w, 400, err)
			return
		}
		cmd = control.ExtendJobCmd(queue, id, token, vis, n.ctl.Now())
	case "redrive":
		cmd = control.RedriveJobCmd(queue, id, n.ctl.Now())
	default:
		errJSON(w, 404, fmt.Errorf("unknown job operation %q", op))
		return
	}
	res, err := n.ctl.Propose(ctx, cmd)
	if err != nil {
		errJSON(w, 503, err)
		return
	}
	out := map[string]any{}
	if res.Job != nil {
		out = jobJSON(res.Job)
	}
	out["duplicate"], out["dead_lettered"] = res.Duplicate, res.DeadLettered
	if err := res.Err(); err != nil {
		out["error"], out["code"] = res.Error, string(res.Code)
		writeJSON(w, controlStatus(err), out)
		return
	}
	writeJSON(w, 200, out)
}

// ---- status ----

// TransportInfo is one KTP connection as the dashboard shows it.
type TransportInfo struct {
	Peer          string  `json:"peer"`
	Addr          string  `json:"addr"`
	Client        bool    `json:"client"`
	SRTTms        float64 `json:"srtt_ms"`
	MinRTTms      float64 `json:"min_rtt_ms"`
	CWND          int     `json:"cwnd"`
	BytesInFlight int     `json:"bytes_in_flight"`
	PacketsSent   uint64  `json:"packets_sent"`
	PacketsLost   uint64  `json:"packets_lost"`
	Retransmits   uint64  `json:"retransmits"`
	Streams       int     `json:"streams"`
	Congestion    string  `json:"congestion"`
	State         string  `json:"state"`
}

// MeshStatus is GET /mesh/status.
type MeshStatus struct {
	Node      map[string]any  `json:"node"`
	Members   []swim.Member   `json:"members"`
	Health    int             `json:"local_health"`
	Raft      map[string]any  `json:"raft"`
	Control   map[string]any  `json:"control"`
	Transport []TransportInfo `json:"transport"`
	Endpoint  any             `json:"endpoint"`
	Actors    map[string]any  `json:"actors"`
	RPC       any             `json:"rpc"`
	Events    []Event         `json:"events"`
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Status snapshots the node for the dashboard and operators.
func (n *Node) Status() MeshStatus {
	members := n.swim.members()
	byAddr := map[string]string{}
	for _, m := range members {
		byAddr[m.Addr] = m.Name
	}
	st := n.ctl.Status()
	progress := map[string]any{}
	for id, p := range st.Progress {
		if id != n.cfg.Name {
			progress[id] = map[string]any{"match": p.Match, "next": p.Next, "state": p.State, "active": p.RecentActive}
		}
	}
	fsm := n.ctl.FSM()
	queues := map[string]any{}
	for _, q := range fsm.Queues() {
		s := fsm.Queue(q)
		queues[q] = map[string]any{"ready": s.Pending, "in_flight": s.Claimed, "completed": s.Completed, "dead": s.Dead}
	}
	var tr []TransportInfo
	for _, c := range n.ep.Conns() {
		s := c.Stats
		name := byAddr[c.Addr]
		if name == "" {
			name = c.Addr
		}
		tr = append(tr, TransportInfo{Peer: name, Addr: c.Addr, Client: c.Client, SRTTms: ms(s.SRTT), MinRTTms: ms(s.MinRTT),
			CWND: s.CWND, BytesInFlight: s.BytesInFlight, PacketsSent: s.PacketsSent, PacketsLost: s.PacketsLost,
			Retransmits: s.StreamBytesRetx, Streams: s.StreamsOpen, Congestion: s.Congestion, State: s.State})
	}
	sort.Slice(tr, func(i, j int) bool {
		if tr[i].Peer != tr[j].Peer {
			return tr[i].Peer < tr[j].Peer
		}
		return tr[i].Client && !tr[j].Client
	})
	var leaseList []map[string]any
	for _, l := range fsm.Leases() {
		leaseList = append(leaseList, map[string]any{"name": l.Name, "holder": l.Holder, "token": l.Token, "expires_at": l.ExpiresAt})
	}
	return MeshStatus{
		Node: map[string]any{"name": n.cfg.Name, "addr": n.cfg.Advertise, "http": n.cfg.HTTPAdvertise,
			"identity": Fingerprint(n.identity.Public().(ed25519.PublicKey)), "hlc": n.clock.Now().String()},
		Members: members,
		Health:  n.swim.localHealth(),
		Raft: map[string]any{"id": st.ID, "term": st.Term, "leader": st.Lead, "role": st.Role.String(),
			"commit": st.Commit, "applied": st.Applied, "last_index": st.LastIndex, "voters": st.Voters,
			"learners": st.Learners, "progress": progress},
		Control: map[string]any{"ring": fsm.Ring(), "leases": len(leaseList), "lease_list": leaseList,
			"queues": queues, "registry_keys": len(fsm.Keys(""))},
		Transport: tr,
		Endpoint:  n.ep.Stats(),
		Actors:    map[string]any{"activations": n.actors.Activations(), "stats": n.actors.Stats()},
		RPC:       n.rpcSrv.Stats(),
		Events:    n.events.recent(80),
	}
}

func (n *Node) handleMeshMetrics(w http.ResponseWriter, r *http.Request) {
	s := n.Status()
	st := n.ctl.Status()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	g := func(name, help string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, v)
	}
	alive := 0
	for _, m := range s.Members {
		if m.State == swim.StateAlive {
			alive++
		}
	}
	isLeader := 0
	if st.Lead == n.cfg.Name {
		isLeader = 1
	}
	g("kvmesh_members_alive", "SWIM members believed alive", alive)
	g("kvmesh_local_health", "Lifeguard local health score (0 = healthy)", s.Health)
	g("kvmesh_raft_term", "current Raft term", st.Term)
	g("kvmesh_raft_commit_index", "Raft commit index", st.Commit)
	g("kvmesh_raft_applied_index", "applied index", st.Applied)
	g("kvmesh_raft_is_leader", "1 if this node leads the control plane", isLeader)
	g("kvmesh_ring_epoch", "storage ring epoch", n.ctl.FSM().Ring().Epoch)
	g("kvmesh_actor_activations", "live actor activations here", n.actors.Stats().Active)
	for _, t := range s.Transport {
		fmt.Fprintf(w, "kvmesh_ktp_srtt_ms{peer=%q} %g\n", t.Peer, t.SRTTms)
		fmt.Fprintf(w, "kvmesh_ktp_cwnd_bytes{peer=%q} %d\n", t.Peer, t.CWND)
		fmt.Fprintf(w, "kvmesh_ktp_packets_lost_total{peer=%q} %d\n", t.Peer, t.PacketsLost)
	}
	for _, m := range n.rpcSrv.Stats() {
		fmt.Fprintf(w, "kvmesh_rpc_calls_total{method=%q} %d\n", m.Method, m.Calls)
		fmt.Fprintf(w, "kvmesh_rpc_errors_total{method=%q} %d\n", m.Method, m.Errors)
	}
}
