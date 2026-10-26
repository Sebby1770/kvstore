package node

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
	"github.com/Sebby1770/kvstore/kvgw/runtime/actor"
	"github.com/Sebby1770/kvstore/kvgw/runtime/raft"
)

type (
	raftConf   = raft.ConfState
	raftStatus = raft.Status
)

// raftTransport carries Raft messages over RPC: one queue and one sender
// goroutine per peer, batching whatever accumulated while the previous call
// was in flight. Queues are bounded and drop when full -- Raft retransmits --
// so a dead peer can never block the control-plane loop.
type raftTransport struct {
	n      *Node
	mu     sync.Mutex
	queues map[string]chan raft.Message
	done   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func newRaftTransport(n *Node) *raftTransport {
	return &raftTransport{n: n, queues: map[string]chan raft.Message{}, done: make(chan struct{})}
}

func (t *raftTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		t.mu.Lock()
		q, ok := t.queues[m.To]
		if !ok {
			select {
			case <-t.done:
				t.mu.Unlock()
				return
			default:
			}
			q = make(chan raft.Message, 4096)
			t.queues[m.To] = q
			t.wg.Add(1)
			go t.sender(m.To, q)
		}
		t.mu.Unlock()
		select {
		case q <- m:
		default:
			if m.Type == raft.MsgSnap {
				go t.n.ctl.ReportSnapshot(m.To, false)
			}
		}
	}
}

func (t *raftTransport) sender(to string, q chan raft.Message) {
	defer t.wg.Done()
	for {
		var batch []raft.Message
		select {
		case m := <-q:
			batch = append(batch, m)
		case <-t.done:
			return
		}
	drain:
		for len(batch) < 512 {
			select {
			case m := <-q:
				batch = append(batch, m)
			default:
				break drain
			}
		}
		t.deliver(to, batch)
	}
}

func (t *raftTransport) deliver(to string, batch []raft.Message) {
	n := t.n
	addr, ok := n.peerAddr(to)
	snap := false
	for _, m := range batch {
		if m.Type == raft.MsgSnap {
			snap = true
		}
	}
	report := func(ok bool) {
		if !ok {
			n.ctl.ReportUnreachable(to)
		}
		if snap {
			n.ctl.ReportSnapshot(to, ok)
		}
	}
	if !ok {
		report(false)
		return
	}
	var body []byte
	for _, m := range batch {
		b := m.Marshal()
		body = binary.AppendUvarint(body, uint64(len(b)))
		body = append(body, b...)
	}
	ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
	defer cancel()
	_, err := n.rpcCli.Call(ctx, addr, "raft.step", body)
	report(err == nil)
}

func (t *raftTransport) close() {
	t.once.Do(func() { close(t.done) })
	t.wg.Wait()
}

// registerRPC installs the node's RPC methods.
func (n *Node) registerRPC() {
	n.rpcSrv.Register("raft.step", func(ctx context.Context, body []byte) ([]byte, error) {
		for len(body) > 0 {
			l, k := binary.Uvarint(body)
			if k <= 0 || l > uint64(len(body)-k) {
				return nil, rpc.Errorf(rpc.InvalidArgument, "bad raft batch")
			}
			m, err := raft.UnmarshalMessage(body[k : k+int(l)])
			body = body[k+int(l):]
			if err != nil {
				return nil, rpc.Errorf(rpc.InvalidArgument, "bad raft message: %v", err)
			}
			n.ctl.Step(m)
		}
		return nil, nil
	})
	n.rpcSrv.Register("actor.deliver", rpc.Unary(func(ctx context.Context, env *actor.Envelope) (*actorReply, error) {
		data, err := n.actors.HandleRemote(ctx, *env)
		if err != nil {
			return &actorReply{Err: err.Error(), Code: actorErrCode(err)}, nil
		}
		return &actorReply{Data: data}, nil
	}))
	n.rpcSrv.Register("control.join", func(ctx context.Context, body []byte) ([]byte, error) {
		name, addr, ok := strings.Cut(string(body), "=")
		if !ok || name == "" || addr == "" {
			return nil, rpc.Errorf(rpc.InvalidArgument, "join wants name=addr")
		}
		n.mu.Lock()
		n.peers[name] = addr
		n.mu.Unlock()
		if !n.ctl.IsLeader() {
			lead := n.ctl.Leader()
			if la, ok := n.peerAddr(lead); ok && lead != "" {
				// Forward to the leader on the joiner's behalf.
				fctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				return n.rpcCli.Call(fctx, la, "control.join", body)
			}
			return nil, rpc.Errorf(rpc.Unavailable, "no leader yet")
		}
		st := n.ctl.Status()
		for _, v := range append(append([]string{}, st.Voters...), st.Learners...) {
			if v == name {
				return nil, nil // already a member
			}
		}
		jctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := n.ctl.AddLearner(jctx, name); err != nil {
			return nil, rpc.Errorf(rpc.Unavailable, "add learner: %v", err)
		}
		n.events.add("raft.join", name+" joined as a learner")
		return nil, nil
	})
}

type actorReply struct {
	Data []byte `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
	Code string `json:"code,omitempty"`
}

var actorSentinels = map[string]error{
	"stale": actor.ErrStaleActivation, "mailbox_full": actor.ErrMailboxFull, "hops": actor.ErrTooManyHops,
	"unknown_type": actor.ErrUnknownType, "no_owner": actor.ErrNoOwner, "closed": actor.ErrClosed,
	"panicked": actor.ErrActorPanicked, "backoff": actor.ErrBackoff,
}

func actorErrCode(err error) string {
	for code, s := range actorSentinels {
		if errors.Is(err, s) {
			return code
		}
	}
	return ""
}

// actorTransport delivers envelopes to the owner node over RPC.
type actorTransport struct{ n *Node }

func (t *actorTransport) Deliver(ctx context.Context, node string, env actor.Envelope) ([]byte, error) {
	addr, ok := t.n.peerAddr(node)
	if !ok {
		return nil, actor.ErrNoOwner
	}
	r, err := rpc.Invoke[actor.Envelope, actorReply](ctx, t.n.rpcCli, addr, "actor.deliver", &env)
	if err != nil {
		return nil, err
	}
	if r.Err != "" {
		if s, ok := actorSentinels[r.Code]; ok {
			return nil, &wrapped{msg: r.Err, base: s}
		}
		return nil, &actor.RemoteError{Msg: r.Err}
	}
	return r.Data, nil
}

type wrapped struct {
	msg  string
	base error
}

func (w *wrapped) Error() string { return w.msg }
func (w *wrapped) Unwrap() error { return w.base }
