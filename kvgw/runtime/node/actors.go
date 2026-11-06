package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Sebby1770/kvstore/kvgw/runtime/actor"
	"github.com/Sebby1770/kvstore/kvgw/runtime/control"
)

// registryStore keeps actor state in the control plane's registry. Its
// compare-and-set is linearizable (it is a Raft-replicated FSM), which is
// exactly what the actor runtime's stale-activation fencing needs. (The KV
// data plane's CAS is per-coordinator; with many coordinators it is not a
// safe fence.) Versions are Raft indices, so they only ever grow.
type registryStore struct{ n *Node }

func actorKey(id actor.ID) string { return "actor/" + id.Type + "/" + id.Key }

func (s *registryStore) Load(ctx context.Context, id actor.ID) ([]byte, uint64, error) {
	if err := s.n.ctl.ReadBarrier(ctx); err != nil {
		return nil, 0, err
	}
	e, ok := s.n.ctl.FSM().Get(actorKey(id))
	if !ok {
		return nil, 0, nil
	}
	return e.Value, e.Version, nil
}

func (s *registryStore) Save(ctx context.Context, id actor.ID, data []byte, expect uint64) (uint64, error) {
	res, err := s.n.ctl.Propose(ctx, control.RegCASCmd(actorKey(id), data, expect, s.n.ctl.Now()))
	if err != nil {
		return 0, err
	}
	if errors.Is(res.Err(), control.ErrConflict) {
		return 0, actor.ErrConflict
	}
	if err := res.Err(); err != nil {
		return 0, err
	}
	return res.Entry.Version, nil
}

// counter is a persistent integer: "inc [n]", "dec [n]", "get", "reset".
type counter struct{}

func (counter) Receive(ctx *actor.Context, msg []byte) ([]byte, error) {
	v, _ := strconv.ParseInt(string(ctx.State()), 10, 64)
	fields := strings.Fields(string(msg))
	op := "get"
	if len(fields) > 0 {
		op = fields[0]
	}
	delta := int64(1)
	if len(fields) > 1 {
		d, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("counter: bad amount %q", fields[1])
		}
		delta = d
	}
	switch op {
	case "get":
		return []byte(strconv.FormatInt(v, 10)), nil
	case "inc":
		v += delta
	case "dec":
		v -= delta
	case "reset":
		v = 0
	default:
		return nil, fmt.Errorf("counter: unknown op %q (inc, dec, get, reset)", op)
	}
	ctx.SetState([]byte(strconv.FormatInt(v, 10)))
	if err := ctx.Persist(); err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(v, 10)), nil
}

// ledger appends entries and keeps the latest 20 plus a running count.
type ledger struct{}

type ledgerState struct {
	Count   int      `json:"count"`
	Entries []string `json:"entries"`
}

func (ledger) Receive(ctx *actor.Context, msg []byte) ([]byte, error) {
	var st ledgerState
	if b := ctx.State(); len(b) > 0 {
		json.Unmarshal(b, &st)
	}
	if text := strings.TrimSpace(string(msg)); text != "" && text != "get" {
		st.Count++
		st.Entries = append(st.Entries, fmt.Sprintf("#%d %s (via %s)", st.Count, text, ctx.Node()))
		if len(st.Entries) > 20 {
			st.Entries = st.Entries[len(st.Entries)-20:]
		}
		b, _ := json.Marshal(st)
		ctx.SetState(b)
		if err := ctx.Persist(); err != nil {
			return nil, err
		}
	}
	return json.Marshal(st)
}

func registerBuiltinActors(h *actor.Host) {
	h.Register("counter", func(actor.ID) actor.Actor { return counter{} })
	h.Register("ledger", func(actor.ID) actor.Actor { return ledger{} })
}
