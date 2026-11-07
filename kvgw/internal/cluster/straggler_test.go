package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// hungNode is a replica that stops answering: every call blocks until
// released, then fails, like a client timing out on a storage node whose
// host vanished (over UDP nothing says "connection refused").
type hungNode struct {
	*fakeNode
	release chan struct{}
	once    sync.Once
}

func (h *hungNode) hang() error {
	<-h.release
	return errDown
}

func (h *hungNode) unblock() { h.once.Do(func() { close(h.release) }) }

func (h *hungNode) Get([]byte) (kv.ReadResult, error) { return kv.ReadResult{}, h.hang() }
func (h *hungNode) PutX([]byte, []byte, uint64, uint64) error {
	return h.hang()
}
func (h *hungNode) Del([]byte, uint64) (bool, error) { return false, h.hang() }

// A replica that hangs must not stall quorum operations: they return once
// the quorum is in (plus a short grace), and the stalled write still leaves
// a hint behind for the replica.
func TestHungReplicaDoesNotStallQuorumOperations(t *testing.T) {
	a, b := newFake("a"), newFake("b")
	hung := &hungNode{fakeNode: newFake("c"), release: make(chan struct{})}
	defer hung.unblock()
	c := New(map[string]kv.Node{"a": a, "b": b, "c": hung}, []string{"a", "b", "c"}, 3, 2, 2)

	quick := func(what string, f func() error) {
		t.Helper()
		start := time.Now()
		if err := f(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if took := time.Since(start); took > 10*straggleGrace {
			t.Fatalf("%s took %v with one hung replica", what, took)
		}
	}
	quick("put", func() error { _, err := c.Put([]byte("k"), []byte("v1")); return err })
	quick("get", func() error {
		v, found, meta, err := c.Get([]byte("k"))
		if err == nil && (!found || string(v) != "v1" || len(meta.Responders) != 2) {
			t.Fatalf("get: %q %v %+v", v, found, meta)
		}
		return err
	})
	quick("cas", func() error {
		_, _, meta, _ := c.Get([]byte("k"))
		_, err := c.CAS([]byte("k"), meta.Version, []byte("v2"))
		return err
	})
	quick("del", func() error { _, _, err := c.Del([]byte("k")); return err })

	// The hung replica's calls fail once released; each write leaves a hint.
	hung.unblock()
	deadline := time.Now().Add(2 * time.Second)
	for c.HintsPending()["c"] < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := c.HintsPending()["c"]; n < 3 {
		t.Fatalf("%d hints for the hung replica, want one per write (3)", n)
	}
}

// Healthy replicas are still all heard from: the grace period is far longer
// than an in-memory (or LAN) answer, so read-repair sees a stale replica.
func TestFanOutWaitsForHealthyStragglers(t *testing.T) {
	res := fanOut(3, 1, func(i int) (int, error) {
		time.Sleep(time.Duration(i) * 5 * time.Millisecond)
		return i, nil
	})
	for i, o := range res {
		if o.err != nil || o.v != i {
			t.Fatalf("replica %d: %+v", i, o)
		}
	}
}
