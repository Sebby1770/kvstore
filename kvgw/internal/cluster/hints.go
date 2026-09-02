package cluster

import (
	"sync/atomic"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

type hintOp int

const (
	hintPut hintOp = iota
	hintDel
)

type hint struct {
	key     []byte
	value   []byte
	version uint64
	expires uint64
	op      hintOp
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func (c *Cluster) storeHint(addr string, h hint) {
	h.key = copyBytes(h.key)
	h.value = copyBytes(h.value)
	c.hintsMu.Lock()
	c.hints[addr] = append(c.hints[addr], h)
	c.hintsMu.Unlock()
	atomic.AddUint64(&c.mHintsStored, 1)
}

// ReplayHints tries to deliver every stored hint. Returns how many succeeded.
func (c *Cluster) ReplayHints() int {
	m := c.snap()
	n := 0
	for _, addr := range m.addrs {
		n += c.replayHints(addr)
	}
	return n
}

func (c *Cluster) replayHints(addr string) int {
	m := c.snap()
	node := m.nodes[addr]
	if node == nil {
		return 0
	}
	c.hintsMu.Lock()
	q := c.hints[addr]
	c.hints[addr] = nil
	c.hintsMu.Unlock()
	if len(q) == 0 {
		return 0
	}

	var leftover []hint
	replayed := 0
	for _, h := range q {
		var err error
		switch h.op {
		case hintDel:
			_, err = node.Del(h.key, h.version)
		default:
			err = node.PutX(h.key, h.value, h.version, h.expires)
		}
		if err != nil {
			leftover = append(leftover, h)
			continue
		}
		replayed++
	}
	if len(leftover) > 0 {
		c.hintsMu.Lock()
		c.hints[addr] = append(leftover, c.hints[addr]...)
		c.hintsMu.Unlock()
	}
	if replayed > 0 {
		atomic.AddUint64(&c.mHintsReplayed, uint64(replayed))
	}
	return replayed
}

func (c *Cluster) dropHints(addr string) {
	c.hintsMu.Lock()
	delete(c.hints, addr)
	c.hintsMu.Unlock()
}

func putOrHint(c *Cluster, addr string, node kv.Node, key, val []byte, version, expires uint64) error {
	if c.IsDown(addr) {
		c.storeHint(addr, hint{op: hintPut, key: key, value: val, version: version, expires: expires})
		return errDownSentinel
	}
	if err := node.PutX(key, val, version, expires); err != nil {
		c.storeHint(addr, hint{op: hintPut, key: key, value: val, version: version, expires: expires})
		return err
	}
	return nil
}

func delOrHint(c *Cluster, addr string, node kv.Node, key []byte, version uint64) (bool, error) {
	if c.IsDown(addr) {
		c.storeHint(addr, hint{op: hintDel, key: key, version: version})
		return false, errDownSentinel
	}
	existed, err := node.Del(key, version)
	if err != nil {
		c.storeHint(addr, hint{op: hintDel, key: key, version: version})
		return false, err
	}
	return existed, nil
}
