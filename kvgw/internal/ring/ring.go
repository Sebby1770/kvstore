// Package ring maps a key to an ordered preference list of nodes using
// rendezvous (highest-random-weight) hashing.
//
// For each key we score every node by hash(key, node) and take the top RF.
// Unlike hash(key) mod N, this is stable under membership change: adding or
// removing one node only re-homes the keys that node wins (or used to win) —
// roughly 1/N of the keyspace — instead of reshuffling everything.
package ring

import (
	"hash/fnv"
	"sort"
)

func fnv64(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// mix64 is a splitmix64 finalizer — a strong avalanche so that structurally
// similar keys (key-0, key-1, ...) still scatter evenly across nodes.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

type Ring struct {
	addrs []string
}

func New(addrs []string) *Ring {
	cp := make([]string, len(addrs))
	copy(cp, addrs)
	return &Ring{addrs: cp}
}

func (r *Ring) Addrs() []string { return r.addrs }
func (r *Ring) Size() int       { return len(r.addrs) }

// score is the rendezvous weight of (key, node): a well-mixed combination of
// the key hash and the node hash. The avalanche step matters — without it,
// sequential keys skew heavily toward one node.
func score(addr string, key []byte) uint64 {
	hk := fnv64(key)
	hn := fnv64([]byte(addr))
	return mix64(hk ^ (hn * 0x9E3779B97F4A7C15))
}

// Preflist returns up to rf node addresses, highest weight first. Ties break
// on the address so the order is deterministic across processes.
func (r *Ring) Preflist(key []byte, rf int) []string {
	n := len(r.addrs)
	if n == 0 || rf <= 0 {
		return nil
	}
	if rf > n {
		rf = n
	}
	type scored struct {
		addr string
		s    uint64
	}
	all := make([]scored, n)
	for i, a := range r.addrs {
		all[i] = scored{addr: a, s: score(a, key)}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].s != all[j].s {
			return all[i].s > all[j].s
		}
		return all[i].addr < all[j].addr
	})
	out := make([]string, rf)
	for i := 0; i < rf; i++ {
		out[i] = all[i].addr
	}
	return out
}

// Pick returns the single primary node for a key (the head of the preflist).
func (r *Ring) Pick(key []byte) string {
	pl := r.Preflist(key, 1)
	if len(pl) == 0 {
		return ""
	}
	return pl[0]
}
