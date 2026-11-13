package fabric

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
)

// Index is a hash index that lives entirely in one registered region and
// is used entirely through one-sided verbs: the owner formats the region
// once and then runs no code for it at all. Any node holding the Remote can
// look keys up and insert them, concurrently, lock-free.
//
// Layout (all words little-endian):
//
//	 0  magic "K_FABIDX"
//	 8  bucket count
//	16  allocation cursor (offset of the next free byte; FETCH_ADD)
//	24  capacity (region size)
//	32  buckets: one word per bucket, the offset of the newest entry (0 = none)
//	 …  entries, each 8-byte aligned:
//	      0 next entry offset   8 key hash   16 key length (u32)   20 value length (u32)
//	     24 key bytes, then value bytes
//
// Insert: FETCH_ADD the cursor to allocate, WRITE the entry pointing at the
// current bucket head, then CAS the bucket head from that head to the new
// entry -- retrying (rewriting only the next word) if another writer got in
// first. The entry is complete before the CAS publishes it, so readers never
// see a partial one. Lookup: READ the bucket head, then CHASE down the chain
// to the first entry whose hash matches, which returns the entry itself:
// two round trips regardless of chain length. New versions go in front, so
// the first match is the newest; a tombstone value deletes.
//
// Space is never reclaimed (an append-only log in a fixed arena, as in many
// RDMA key-value stores); a full index reports ErrIndexFull.
type Index struct {
	n        *Node
	rr       Remote
	buckets  uint64
	capacity uint64
}

const (
	indexMagic   = 0x5844494241465f4b // "K_FABIDX" read little-endian
	indexHeader  = 32
	entryHeader  = 24
	maxChainHops = 256
	tombstone    = ^uint32(0)
	// MaxEntry bounds key plus value so an entry fits one CHASE.
	MaxEntry = MaxTransfer - entryHeader
)

// Errors.
var (
	ErrIndexFull = errors.New("fabric: index region is full")
	ErrNotIndex  = errors.New("fabric: region does not hold an index")
	ErrEntrySize = errors.New("fabric: key plus value too large for an index entry")
	ErrNotFound  = errors.New("fabric: key not found")
)

// FormatIndex lays an empty index with the given bucket count out in r (the
// owner does this once, locally).
func FormatIndex(r *Region, buckets int) error {
	if buckets <= 0 {
		return errors.New("fabric: need at least one bucket")
	}
	start := uint64(indexHeader + 8*buckets)
	if start >= r.Size() {
		return ErrIndexFull
	}
	b, _ := r.Bytes(0, start)
	clear(b)
	r.Store64(8, uint64(buckets))
	r.Store64(16, start)
	r.Store64(24, r.Size())
	return r.Store64(0, indexMagic)
}

// OpenIndex reads an index's header through the fabric.
func OpenIndex(ctx context.Context, n *Node, rr Remote) (*Index, error) {
	hdr, err := n.Read(ctx, rr, 0, indexHeader)
	if err != nil {
		return nil, err
	}
	if le.Uint64(hdr) != indexMagic {
		return nil, ErrNotIndex
	}
	return &Index{n: n, rr: rr, buckets: le.Uint64(hdr[8:]), capacity: le.Uint64(hdr[24:])}, nil
}

func keyHash(k []byte) uint64 {
	h := fnv.New64a()
	h.Write(k)
	return h.Sum64()
}

func (x *Index) bucketOff(h uint64) uint64 { return indexHeader + 8*(h%x.buckets) }

// Put inserts (or shadows) key -> value.
func (x *Index) Put(ctx context.Context, key, value []byte) error {
	return x.insert(ctx, key, value, uint32(len(value)))
}

// Delete hides key behind a tombstone.
func (x *Index) Delete(ctx context.Context, key []byte) error {
	return x.insert(ctx, key, nil, tombstone)
}

func (x *Index) insert(ctx context.Context, key, value []byte, vlen uint32) error {
	if len(key)+len(value) > MaxEntry {
		return ErrEntrySize
	}
	size := uint64(entryHeader+len(key)+len(value)+7) &^ 7
	off, err := x.n.FetchAdd(ctx, x.rr, 16, size)
	if err != nil {
		return err
	}
	if off+size > x.capacity {
		return ErrIndexFull
	}
	h := keyHash(key)
	bucket := x.bucketOff(h)
	head, err := x.n.Load64(ctx, x.rr, bucket)
	if err != nil {
		return err
	}
	entry := make([]byte, entryHeader+len(key)+len(value))
	le.PutUint64(entry[0:], head)
	le.PutUint64(entry[8:], h)
	le.PutUint32(entry[16:], uint32(len(key)))
	le.PutUint32(entry[20:], vlen)
	copy(entry[entryHeader:], key)
	copy(entry[entryHeader+len(key):], value)
	if err := x.n.Write(ctx, x.rr, off, entry); err != nil {
		return err
	}
	for {
		prev, ok, err := x.n.CAS(ctx, x.rr, bucket, head, off)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Another writer published first: chain behind its entry instead.
		head = prev
		var w [8]byte
		le.PutUint64(w[:], head)
		if err := x.n.Write(ctx, x.rr, off, w[:]); err != nil {
			return err
		}
	}
}

// Get returns key's newest value.
func (x *Index) Get(ctx context.Context, key []byte) ([]byte, error) {
	h := keyHash(key)
	at, err := x.n.Load64(ctx, x.rr, x.bucketOff(h))
	if err != nil {
		return nil, err
	}
	for hops := 0; at != 0 && hops < maxChainHops; hops++ {
		res, err := x.n.Chase(ctx, x.rr, at, ChaseSpec{LinkOff: 0, KeyOff: 8, Key: h, Match: true,
			MaxHops: maxChainHops, ReadLen: MaxTransfer})
		if err != nil {
			return nil, err
		}
		if !res.Found || len(res.Data) < entryHeader {
			return nil, ErrNotFound
		}
		e := res.Data
		klen, vlen := le.Uint32(e[16:]), le.Uint32(e[20:])
		end := uint64(entryHeader) + uint64(klen)
		if vlen != tombstone {
			end += uint64(vlen)
		}
		if end > uint64(len(e)) {
			return nil, ErrMalformed
		}
		if bytes.Equal(e[entryHeader:entryHeader+klen], key) {
			if vlen == tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e[entryHeader+klen:end]...), nil
		}
		at = le.Uint64(e[0:]) // a hash collision: keep walking past it
	}
	return nil, ErrNotFound
}
