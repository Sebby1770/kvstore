package offload

import (
	"container/list"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

// Map is state shared between programs and the control plane. Programs
// reach it through helpers; Go code through Lookup, Update and Delete.
// Every map has a lock: a program run holds the locks of all its maps, and
// the Go-side methods take it too, so the two never interleave.
type Map interface {
	Name() string
	KeySize() int
	ValueSize() int
	Lookup(key []byte) ([]byte, bool)
	Update(key, value []byte) error
	Delete(key []byte) error

	base() *mapBase
	// lookupLocked returns the value's storage (programs write through it).
	lookupLocked(key []byte) []byte
	updateLocked(key, value []byte, flags uint64) error
	deleteLocked(key []byte) error
}

// Update flags (as the kernel's BPF_ANY / BPF_NOEXIST / BPF_EXIST).
const (
	UpdateAny     = 0
	UpdateNoExist = 1
	UpdateExist   = 2
)

// Map errors.
var (
	ErrKeySize   = errors.New("offload: wrong key size")
	ErrValueSize = errors.New("offload: wrong value size")
	ErrMapFull   = errors.New("offload: map full")
	ErrNoEntry   = errors.New("offload: no such entry")
	ErrExists    = errors.New("offload: entry exists")
)

var mapIDs atomic.Uint64

type mapBase struct {
	id               uint64 // creation order, for deadlock-free lock ordering
	mu               sync.Mutex
	name             string
	keySize, valSize int
	maxEntries       int
}

func (m *mapBase) Name() string   { return m.name }
func (m *mapBase) KeySize() int   { return m.keySize }
func (m *mapBase) ValueSize() int { return m.valSize }
func (m *mapBase) base() *mapBase { return m }

func lookupCopy(m Map, key []byte) ([]byte, bool) {
	b := m.base()
	b.mu.Lock()
	defer b.mu.Unlock()
	v := m.lookupLocked(key)
	if v == nil {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// ---- array ----

// ArrayMap is a fixed array of values indexed by a 4-byte little-endian key.
type ArrayMap struct {
	mapBase
	vals [][]byte
}

// NewArrayMap makes an array of n zeroed values of valueSize bytes.
func NewArrayMap(name string, valueSize, n int) *ArrayMap {
	m := &ArrayMap{mapBase: mapBase{name: name, keySize: 4, valSize: valueSize, maxEntries: n, id: mapIDs.Add(1)}}
	m.vals = make([][]byte, n)
	for i := range m.vals {
		m.vals[i] = make([]byte, valueSize)
	}
	return m
}

func (m *ArrayMap) index(key []byte) (int, bool) {
	if len(key) != 4 {
		return 0, false
	}
	i := int(binary.LittleEndian.Uint32(key))
	return i, i < len(m.vals)
}

func (m *ArrayMap) lookupLocked(key []byte) []byte {
	if i, ok := m.index(key); ok {
		return m.vals[i]
	}
	return nil
}

func (m *ArrayMap) updateLocked(key, value []byte, flags uint64) error {
	i, ok := m.index(key)
	if !ok {
		return ErrKeySize
	}
	if len(value) != m.valSize {
		return ErrValueSize
	}
	if flags == UpdateNoExist {
		return ErrExists // array slots always exist
	}
	copy(m.vals[i], value)
	return nil
}

func (m *ArrayMap) deleteLocked([]byte) error {
	return errors.New("offload: array entries cannot be deleted")
}

// Lookup copies the value at key.
func (m *ArrayMap) Lookup(key []byte) ([]byte, bool) { return lookupCopy(m, key) }

// Update sets the value at key.
func (m *ArrayMap) Update(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateLocked(key, value, UpdateAny)
}

// Delete is not supported on arrays.
func (m *ArrayMap) Delete(key []byte) error { return m.deleteLocked(key) }

// Set writes v at byte offset off of slot i (a convenience for configs).
func (m *ArrayMap) SetU64(i int, off int, v uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < len(m.vals) && off+8 <= m.valSize {
		binary.LittleEndian.PutUint64(m.vals[i][off:], v)
	}
}

// U64 reads the word at byte offset off of slot i.
func (m *ArrayMap) U64(i int, off int) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < len(m.vals) && off+8 <= m.valSize {
		return binary.LittleEndian.Uint64(m.vals[i][off:])
	}
	return 0
}

// ---- hash ----

// HashMap maps fixed-size keys to fixed-size values. With LRU set, inserting
// into a full map evicts the least recently used entry (the kernel's
// BPF_MAP_TYPE_LRU_HASH) -- what per-flow tables need so a flood of new
// sources cannot fill them for good.
type HashMap struct {
	mapBase
	lru     bool
	entries map[string]*list.Element
	order   *list.List // front = most recently used
}

type hashEntry struct {
	key string
	val []byte
}

// NewHashMap makes a hash map.
func NewHashMap(name string, keySize, valueSize, maxEntries int, lru bool) *HashMap {
	return &HashMap{mapBase: mapBase{name: name, keySize: keySize, valSize: valueSize, maxEntries: maxEntries, id: mapIDs.Add(1)},
		lru: lru, entries: map[string]*list.Element{}, order: list.New()}
}

func (m *HashMap) lookupLocked(key []byte) []byte {
	if len(key) != m.keySize {
		return nil
	}
	e, ok := m.entries[string(key)]
	if !ok {
		return nil
	}
	m.order.MoveToFront(e)
	return e.Value.(*hashEntry).val
}

func (m *HashMap) updateLocked(key, value []byte, flags uint64) error {
	if len(key) != m.keySize {
		return ErrKeySize
	}
	if len(value) != m.valSize {
		return ErrValueSize
	}
	if e, ok := m.entries[string(key)]; ok {
		if flags == UpdateNoExist {
			return ErrExists
		}
		copy(e.Value.(*hashEntry).val, value)
		m.order.MoveToFront(e)
		return nil
	}
	if flags == UpdateExist {
		return ErrNoEntry
	}
	if len(m.entries) >= m.maxEntries {
		if !m.lru {
			return ErrMapFull
		}
		old := m.order.Back()
		delete(m.entries, old.Value.(*hashEntry).key)
		m.order.Remove(old)
	}
	m.entries[string(key)] = m.order.PushFront(&hashEntry{key: string(key), val: append([]byte(nil), value...)})
	return nil
}

func (m *HashMap) deleteLocked(key []byte) error {
	e, ok := m.entries[string(key)]
	if !ok {
		return ErrNoEntry
	}
	delete(m.entries, string(key))
	m.order.Remove(e)
	return nil
}

func (m *HashMap) Lookup(key []byte) ([]byte, bool) { return lookupCopy(m, key) }

func (m *HashMap) Update(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateLocked(key, value, UpdateAny)
}

func (m *HashMap) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteLocked(key)
}

// Len is the number of entries.
func (m *HashMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// ---- redirect targets ----

// AddrMap holds forwarding targets for redirect_map (the kernel's DEVMAP,
// with peer addresses for devices). Programs cannot read it, only redirect
// through it.
type AddrMap struct {
	mapBase
	addrs []net.Addr
}

// NewAddrMap makes a target table with n slots.
func NewAddrMap(name string, n int) *AddrMap {
	return &AddrMap{mapBase: mapBase{name: name, keySize: 4, valSize: 0, maxEntries: n, id: mapIDs.Add(1)}, addrs: make([]net.Addr, n)}
}

// Set installs (or with nil clears) target i.
func (m *AddrMap) Set(i int, a net.Addr) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i >= 0 && i < len(m.addrs) {
		m.addrs[i] = a
	}
}

func (m *AddrMap) target(i uint32) net.Addr {
	if int(i) < len(m.addrs) {
		return m.addrs[i]
	}
	return nil
}

func (m *AddrMap) lookupLocked([]byte) []byte                { return nil }
func (m *AddrMap) updateLocked([]byte, []byte, uint64) error { return ErrValueSize }
func (m *AddrMap) deleteLocked([]byte) error                 { return ErrNoEntry }
func (m *AddrMap) Lookup([]byte) ([]byte, bool)              { return nil, false }
func (m *AddrMap) Update([]byte, []byte) error               { return errors.New("offload: use AddrMap.Set") }
func (m *AddrMap) Delete([]byte) error                       { return errors.New("offload: use AddrMap.Set") }

// ---- user queues ----

// QueueMap delivers redirected packets to Go code (the kernel's XSKMAP,
// feeding AF_XDP sockets): a program steers a packet to queue i, and
// whoever reads Queue(i) gets a copy, bypassing the KTP stack entirely.
type QueueMap struct {
	mapBase
	qs []chan Packet
}

// Packet is a packet a program steered to a user queue.
type Packet struct {
	Data []byte
	From net.Addr
}

// NewQueueMap makes n queues of depth packets each.
func NewQueueMap(name string, n, depth int) *QueueMap {
	m := &QueueMap{mapBase: mapBase{name: name, keySize: 4, maxEntries: n, id: mapIDs.Add(1)}, qs: make([]chan Packet, n)}
	for i := range m.qs {
		m.qs[i] = make(chan Packet, depth)
	}
	return m
}

// Queue is the channel for queue i.
func (m *QueueMap) Queue(i int) <-chan Packet { return m.qs[i] }

func (m *QueueMap) deliver(i uint32, p Packet) bool {
	if int(i) >= len(m.qs) {
		return false
	}
	select {
	case m.qs[i] <- p:
	default: // a full queue drops, like a full AF_XDP fill ring
	}
	return true
}

func (m *QueueMap) lookupLocked([]byte) []byte                { return nil }
func (m *QueueMap) updateLocked([]byte, []byte, uint64) error { return ErrValueSize }
func (m *QueueMap) deleteLocked([]byte) error                 { return ErrNoEntry }
func (m *QueueMap) Lookup([]byte) ([]byte, bool)              { return nil, false }
func (m *QueueMap) Update([]byte, []byte) error {
	return errors.New("offload: queue maps hold no values")
}
func (m *QueueMap) Delete([]byte) error { return errors.New("offload: queue maps hold no values") }

// U32 encodes an array-map key.
func U32(i uint32) []byte { return binary.LittleEndian.AppendUint32(nil, i) }

// U64 encodes an 8-byte key or value.
func U64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }
