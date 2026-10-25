package simnet

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Wildcard matches any address in SetLink, Block and LinkStats.
const Wildcard = "*"

// DefaultQueueLimit is the default per-conn receive queue length in packets.
const DefaultQueueLimit = 1024

// firstEphemeralPort is where automatic port allocation starts, mirroring the
// IANA dynamic range.
const firstEphemeralPort = 49152

var (
	errAddrInUse      = errors.New("address already in use")
	errInvalidAddr    = errors.New("invalid simnet address")
	errNetworkClosed  = errors.New("simnet: network closed")
	errMissingAddress = errors.New("missing address")
)

// Addr is a simnet address. Any non-empty string without '*' is valid; the
// conventional form is "host:port".
type Addr string

// Network returns "sim".
func (Addr) Network() string { return "sim" }

func (a Addr) String() string { return string(a) }

// Resolve validates addr and returns it as a net.Addr usable with WriteTo.
func Resolve(addr string) (net.Addr, error) {
	if err := validAddr(addr); err != nil {
		return nil, &net.AddrError{Err: err.Error(), Addr: addr}
	}
	return Addr(addr), nil
}

// Resolve is the method form of the package-level Resolve, so a Network can
// stand in for a resolver interface.
func (n *Network) Resolve(addr string) (net.Addr, error) { return Resolve(addr) }

func validAddr(addr string) error {
	if addr == "" || strings.Contains(addr, Wildcard) {
		return errInvalidAddr
	}
	return nil
}

type pair struct{ from, to string }

// Network is a simulated packet network. It is safe for concurrent use.
type Network struct {
	mu sync.Mutex

	rng        *rand.Rand
	conns      map[string]*PacketConn
	nextPort   int
	queueLimit int
	closed     bool

	defaultLink Link
	links       map[pair]Link

	// Topology faults. groups is nil when the network is not partitioned;
	// otherwise it maps each listed address to its group index.
	blocked  map[pair]bool
	isolated map[string]bool
	groups   map[string]int

	paths map[pair]*path
	stats Stats
	seq   uint64
}

// New returns an empty, fully connected network whose randomness is derived
// from seed.
func New(seed int64) *Network {
	s := uint64(seed)
	return &Network{
		rng:        rand.New(rand.NewPCG(s, s^0x9e3779b97f4a7c15)),
		conns:      make(map[string]*PacketConn),
		nextPort:   firstEphemeralPort,
		queueLimit: DefaultQueueLimit,
		links:      make(map[pair]Link),
		blocked:    make(map[pair]bool),
		isolated:   make(map[string]bool),
		paths:      make(map[pair]*path),
	}
}

// Listen opens a PacketConn at addr. An empty addr, or one ending in ":0",
// gets a free port assigned (e.g. "host:49152"); the result's LocalAddr says
// which.
func (n *Network) Listen(addr string) (*PacketConn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, &net.OpError{Op: "listen", Net: "sim", Addr: Addr(addr), Err: errNetworkClosed}
	}
	if addr == "" || strings.HasSuffix(addr, ":0") {
		host := strings.TrimSuffix(addr, ":0")
		if host == "" {
			host = "sim"
		}
		for {
			cand := host + ":" + strconv.Itoa(n.nextPort)
			n.nextPort++
			if _, used := n.conns[cand]; !used {
				addr = cand
				break
			}
		}
	}
	if err := validAddr(addr); err != nil {
		return nil, &net.OpError{Op: "listen", Net: "sim", Addr: Addr(addr), Err: err}
	}
	if _, used := n.conns[addr]; used {
		return nil, &net.OpError{Op: "listen", Net: "sim", Addr: Addr(addr), Err: errAddrInUse}
	}
	c := &PacketConn{
		net:    n,
		addr:   addr,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
		rdCh:   make(chan struct{}),
	}
	n.conns[addr] = c
	return c, nil
}

// Close closes every conn and cancels all in-flight packets. Later Listen
// calls fail.
func (n *Network) Close() error {
	n.mu.Lock()
	conns := make([]*PacketConn, 0, len(n.conns))
	for _, c := range n.conns {
		conns = append(conns, c)
	}
	n.closed = true
	for _, p := range n.paths {
		if p.timer != nil {
			p.timer.Stop()
			p.timer = nil
		}
		p.q = nil
	}
	n.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return nil
}

// Addrs returns the addresses with an open conn, sorted.
func (n *Network) Addrs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.conns))
	for a := range n.conns {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// SetQueueLimit sets the receive queue length (packets) of every conn,
// existing and future. Values < 1 restore DefaultQueueLimit. Packets already
// queued beyond a lowered limit are kept.
func (n *Network) SetQueueLimit(packets int) {
	if packets < 1 {
		packets = DefaultQueueLimit
	}
	n.mu.Lock()
	n.queueLimit = packets
	n.mu.Unlock()
}

// SetDefaultLink sets the link used for every direction without a more
// specific SetLink.
func (n *Network) SetDefaultLink(l Link) {
	n.mu.Lock()
	n.defaultLink = l
	n.mu.Unlock()
}

// SetLink sets the link for packets from → to. Either side may be Wildcard.
// Lookup prefers (from,to), then (from,*), then (*,to), then the default;
// SetLink("*","*",l) is the same as SetDefaultLink(l).
func (n *Network) SetLink(from, to string, l Link) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if from == Wildcard && to == Wildcard {
		n.defaultLink = l
		return
	}
	n.links[pair{from, to}] = l
}

// ClearLink removes a SetLink rule, falling back to less specific ones.
func (n *Network) ClearLink(from, to string) {
	n.mu.Lock()
	delete(n.links, pair{from, to})
	n.mu.Unlock()
}

// Link returns the effective link for packets from → to.
func (n *Network) Link(from, to string) Link {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.linkLocked(from, to)
}

func (n *Network) linkLocked(from, to string) Link {
	if l, ok := n.links[pair{from, to}]; ok {
		return l
	}
	if l, ok := n.links[pair{from, Wildcard}]; ok {
		return l
	}
	if l, ok := n.links[pair{Wildcard, to}]; ok {
		return l
	}
	return n.defaultLink
}

// Partition splits the network: addresses in different groups cannot reach
// each other in either direction. Addresses not named in any group together
// form one more implicit group, so Partition([]string{"a"}) cuts "a" off from
// everyone else. A new Partition replaces the previous one; Block and Isolate
// rules are independent of it.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.groups = make(map[string]int)
	for i, g := range groups {
		for _, a := range g {
			n.groups[a] = i
		}
	}
}

// Heal removes the partition, every Block and every Isolate: the network is
// fully connected again (links keep their fault parameters).
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.groups = nil
	n.blocked = make(map[pair]bool)
	n.isolated = make(map[string]bool)
}

// Block drops packets from → to (one direction only). Either side may be
// Wildcard.
func (n *Network) Block(from, to string) {
	n.mu.Lock()
	n.blocked[pair{from, to}] = true
	n.mu.Unlock()
}

// Unblock removes a Block rule.
func (n *Network) Unblock(from, to string) {
	n.mu.Lock()
	delete(n.blocked, pair{from, to})
	n.mu.Unlock()
}

// Isolate cuts addr off from every other address in both directions.
// (Packets a conn sends to itself still arrive.)
func (n *Network) Isolate(addr string) {
	n.mu.Lock()
	n.isolated[addr] = true
	n.mu.Unlock()
}

// Unisolate reverses Isolate.
func (n *Network) Unisolate(addr string) {
	n.mu.Lock()
	delete(n.isolated, addr)
	n.mu.Unlock()
}

// Reachable reports whether the topology currently lets packets flow from →
// to (ignoring random loss).
func (n *Network) Reachable(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.cutLocked(from, to)
}

func (n *Network) cutLocked(from, to string) bool {
	if from == to {
		return false
	}
	if n.isolated[from] || n.isolated[to] {
		return true
	}
	if n.blocked[pair{from, to}] || n.blocked[pair{from, Wildcard}] || n.blocked[pair{Wildcard, to}] || n.blocked[pair{Wildcard, Wildcard}] {
		return true
	}
	if n.groups != nil {
		gf, ok := n.groups[from]
		if !ok {
			gf = -1
		}
		gt, ok := n.groups[to]
		if !ok {
			gt = -1
		}
		if gf != gt {
			return true
		}
	}
	return false
}

// Stats returns network-wide counters.
func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// LinkStats returns the counters for packets from → to. Either side may be
// Wildcard to aggregate over all matching directions.
func (n *Network) LinkStats(from, to string) Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	var s Stats
	for k, p := range n.paths {
		if (from == Wildcard || from == k.from) && (to == Wildcard || to == k.to) {
			s.add(&p.stats)
		}
	}
	return s
}

// ResetStats zeroes every counter.
func (n *Network) ResetStats() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats = Stats{}
	for _, p := range n.paths {
		p.stats = Stats{}
	}
}

func (n *Network) pathLocked(from, to string) *path {
	k := pair{from, to}
	p := n.paths[k]
	if p == nil {
		p = &path{net: n, key: k}
		n.paths[k] = p
	}
	return p
}

// count applies f to the global and the per-path counters.
func (n *Network) count(p *path, f func(*Stats)) {
	f(&n.stats)
	f(&p.stats)
}

// sendLocked is the transmit side of WriteTo: decide the packet's fate and
// schedule the surviving copies on the path's FIFO.
func (n *Network) sendLocked(from, to string, b []byte) {
	now := time.Now()
	p := n.pathLocked(from, to)
	size := len(b)
	n.count(p, func(s *Stats) { s.Sent++; s.BytesSent += uint64(size) })
	if n.cutLocked(from, to) {
		n.count(p, func(s *Stats) { s.DroppedPartition++ })
		return
	}
	l := n.linkLocked(from, to)
	if l.MTU > 0 && size > l.MTU {
		n.count(p, func(s *Stats) { s.DroppedMTU++ })
		return
	}
	copies, corrupt := l.Fate(n.rng, size)
	if len(copies) == 0 {
		n.count(p, func(s *Stats) { s.DroppedLoss++ })
		return
	}
	depart := now
	if l.Bandwidth > 0 {
		start := now
		if p.busyUntil.After(start) {
			start = p.busyUntil
		}
		if l.QueueBytes > 0 && start.Sub(now) > l.TransmitTime(l.QueueBytes) {
			n.count(p, func(s *Stats) { s.DroppedLinkQueue++ })
			return
		}
		p.busyUntil = start.Add(l.TransmitTime(size))
		depart = p.busyUntil
	}
	data := append([]byte(nil), b...)
	if corrupt {
		CorruptPacket(n.rng, data)
		n.count(p, func(s *Stats) { s.Corrupted++ })
	}
	if len(copies) > 1 {
		n.count(p, func(s *Stats) { s.Duplicated += uint64(len(copies) - 1) })
	}
	for _, d := range copies {
		at := depart.Add(d)
		if at.Before(p.lastAt) {
			n.count(p, func(s *Stats) { s.Reordered++ })
		} else {
			p.lastAt = at
		}
		n.seq++
		heap.Push(&p.q, &packet{at: at, seq: n.seq, data: data})
	}
	p.arm(now)
}

// deliverLocked hands a packet whose time has come to the destination conn.
func (n *Network) deliverLocked(p *path, pkt *packet) {
	if n.cutLocked(p.key.from, p.key.to) {
		n.count(p, func(s *Stats) { s.DroppedPartition++ })
		return
	}
	c := n.conns[p.key.to]
	if c == nil || c.closed {
		n.count(p, func(s *Stats) { s.DroppedUnreachable++ })
		return
	}
	if len(c.queue) >= n.queueLimit {
		n.count(p, func(s *Stats) { s.DroppedQueueFull++ })
		return
	}
	c.queue = append(c.queue, rpacket{from: p.key.from, data: pkt.data})
	size := len(pkt.data)
	n.count(p, func(s *Stats) { s.Delivered++; s.BytesDelivered += uint64(size) })
	c.signal()
}

// path is one direction between two concrete addresses: a FIFO of packets in
// flight ordered by arrival time, the link's transmit backlog, and counters.
// One timer per path (armed for the earliest arrival) instead of one per
// packet keeps same-instant packets in send order, which per-packet timers
// would not guarantee.
type path struct {
	net       *Network
	key       pair
	q         packetHeap
	timer     *time.Timer
	timerAt   time.Time
	gen       uint64
	busyUntil time.Time // when the link finishes transmitting its backlog
	lastAt    time.Time // latest scheduled arrival, to count reordering
	stats     Stats
}

func (p *path) arm(now time.Time) {
	if len(p.q) == 0 || p.net.closed {
		return
	}
	at := p.q[0].at
	if p.timer != nil && !p.timerAt.After(at) {
		return // already armed early enough
	}
	if p.timer != nil {
		p.timer.Stop()
	}
	p.gen++
	gen := p.gen
	p.timerAt = at
	p.timer = time.AfterFunc(at.Sub(now), func() { p.fire(gen) })
}

func (p *path) fire(gen uint64) {
	n := p.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if gen == p.gen {
		p.timer = nil
	}
	if n.closed {
		return
	}
	now := time.Now()
	for len(p.q) > 0 && !p.q[0].at.After(now) {
		pkt := heap.Pop(&p.q).(*packet)
		n.deliverLocked(p, pkt)
	}
	p.arm(now)
}

type packet struct {
	at   time.Time
	seq  uint64
	data []byte
}

// packetHeap orders by arrival time, then send order.
type packetHeap []*packet

func (h packetHeap) Len() int { return len(h) }
func (h packetHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].seq < h[j].seq
	}
	return h[i].at.Before(h[j].at)
}
func (h packetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *packetHeap) Push(x any)   { *h = append(*h, x.(*packet)) }
func (h *packetHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}

// Stats counts packet fates. Every sent packet ends up in exactly one of
// Delivered or a Dropped* counter, except that duplicates add extra deliveries
// (Duplicated) and packets still in flight are in neither yet.
type Stats struct {
	Sent           uint64 // packets written
	BytesSent      uint64
	Delivered      uint64 // copies placed in a receive queue
	BytesDelivered uint64

	Duplicated uint64 // extra copies created
	Reordered  uint64 // copies scheduled to arrive before an earlier-sent one
	Corrupted  uint64 // packets with a flipped bit

	DroppedLoss        uint64 // random loss
	DroppedPartition   uint64 // partition, block or isolation (at send or delivery)
	DroppedMTU         uint64 // larger than the link MTU
	DroppedLinkQueue   uint64 // link transmit backlog full (QueueBytes)
	DroppedQueueFull   uint64 // receiver's queue full
	DroppedUnreachable uint64 // nobody listening at the destination
}

// Dropped is the total of all drop reasons.
func (s Stats) Dropped() uint64 {
	return s.DroppedLoss + s.DroppedPartition + s.DroppedMTU + s.DroppedLinkQueue +
		s.DroppedQueueFull + s.DroppedUnreachable
}

func (s Stats) String() string {
	return fmt.Sprintf("sent=%d delivered=%d dup=%d reorder=%d corrupt=%d dropped=%d (loss=%d partition=%d mtu=%d linkq=%d rxq=%d unreachable=%d)",
		s.Sent, s.Delivered, s.Duplicated, s.Reordered, s.Corrupted, s.Dropped(),
		s.DroppedLoss, s.DroppedPartition, s.DroppedMTU, s.DroppedLinkQueue, s.DroppedQueueFull, s.DroppedUnreachable)
}

func (s *Stats) add(o *Stats) {
	s.Sent += o.Sent
	s.BytesSent += o.BytesSent
	s.Delivered += o.Delivered
	s.BytesDelivered += o.BytesDelivered
	s.Duplicated += o.Duplicated
	s.Reordered += o.Reordered
	s.Corrupted += o.Corrupted
	s.DroppedLoss += o.DroppedLoss
	s.DroppedPartition += o.DroppedPartition
	s.DroppedMTU += o.DroppedMTU
	s.DroppedLinkQueue += o.DroppedLinkQueue
	s.DroppedQueueFull += o.DroppedQueueFull
	s.DroppedUnreachable += o.DroppedUnreachable
}
