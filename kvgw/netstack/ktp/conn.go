package ktp

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// State is a connection's lifecycle stage.
type State int

const (
	StateHandshake   State = iota // keys not yet agreed (client) or peer not yet authenticated (server)
	StateEstablished              // streams and datagrams flow
	StateClosing                  // we sent CONNECTION_CLOSE; waiting out the drain period
	StateDraining                 // the peer closed; waiting out the drain period silently
	StateClosed                   // all state may be discarded
)

func (s State) String() string {
	return [...]string{"handshake", "established", "closing", "draining", "closed"}[s]
}

// EventType classifies events from PollEvent.
type EventType int

const (
	EventConnected      EventType = iota // handshake complete; streams may be opened
	EventStreamOpened                    // the peer opened a stream (see AcceptStream)
	EventStreamReadable                  // data, FIN, or a reset is waiting on a stream
	EventStreamWritable                  // send-buffer space or credit freed up
	EventStreamReset                     // the peer reset a stream's send half
	EventStopSending                     // the peer asked us to stop sending
	EventDatagram                        // a datagram arrived (see RecvDatagram)
	EventClosed                          // the connection is closing or closed (see Err)
)

// Event is one notification for the driver.
type Event struct {
	Type   EventType
	Stream StreamID
	Err    error
}

// Transport error codes carried in CONNECTION_CLOSE.
const (
	CodeNoError           = 0x00
	CodeInternal          = 0x01
	CodeFlowControl       = 0x03
	CodeStreamLimit       = 0x04
	CodeStreamState       = 0x05
	CodeFinalSize         = 0x06
	CodeProtocolViolation = 0x0a
	CodeHandshakeFailed   = 0x10
	CodePeerRejected      = 0x11
	// CodeApplication and above are free for applications.
	CodeApplication = 0x100
)

// CloseError describes why a connection closed via CONNECTION_CLOSE.
type CloseError struct {
	Code   uint64
	Reason string
	Remote bool // the peer closed
}

func (e *CloseError) Error() string {
	who := "locally"
	if e.Remote {
		who = "by peer"
	}
	if e.Reason == "" {
		return fmt.Sprintf("ktp: connection closed %s (code 0x%x)", who, e.Code)
	}
	return fmt.Sprintf("ktp: connection closed %s (code 0x%x): %s", who, e.Code, e.Reason)
}

// Connection-level errors.
var (
	ErrIdleTimeout        = errors.New("ktp: idle timeout")
	ErrHandshakeTimeout   = errors.New("ktp: handshake timeout")
	ErrStatelessReset     = errors.New("ktp: peer has no state for this connection (stateless reset)")
	ErrClusterKeyMismatch = errors.New("ktp: handshake failed: cluster key mismatch")
	ErrNotEstablished     = errors.New("ktp: connection not established")
	ErrClosed             = errors.New("ktp: connection closed")
	ErrDatagramTooLarge   = errors.New("ktp: datagram too large for one packet")

	errFlowControl = &CloseError{Code: CodeFlowControl, Reason: "flow control limit exceeded"}
	errFinalSize   = &CloseError{Code: CodeFinalSize, Reason: "data beyond final size"}
	errStreamLimit = &CloseError{Code: CodeStreamLimit, Reason: "stream limit exceeded"}
	errStreamState = &CloseError{Code: CodeStreamState, Reason: "frame for a stream in the wrong state"}
	errViolation   = &CloseError{Code: CodeProtocolViolation, Reason: "protocol violation"}
)

// Stats is a snapshot of a connection's transport counters.
type Stats struct {
	State            string        `json:"state"`
	Congestion       string        `json:"congestion"`
	SRTT             time.Duration `json:"srtt"`
	MinRTT           time.Duration `json:"min_rtt"`
	RTTVar           time.Duration `json:"rttvar"`
	LatestRTT        time.Duration `json:"latest_rtt"`
	CWND             int           `json:"cwnd"`
	SSThresh         int           `json:"ssthresh"`
	BytesInFlight    int           `json:"bytes_in_flight"`
	PacketsSent      uint64        `json:"packets_sent"`
	PacketsReceived  uint64        `json:"packets_received"`
	PacketsLost      uint64        `json:"packets_lost"`
	PacketsDropped   uint64        `json:"packets_dropped"` // undecryptable, duplicate, malformed
	BytesSent        uint64        `json:"bytes_sent"`
	BytesReceived    uint64        `json:"bytes_received"`
	StreamBytesRetx  uint64        `json:"stream_bytes_retransmitted"`
	PTOCount         uint64        `json:"pto_count"`
	CongestionEvents uint64        `json:"congestion_events"`
	StreamsOpen      int           `json:"streams_open"`
	Handshake        time.Duration `json:"handshake"`
}

// Conn is one KTP connection as a pure state machine: feed it datagrams with
// HandlePacket, drain datagrams to send with Poll, honour Timeout with
// HandleTimeout, and read notifications with PollEvent. It does no I/O, reads
// no clock, starts no goroutines, and is not safe for concurrent use -- the
// endpoint driver owns it from one goroutine. See the package documentation
// for the protocol.
type Conn struct {
	cfg      *Config
	isClient bool
	state    State
	err      error

	localCID, remoteCID, origDCID wire.ConnID

	// handshake
	eph            *ecdh.PrivateKey
	helloMsg       []byte
	serverHelloMsg []byte
	th             []byte
	sendProt       *protector
	recvProt       *protector
	peerID         ed25519.PublicKey
	authed         bool // server: client proven; client: server proven
	confirmed      bool // client: HANDSHAKE_DONE received; server: == authed
	addrValidated  bool // server: client proved it can receive from us
	pendingHello   bool // client: HELLO must be (re)sent
	pendingSHello  bool // server: SERVER_HELLO must be (re)sent
	peerParams     transportParams
	created        time.Time
	establishedAt  time.Time

	// packet numbers and acknowledgements
	nextPN uint64
	acks   ackTracker
	rec    recovery
	pace   pacer
	paceAt time.Time
	probes int

	// streams
	streams       map[uint64]*stream
	order         []uint64 // stream ids, sorted, for round-robin sending
	rr            int
	nextLocalIdx  uint64
	peerMaxIdx    uint64 // streams we may open (peer's MAX_STREAMS)
	peerOpenedIdx uint64 // peer-initiated streams opened so far
	localMaxIdx   uint64 // peer-initiated streams we allow
	acceptQ       []uint64

	// connection flow control
	sendMaxData uint64 // peer's limit on our new stream bytes
	sentData    uint64 // new stream bytes sent so far
	recvMaxData uint64 // our advertised limit
	recvData    uint64 // highest-offset sum received
	consumed    uint64 // bytes the application has read (or abandoned)

	// queued control frames
	ctrl           []wire.Frame
	needMaxData    bool
	needMaxStreams bool

	dgOut, dgIn [][]byte
	events      []Event

	// close
	closeFrame *wire.ConnectionClose
	closeSent  bool
	drainUntil time.Time

	// timers / accounting
	lastRecv, lastAckElicitSent, lastSend time.Time
	pingQueued                            bool
	bytesSent, bytesRecv                  uint64
	pktsSent, pktsRecv, pktsDropped       uint64
	retxBytes                             uint64
	scratch                               []byte
}

func newConn(cfg *Config, client bool, now time.Time) (*Conn, error) {
	cfg, err := cfg.resolved()
	if err != nil {
		return nil, err
	}
	c := &Conn{
		cfg: cfg, isClient: client, created: now, lastRecv: now,
		streams:     make(map[uint64]*stream),
		localMaxIdx: cfg.MaxStreams,
		recvMaxData: cfg.InitialMaxData,
		scratch:     make([]byte, 0, cfg.MaxPacketSize),
	}
	c.rec.cc = newController(cfg.Congestion, c.mss())
	if c.eph, err = x25519Key(cfg.Rand); err != nil {
		return nil, err
	}
	cid, err := readRand(cfg.Rand, 8)
	if err != nil {
		return nil, err
	}
	copy(c.localCID[:], cid)
	return c, nil
}

func (c *Conn) localParams() transportParams {
	return transportParams{maxData: c.cfg.InitialMaxData, maxStreamData: c.cfg.InitialMaxStreamData, maxStreams: c.cfg.MaxStreams}
}

func (c *Conn) applyPeerParams(p transportParams) {
	c.peerParams = p
	c.sendMaxData = p.maxData
	c.peerMaxIdx = p.maxStreams
}

// mss is the payload a full packet can carry, used as the congestion
// controller's segment size.
func (c *Conn) mss() int { return c.cfg.MaxPacketSize }

// NewClientConn starts a client connection; the first Poll returns its
// Initial packet.
func NewClientConn(cfg *Config, now time.Time) (*Conn, error) {
	c, err := newConn(cfg, true, now)
	if err != nil {
		return nil, err
	}
	dcid, err := readRand(c.cfg.Rand, 8)
	if err != nil {
		return nil, err
	}
	copy(c.origDCID[:], dcid)
	c.remoteCID = c.origDCID
	nonce, err := readRand(c.cfg.Rand, 32)
	if err != nil {
		return nil, err
	}
	c.helloMsg = hello{eph: c.eph.PublicKey().Bytes(), nonce: nonce, params: c.localParams()}.encode()
	c.pendingHello = true
	return c, nil
}

// NewServerConn accepts a connection from the client's Initial packet.
func NewServerConn(cfg *Config, initial []byte, now time.Time) (*Conn, error) {
	h, err := wire.ParseHeader(initial)
	if err != nil {
		return nil, err
	}
	if h.Type != wire.InitialPacket {
		return nil, fmt.Errorf("ktp: expected an initial packet, got %s", h.Type)
	}
	if len(initial) < MinInitialSize {
		return nil, fmt.Errorf("ktp: initial packet of %d bytes is under the %d-byte minimum", len(initial), MinInitialSize)
	}
	payload, err := (*protector)(nil).open(h, initial)
	if err != nil {
		return nil, err
	}
	frames, err := wire.ParseFrames(payload)
	if err != nil {
		return nil, err
	}
	var helloMsg []byte
	for _, f := range frames {
		if hs, ok := f.(wire.Handshake); ok {
			helloMsg = append([]byte(nil), hs.Data...)
		}
	}
	hl, err := parseHello(helloMsg)
	if err != nil {
		return nil, err
	}
	c, err := newConn(cfg, false, now)
	if err != nil {
		return nil, err
	}
	c.remoteCID, c.origDCID = h.SrcCID, h.DstCID
	peerEph, err := ecdh.X25519().NewPublicKey(hl.eph)
	if err != nil {
		return nil, fmt.Errorf("ktp: bad client key: %w", err)
	}
	shared, err := c.eph.ECDH(peerEph)
	if err != nil {
		return nil, err
	}
	nonce, err := readRand(c.cfg.Rand, 32)
	if err != nil {
		return nil, err
	}
	sh := serverHello{eph: c.eph.PublicKey().Bytes(), nonce: nonce,
		pub: c.cfg.Identity.Public().(ed25519.PublicKey), params: c.localParams()}
	core := sh.encodeCore()
	c.th = transcriptHash(helloMsg, core)
	keys, err := deriveKeys(shared, c.cfg.ClusterKey, c.th)
	if err != nil {
		return nil, err
	}
	sig := ed25519.Sign(c.cfg.Identity, signedMessage("server", c.th))
	c.serverHelloMsg = append(append(core, sig...), confirmTag(keys.confirm, c.th)...)
	if c.sendProt, err = newProtector(keys.s2cKey, keys.s2cIV, c.cfg.Insecure); err != nil {
		return nil, err
	}
	if c.recvProt, err = newProtector(keys.c2sKey, keys.c2sIV, c.cfg.Insecure); err != nil {
		return nil, err
	}
	c.helloMsg = helloMsg
	c.applyPeerParams(hl.params)
	c.pendingSHello = true
	c.bytesRecv += uint64(len(initial))
	c.pktsRecv++
	c.acks.record(now, h.PacketNumber, true)
	return c, nil
}

// ---- accessors ----

// State returns the lifecycle stage.
func (c *Conn) State() State { return c.state }

// Established reports whether streams may be used.
func (c *Conn) Established() bool { return c.state == StateEstablished }

// Err is why the connection closed (nil while open).
func (c *Conn) Err() error { return c.err }

// LocalCID is the connection ID peers address this end by.
func (c *Conn) LocalCID() wire.ConnID { return c.localCID }

// RemoteCID is the connection ID this end addresses the peer by.
func (c *Conn) RemoteCID() wire.ConnID { return c.remoteCID }

// OriginalDCID is the destination CID of the client's first Initial; servers
// route retransmitted Initials by it.
func (c *Conn) OriginalDCID() wire.ConnID { return c.origDCID }

// IsClient reports the role.
func (c *Conn) IsClient() bool { return c.isClient }

// PeerIdentity is the peer's proven Ed25519 key (nil until authenticated).
func (c *Conn) PeerIdentity() ed25519.PublicKey { return c.peerID }

// PollEvent returns the next pending event.
func (c *Conn) PollEvent() (Event, bool) {
	if len(c.events) == 0 {
		return Event{}, false
	}
	e := c.events[0]
	c.events = c.events[1:]
	if len(c.events) == 0 {
		c.events = nil
	}
	return e, true
}

func (c *Conn) emit(t EventType, id StreamID, err error) {
	c.events = append(c.events, Event{Type: t, Stream: id, Err: err})
}

// Stats snapshots the transport counters.
func (c *Conn) Stats() Stats {
	r := &c.rec.rtt
	s := Stats{
		State: c.state.String(), Congestion: c.rec.cc.Name(),
		SRTT: r.smoothed, MinRTT: r.minRTT, RTTVar: r.variance, LatestRTT: r.latest,
		CWND: c.rec.cc.Window(), SSThresh: c.rec.cc.SSThresh(), BytesInFlight: c.rec.bytesInFlight,
		PacketsSent: c.pktsSent, PacketsReceived: c.pktsRecv, PacketsLost: c.rec.lost, PacketsDropped: c.pktsDropped,
		BytesSent: c.bytesSent, BytesReceived: c.bytesRecv, StreamBytesRetx: c.retxBytes,
		PTOCount: c.rec.ptos, CongestionEvents: c.rec.congestionEvents, StreamsOpen: len(c.streams),
	}
	if s.SSThresh > 1<<40 {
		s.SSThresh = 0 // "infinite" before the first loss
	}
	if !c.establishedAt.IsZero() {
		s.Handshake = c.establishedAt.Sub(c.created)
	}
	return s
}

// ---- streams ----

func (c *Conn) localBit() uint64 {
	if c.isClient {
		return 0
	}
	return 1
}

func (c *Conn) addStream(id uint64) *stream {
	s := newStream(id, c.peerParams.maxStreamData, c.cfg.InitialMaxStreamData)
	c.streams[id] = s
	i := sort.Search(len(c.order), func(i int) bool { return c.order[i] >= id })
	c.order = append(c.order, 0)
	copy(c.order[i+1:], c.order[i:])
	c.order[i] = id
	return s
}

// OpenStream opens a new bidirectional stream.
func (c *Conn) OpenStream() (StreamID, error) {
	if c.state != StateEstablished {
		if c.err != nil {
			return 0, c.err
		}
		return 0, ErrNotEstablished
	}
	if c.nextLocalIdx >= c.peerMaxIdx {
		return 0, ErrStreamLimit
	}
	id := c.nextLocalIdx<<1 | c.localBit()
	c.nextLocalIdx++
	c.addStream(id)
	return id, nil
}

// AcceptStream returns the next stream the peer opened, if any.
func (c *Conn) AcceptStream() (StreamID, bool) {
	if len(c.acceptQ) == 0 {
		return 0, false
	}
	id := c.acceptQ[0]
	c.acceptQ = c.acceptQ[1:]
	return id, true
}

func (c *Conn) stream(id StreamID) (*stream, error) {
	if s, ok := c.streams[id]; ok {
		return s, nil
	}
	if c.err != nil {
		return nil, c.err
	}
	return nil, ErrUnknownStream
}

// StreamWrite queues p on a stream. It returns how much was accepted, which
// is less than len(p) when the send buffer is full; wait for
// EventStreamWritable and try again.
func (c *Conn) StreamWrite(id StreamID, p []byte) (int, error) {
	s, err := c.stream(id)
	if err != nil {
		return 0, err
	}
	switch {
	case s.stopErr != nil:
		return 0, s.stopErr
	case s.finQueued || s.resetSent:
		return 0, ErrWriteClosed
	case c.state != StateEstablished:
		return 0, ErrNotEstablished
	}
	room := c.cfg.MaxSendBuffer - len(s.sendBuf)
	n := min(room, len(p))
	if n > 0 {
		s.sendBuf = append(s.sendBuf, p[:n]...)
	}
	return n, nil
}

// StreamWritable reports whether StreamWrite would accept at least one byte.
func (c *Conn) StreamWritable(id StreamID) bool {
	s, ok := c.streams[id]
	return ok && s.stopErr == nil && !s.finQueued && !s.resetSent && len(s.sendBuf) < c.cfg.MaxSendBuffer
}

// StreamRead reads ready data. It returns (0, nil) when nothing is ready yet,
// io.EOF after the peer's FIN, or a *StreamResetError.
func (c *Conn) StreamRead(id StreamID, p []byte) (int, error) {
	s, err := c.stream(id)
	if err != nil {
		return 0, err
	}
	n, err := s.read(p)
	if n > 0 {
		c.consume(uint64(n))
	}
	c.maybeRetire(s)
	return n, err
}

// StreamReadable reports whether StreamRead would return data, EOF or an error.
func (c *Conn) StreamReadable(id StreamID) bool {
	s, ok := c.streams[id]
	return ok && s.readable()
}

func (c *Conn) consume(n uint64) {
	c.consumed += n
	w := c.cfg.InitialMaxData
	if c.recvMaxData-c.consumed < w/2 {
		c.recvMaxData = c.consumed + w
		c.needMaxData = true
	}
}

// StreamCloseWrite half-closes a stream: queued data is delivered, then FIN.
func (c *Conn) StreamCloseWrite(id StreamID) error {
	s, err := c.stream(id)
	if err != nil {
		return err
	}
	s.finQueued = true
	return nil
}

// StreamReset abandons a stream's send half with an application error code.
func (c *Conn) StreamReset(id StreamID, code uint64) error {
	s, err := c.stream(id)
	if err != nil {
		return err
	}
	if s.resetSent || s.finAcked {
		return nil
	}
	s.resetSent = true
	s.retx = nil
	c.ctrl = append(c.ctrl, wire.ResetStream{StreamID: id, Code: code, FinalSize: s.sendNext})
	s.sendBuf = nil
	c.maybeRetire(s)
	return nil
}

// StreamStopSending abandons a stream's receive half: buffered data is
// dropped and the peer is asked to stop sending.
func (c *Conn) StreamStopSending(id StreamID, code uint64) error {
	s, err := c.stream(id)
	if err != nil {
		return err
	}
	if s.readClosed {
		return nil
	}
	s.readClosed = true
	dropped := uint64(len(s.ready))
	for _, ch := range s.chunks {
		dropped += uint64(len(ch.data))
	}
	s.readOff += uint64(len(s.ready))
	s.ready, s.chunks = nil, nil
	if dropped > 0 {
		c.consume(dropped)
	}
	if s.finalSize < 0 {
		c.ctrl = append(c.ctrl, wire.StopSending{StreamID: id, Code: code})
	}
	c.maybeRetire(s)
	return nil
}

// maybeRetire forgets a stream once both halves are finished, granting the
// peer a new stream slot if it opened it.
func (c *Conn) maybeRetire(s *stream) {
	if !s.sendDone() || !s.recvDone() {
		return
	}
	if _, ok := c.streams[s.id]; !ok {
		return
	}
	delete(c.streams, s.id)
	i := sort.Search(len(c.order), func(i int) bool { return c.order[i] >= s.id })
	if i < len(c.order) && c.order[i] == s.id {
		c.order = append(c.order[:i], c.order[i+1:]...)
	}
	if s.id&1 != c.localBit() {
		c.localMaxIdx++
		c.needMaxStreams = true
	}
}

// ---- datagrams ----

func (c *Conn) maxDatagram() int {
	return c.cfg.MaxPacketSize - wire.HeaderLen - 16 - 1 - 2 - wire.Ack{}.Len()*2
}

// SendDatagram queues an unreliable datagram. When the queue is full the
// oldest datagram is dropped: datagrams are for data that is useless late.
func (c *Conn) SendDatagram(p []byte) error {
	if c.state != StateEstablished {
		if c.err != nil {
			return c.err
		}
		return ErrNotEstablished
	}
	if len(p) > c.maxDatagram() {
		return ErrDatagramTooLarge
	}
	if len(c.dgOut) >= c.cfg.MaxDatagramQueue {
		c.dgOut = c.dgOut[1:]
	}
	c.dgOut = append(c.dgOut, append([]byte(nil), p...))
	return nil
}

// RecvDatagram returns the next received datagram, if any.
func (c *Conn) RecvDatagram() ([]byte, bool) {
	if len(c.dgIn) == 0 {
		return nil, false
	}
	d := c.dgIn[0]
	c.dgIn = c.dgIn[1:]
	return d, true
}

// ---- close ----

// Close starts an orderly close: the next Poll sends CONNECTION_CLOSE, after
// which the connection drains for three PTOs and becomes StateClosed.
func (c *Conn) Close(code uint64, reason string) {
	if c.state >= StateClosing {
		return
	}
	if len(reason) > 256 {
		reason = reason[:256]
	}
	c.closeWith(&CloseError{Code: code, Reason: reason})
}

func (c *Conn) closeWith(e *CloseError) {
	if c.state >= StateClosing {
		return
	}
	c.state = StateClosing
	c.err = e
	c.closeFrame = &wire.ConnectionClose{Code: e.Code, Reason: e.Reason}
	c.closeSent = false
	c.emit(EventClosed, 0, e)
}

// closeNow discards the connection without telling the peer (idle timeout,
// stateless reset, handshake failure before keys exist).
func (c *Conn) closeNow(err error) {
	if c.state == StateClosed {
		return
	}
	already := c.state >= StateClosing
	c.state = StateClosed
	if c.err == nil {
		c.err = err
	}
	if !already {
		c.emit(EventClosed, 0, c.err)
	}
}

func (c *Conn) drainPeriod() time.Duration {
	return 3 * c.rec.rtt.pto(c.cfg.InitialRTT, c.cfg.MaxAckDelay)
}
