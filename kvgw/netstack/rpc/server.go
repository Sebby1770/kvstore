package rpc

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

// Handler serves a unary call.
type Handler func(ctx context.Context, req []byte) ([]byte, error)

// StreamHandler serves a server-streaming call: send each message, then
// return (nil for END, an error for ERR).
type StreamHandler func(ctx context.Context, req []byte, send func([]byte) error) error

// Interceptor wraps every unary call (logging, auth, clocks, metrics).
type Interceptor func(ctx context.Context, method string, req []byte, next Handler) ([]byte, error)

// Peer describes the caller of a call being served.
type Peer struct {
	Addr     string
	Identity ed25519.PublicKey
}

type ctxKey int

const (
	keyMD ctxKey = iota
	keyPeer
	keyOutMD
)

// Metadata returns the caller's metadata inside a handler.
func Metadata(ctx context.Context) map[string]string {
	md, _ := ctx.Value(keyMD).(map[string]string)
	return md
}

// PeerFrom returns the caller inside a handler.
func PeerFrom(ctx context.Context) (Peer, bool) {
	p, ok := ctx.Value(keyPeer).(Peer)
	return p, ok
}

// WithMetadata attaches outgoing metadata to a client call.
func WithMetadata(ctx context.Context, kv ...string) context.Context {
	md := map[string]string{}
	for k, v := range OutgoingMetadata(ctx) {
		md[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		md[kv[i]] = kv[i+1]
	}
	return context.WithValue(ctx, keyOutMD, md)
}

// OutgoingMetadata is what WithMetadata attached.
func OutgoingMetadata(ctx context.Context) map[string]string {
	md, _ := ctx.Value(keyOutMD).(map[string]string)
	return md
}

// MethodStats are per-method server counters.
type MethodStats struct {
	Method     string  `json:"method"`
	Calls      uint64  `json:"calls"`
	Errors     uint64  `json:"errors"`
	InFlight   int64   `json:"in_flight"`
	LatencyMs  float64 `json:"latency_ms"` // EWMA
	MaxLatency float64 `json:"max_latency_ms"`
}

// Server dispatches calls arriving on an endpoint's connections.
type Server struct {
	MaxMessage int

	mu           sync.RWMutex
	unary        map[string]Handler
	stream       map[string]StreamHandler
	interceptors []Interceptor
	stats        map[string]*MethodStats
}

// NewServer returns an empty server.
func NewServer() *Server {
	return &Server{MaxMessage: DefaultMaxMessage, unary: map[string]Handler{},
		stream: map[string]StreamHandler{}, stats: map[string]*MethodStats{}}
}

// Register adds a unary method.
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	s.unary[method] = h
	s.mu.Unlock()
}

// RegisterStream adds a server-streaming method.
func (s *Server) RegisterStream(method string, h StreamHandler) {
	s.mu.Lock()
	s.stream[method] = h
	s.mu.Unlock()
}

// Use appends a unary interceptor; the first added runs outermost.
func (s *Server) Use(i Interceptor) {
	s.mu.Lock()
	s.interceptors = append(s.interceptors, i)
	s.mu.Unlock()
}

// Methods lists registered method names.
func (s *Server) Methods() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for m := range s.unary {
		out = append(out, m)
	}
	for m := range s.stream {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// Stats snapshots per-method counters.
func (s *Server) Stats() []MethodStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]MethodStats, 0, len(s.stats))
	for _, st := range s.stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Method < out[j].Method })
	return out
}

func (s *Server) begin(method string) {
	s.mu.Lock()
	st := s.stats[method]
	if st == nil {
		st = &MethodStats{Method: method}
		s.stats[method] = st
	}
	st.Calls++
	st.InFlight++
	s.mu.Unlock()
}

func (s *Server) end(method string, took time.Duration, failed bool) {
	ms := float64(took) / float64(time.Millisecond)
	s.mu.Lock()
	st := s.stats[method]
	st.InFlight--
	if failed {
		st.Errors++
	}
	if st.Calls == 1 {
		st.LatencyMs = ms
	} else {
		st.LatencyMs = 0.8*st.LatencyMs + 0.2*ms
	}
	st.MaxLatency = max(st.MaxLatency, ms)
	s.mu.Unlock()
}

// Serve accepts connections on ep until ctx ends or ep closes.
func (s *Server) Serve(ctx context.Context, ep *endpoint.Endpoint) error {
	for {
		c, err := ep.Accept(ctx)
		if err != nil {
			return err
		}
		go s.ServeConn(ctx, c)
	}
}

// ServeConn serves every stream the peer opens on c.
func (s *Server) ServeConn(ctx context.Context, c *endpoint.Conn) {
	for {
		st, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.serveStream(ctx, c, st)
	}
}

func (s *Server) serveStream(parent context.Context, c *endpoint.Conn, st *endpoint.Stream) {
	r := bufio.NewReader(st)
	req, err := readRequest(r, s.MaxMessage)
	if err != nil {
		st.Write(errFrame(toError(fmt.Errorf("bad request: %w", err))))
		st.Close()
		st.CancelRead(0)
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if req.deadline > 0 {
		var c2 context.CancelFunc
		ctx, c2 = context.WithTimeout(ctx, time.Duration(req.deadline)*time.Microsecond)
		defer c2()
	}
	ctx = context.WithValue(ctx, keyMD, req.md)
	ctx = context.WithValue(ctx, keyPeer, Peer{Addr: c.RemoteAddrString(), Identity: c.PeerIdentity()})

	// The caller keeps its write side open for the life of the call: EOF
	// means it finished normally, anything else (a reset) means it gave up.
	go func() {
		_, err := r.ReadByte()
		if err != io.EOF {
			cancel()
		}
	}()

	s.begin(req.method)
	start := time.Now()
	w := bufio.NewWriter(st)
	failed := s.dispatch(ctx, req, w)
	w.Flush()
	st.Close()
	s.end(req.method, time.Since(start), failed)
}

func (s *Server) dispatch(ctx context.Context, req request, w *bufio.Writer) (failed bool) {
	defer func() {
		if p := recover(); p != nil {
			w.Write(errFrame(&Error{Code: Internal, Message: fmt.Sprintf("handler panicked: %v", p)}))
			_ = debug.Stack // keep for local debugging; never sent to callers
			failed = true
		}
	}()
	s.mu.RLock()
	uh, isUnary := s.unary[req.method]
	sh, isStream := s.stream[req.method]
	chain := s.interceptors
	s.mu.RUnlock()
	switch {
	case isUnary:
		h := uh
		for i := len(chain) - 1; i >= 0; i-- {
			ic, next := chain[i], h
			h = func(ctx context.Context, b []byte) ([]byte, error) { return ic(ctx, req.method, b, next) }
		}
		resp, err := h(ctx, req.payload)
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			w.Write(errFrame(toError(err)))
			return true
		}
		if len(resp) > s.MaxMessage {
			w.Write(errFrame(&Error{Code: ResourceExhausted, Message: "response too large"}))
			return true
		}
		w.Write(appendFrame(nil, kindData, resp))
		w.Write(appendFrame(nil, kindEnd, nil))
		return false
	case isStream:
		send := func(b []byte) error {
			if len(b) > s.MaxMessage {
				return Errorf(ResourceExhausted, "message too large")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := w.Write(appendFrame(nil, kindData, b)); err != nil {
				return err
			}
			return w.Flush()
		}
		if err := sh(ctx, req.payload, send); err != nil {
			w.Write(errFrame(toError(err)))
			return true
		}
		w.Write(appendFrame(nil, kindEnd, nil))
		return false
	}
	w.Write(errFrame(&Error{Code: Unimplemented, Message: "unknown method " + req.method}))
	return true
}

// ErrCode is the KTP stream reset code a cancelled caller uses.
const ErrCode = ktp.CodeApplication + 1
