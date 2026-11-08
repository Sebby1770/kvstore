// Package client is a small synchronous client for kvnode, the storage tier.
// It speaks kvnode's request protocol over one of two transports, chosen by
// the address:
//
//	ktp://host:port   a stream on the coordinator's KTP endpoint: encrypted,
//	                  authenticated, keyed to the cluster secret
//	host:port         plain TCP (also tcp://host:port)
//
// Each Client keeps a few TCP connections or KTP streams (MaxConns), each
// carrying one request at a time, reused across calls and re-opened once if
// stale (the common failure mode when a node restarts between requests; over
// KTP the restarted node answers with a stateless reset, so the stale
// connection is noticed at once). It implements kv.Node so the quorum
// coordinator can drive it.
package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/internal/proto"
	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

// Address schemes.
const (
	SchemeKTP = "ktp://"
	SchemeTCP = "tcp://"
)

// abortCode is the application error code on streams abandoned mid-request.
const abortCode = ktp.CodeApplication + 1

// IsKTP reports whether addr names a storage node reached over KTP.
func IsKTP(addr string) bool { return strings.HasPrefix(addr, SchemeKTP) }

// transport is one TCP connection or KTP stream.
type transport interface {
	io.Reader
	io.Writer
	SetDeadline(t time.Time) error
	// close ends the transport after a clean exchange; abort after a failed
	// one, when the peer may still be sending a reply nobody will read.
	close()
	abort()
}

type tcpTransport struct{ net.Conn }

func (t tcpTransport) close() { t.Conn.Close() }
func (t tcpTransport) abort() { t.Conn.Close() }

type ktpTransport struct{ *endpoint.Stream }

// close sends FIN and stops reading: kvnode finishes its side and both ends
// forget the stream.
func (t ktpTransport) close() {
	t.Stream.Close()
	t.Stream.CancelRead(ktp.CodeNoError)
}

// abort resets both directions, so no stale reply can ever be read as the
// answer to a later request.
func (t ktpTransport) abort() {
	t.Stream.Reset(abortCode)
	t.Stream.CancelRead(abortCode)
}

// MaxConns is how many requests one Client runs at once, each on its own
// TCP connection or KTP stream, so that a slow request (a big scan, a node
// that stopped answering) never queues the others behind it.
const MaxConns = 4

type Client struct {
	addr    string // as configured, scheme included: the node's ring identity
	timeout time.Duration
	dial    func(ctx context.Context) (transport, error)
	slots   chan struct{} // one per request in flight

	mu     sync.Mutex
	idle   []*conn // ready for reuse, most recently used last
	closed bool
}

// conn is one connection or stream with its buffers.
type conn struct {
	t transport
	r *bufio.Reader
	w *bufio.Writer
}

func newClient(addr string, timeout time.Duration, dial func(ctx context.Context) (transport, error)) *Client {
	return &Client{addr: addr, timeout: timeout, dial: dial, slots: make(chan struct{}, MaxConns)}
}

// New returns a TCP client for addr ("host:port" or "tcp://host:port").
func New(addr string, timeout time.Duration) *Client {
	hostport := strings.TrimPrefix(addr, SchemeTCP)
	return newClient(addr, timeout, func(ctx context.Context) (transport, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", hostport)
		if err != nil {
			return nil, err
		}
		return tcpTransport{conn}, nil
	})
}

// NewKTP returns a client for addr ("ktp://host:port") whose requests travel
// on a stream of ep's (shared, per-peer) connection to the node.
func NewKTP(addr string, ep *endpoint.Endpoint, timeout time.Duration) *Client {
	hostport := strings.TrimPrefix(addr, SchemeKTP)
	return newClient(addr, timeout, func(ctx context.Context) (transport, error) {
		conn, err := ep.Conn(ctx, hostport)
		if err != nil {
			return nil, fmt.Errorf("ktp %s: %w", hostport, err)
		}
		s, err := conn.OpenStream(ctx)
		if err != nil {
			return nil, fmt.Errorf("ktp %s: open stream: %w", hostport, err)
		}
		return ktpTransport{s}, nil
	})
}

// Open returns a client for addr, choosing the transport by scheme; ep is
// needed for ktp:// addresses. The error reports an unusable address
// (unknown scheme, or ktp:// without an endpoint). The client is never nil:
// for an unusable address every call fails with that error, so node
// factories that cannot report errors may ignore it.
func Open(addr string, ep *endpoint.Endpoint, timeout time.Duration) (*Client, error) {
	var err error
	switch {
	case IsKTP(addr) && ep != nil:
		return NewKTP(addr, ep, timeout), nil
	case IsKTP(addr):
		err = fmt.Errorf("client: %s is a KTP address but no KTP endpoint is configured", addr)
	case strings.HasPrefix(addr, SchemeTCP) || !strings.Contains(addr, "://"):
		return New(addr, timeout), nil
	default:
		err = fmt.Errorf("client: unsupported address scheme in %q (use host:port or ktp://host:port)", addr)
	}
	return newClient(addr, timeout, func(context.Context) (transport, error) { return nil, err }), err
}

func (c *Client) Addr() string { return c.addr }

var errClosed = errors.New("client: closed")

func (c *Client) dialConn() (*conn, error) {
	ctx := context.Background()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	t, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{t: t, r: bufio.NewReader(t), w: bufio.NewWriter(t)}, nil
}

// take returns an idle connection (fresh=false) or dials one.
func (c *Client) take() (cn *conn, fresh bool, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false, errClosed
	}
	if n := len(c.idle); n > 0 {
		cn = c.idle[n-1]
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		return cn, false, nil
	}
	c.mu.Unlock()
	cn, err = c.dialConn()
	return cn, true, err
}

// give returns a connection after a clean exchange.
func (c *Client) give(cn *conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		cn.t.close()
		return
	}
	c.idle = append(c.idle, cn)
}

func (c *Client) deadline() time.Time {
	if c.timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.timeout)
}

// withConn runs fn on a connection of its own (at most MaxConns at once),
// retrying once on a fresh connection if a cached one had gone stale. A
// connection that failed mid-exchange is aborted, never reused: whatever it
// still carries belongs to a request nobody waits for any more.
func (c *Client) withConn(fn func(cn *conn) error) error {
	var timeout <-chan time.Time
	if c.timeout > 0 {
		t := time.NewTimer(c.timeout)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case c.slots <- struct{}{}:
	case <-timeout:
		return fmt.Errorf("client %s: %d requests already in flight: %w", c.addr, MaxConns, os.ErrDeadlineExceeded)
	}
	defer func() { <-c.slots }()

	cn, fresh, err := c.take()
	if err != nil {
		return err
	}
	cn.t.SetDeadline(c.deadline())
	if err = fn(cn); err == nil {
		c.give(cn)
		return nil
	}
	cn.t.abort()
	if fresh || !retryable(err) {
		return err
	}
	if cn, err = c.dialConn(); err != nil {
		return err
	}
	cn.t.SetDeadline(c.deadline())
	if err := fn(cn); err != nil {
		cn.t.abort()
		return err
	}
	c.give(cn)
	return nil
}

// retryable reports whether err means the cached connection had gone stale
// before the request (worth one retry on a fresh one), rather than that the
// node is slow or answered badly -- retrying a timeout would double the wait.
func retryable(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return !errors.Is(err, context.DeadlineExceeded)
}

func (c *Client) Ping() error {
	return c.withConn(func(cn *conn) error {
		if err := proto.WriteU8(cn.w, proto.OpPing); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("ping: %s", proto.StatusString(s))
		}
		return nil
	})
}

func (c *Client) Stat() (kv.Stat, error) {
	var st kv.Stat
	err := c.withConn(func(cn *conn) error {
		if err := proto.WriteU8(cn.w, proto.OpStat); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("stat: %s", proto.StatusString(s))
		}
		if st.Count, err = proto.ReadU32(cn.r); err != nil {
			return err
		}
		if st.Capacity, err = proto.ReadU32(cn.r); err != nil {
			return err
		}
		if st.MaxVersion, err = proto.ReadU64(cn.r); err != nil {
			return err
		}
		st.Tombstones, err = proto.ReadU32(cn.r)
		return err
	})
	return st, err
}

func (c *Client) Get(key []byte) (kv.ReadResult, error) {
	var res kv.ReadResult
	err := c.withConn(func(cn *conn) error {
		res = kv.ReadResult{}
		if err := proto.WriteU8(cn.w, proto.OpGet); err != nil {
			return err
		}
		if err := proto.WriteU32(cn.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(cn.w, key); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		switch s {
		case proto.StatusOK:
			ver, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			n, err := proto.ReadU32(cn.r)
			if err != nil {
				return err
			}
			val, err := proto.ReadBytes(cn.r, n)
			if err != nil {
				return err
			}
			res = kv.ReadResult{State: kv.Present, Value: val, Version: ver, ExpiresAt: exp}
			return nil
		case proto.StatusDeleted:
			ver, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			res = kv.ReadResult{State: kv.Deleted, Version: ver, ExpiresAt: exp}
			return nil
		case proto.StatusNotFound:
			res = kv.ReadResult{State: kv.Missing}
			return nil
		default:
			return fmt.Errorf("get: %s", proto.StatusString(s))
		}
	})
	return res, err
}

func (c *Client) Put(key, val []byte, version uint64) error {
	return c.PutX(key, val, version, 0)
}

func (c *Client) PutX(key, val []byte, version, expiresAt uint64) error {
	return c.withConn(func(cn *conn) error {
		if err := proto.WriteU8(cn.w, proto.OpPutX); err != nil {
			return err
		}
		if err := proto.WriteU32(cn.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(cn.w, key); err != nil {
			return err
		}
		if err := proto.WriteU64(cn.w, version); err != nil {
			return err
		}
		if err := proto.WriteU64(cn.w, expiresAt); err != nil {
			return err
		}
		if err := proto.WriteU32(cn.w, uint32(len(val))); err != nil {
			return err
		}
		if err := proto.WriteBytes(cn.w, val); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("put: %s", proto.StatusString(s))
		}
		return nil
	})
}

func (c *Client) Del(key []byte, version uint64) (bool, error) {
	var existed bool
	err := c.withConn(func(cn *conn) error {
		existed = false
		if err := proto.WriteU8(cn.w, proto.OpDel); err != nil {
			return err
		}
		if err := proto.WriteU32(cn.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(cn.w, key); err != nil {
			return err
		}
		if err := proto.WriteU64(cn.w, version); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		switch s {
		case proto.StatusOK:
			existed = true
			return nil
		case proto.StatusNotFound:
			existed = false
			return nil
		default:
			return fmt.Errorf("del: %s", proto.StatusString(s))
		}
	})
	return existed, err
}

func (c *Client) scanOp(op byte, withTomb bool) ([]kv.Entry, error) {
	var out []kv.Entry
	err := c.withConn(func(cn *conn) error {
		out = nil
		if err := proto.WriteU8(cn.w, op); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("scan: %s", proto.StatusString(s))
		}
		for {
			klen, err := proto.ReadU32(cn.r)
			if err != nil {
				return err
			}
			if klen == proto.ScanTerminator {
				return nil
			}
			key, err := proto.ReadBytes(cn.r, klen)
			if err != nil {
				return err
			}
			ver, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(cn.r)
			if err != nil {
				return err
			}
			var tomb byte
			if withTomb {
				tomb, err = proto.ReadU8(cn.r)
				if err != nil {
					return err
				}
			}
			vlen, err := proto.ReadU32(cn.r)
			if err != nil {
				return err
			}
			val, err := proto.ReadBytes(cn.r, vlen)
			if err != nil {
				return err
			}
			out = append(out, kv.Entry{
				Key:       key,
				Value:     val,
				Version:   ver,
				ExpiresAt: exp,
				Tombstone: tomb != 0,
			})
		}
	})
	return out, err
}

func (c *Client) Scan() ([]kv.Entry, error) {
	return c.scanOp(proto.OpScan, false)
}

func (c *Client) ScanAll() ([]kv.Entry, error) {
	return c.scanOp(proto.OpScanAll, true)
}

func (c *Client) Compact() error {
	return c.withConn(func(cn *conn) error {
		if err := proto.WriteU8(cn.w, proto.OpCompact); err != nil {
			return err
		}
		if err := cn.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(cn.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("compact: %s", proto.StatusString(s))
		}
		return nil
	})
}

// Close releases the client's connections (or streams); requests still in
// flight release theirs when they finish.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, cn := range c.idle {
		cn.t.close()
	}
	c.idle = nil
}
