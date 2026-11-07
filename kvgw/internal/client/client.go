// Package client is a small synchronous client for kvnode, the storage tier.
// It speaks kvnode's request protocol over one of two transports, chosen by
// the address:
//
//	ktp://host:port   a stream on the coordinator's KTP endpoint: encrypted,
//	                  authenticated, keyed to the cluster secret
//	host:port         plain TCP (also tcp://host:port)
//
// Each Client holds one TCP connection or one KTP stream, reused across calls
// and re-opened once if it has gone stale (the common failure mode when a
// node restarts between requests; over KTP the restarted node answers with
// a stateless reset, so the stale connection is noticed at once). It
// implements kv.Node so the quorum coordinator can drive it.
package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

type Client struct {
	addr    string // as configured, scheme included: the node's ring identity
	timeout time.Duration
	dial    func(ctx context.Context) (transport, error)

	mu   sync.Mutex
	conn transport
	r    *bufio.Reader
	w    *bufio.Writer
}

// New returns a TCP client for addr ("host:port" or "tcp://host:port").
func New(addr string, timeout time.Duration) *Client {
	hostport := strings.TrimPrefix(addr, SchemeTCP)
	return &Client{addr: addr, timeout: timeout, dial: func(ctx context.Context) (transport, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", hostport)
		if err != nil {
			return nil, err
		}
		return tcpTransport{conn}, nil
	}}
}

// NewKTP returns a client for addr ("ktp://host:port") whose requests travel
// on a stream of ep's (shared, per-peer) connection to the node.
func NewKTP(addr string, ep *endpoint.Endpoint, timeout time.Duration) *Client {
	hostport := strings.TrimPrefix(addr, SchemeKTP)
	return &Client{addr: addr, timeout: timeout, dial: func(ctx context.Context) (transport, error) {
		conn, err := ep.Conn(ctx, hostport)
		if err != nil {
			return nil, fmt.Errorf("ktp %s: %w", hostport, err)
		}
		s, err := conn.OpenStream(ctx)
		if err != nil {
			return nil, fmt.Errorf("ktp %s: open stream: %w", hostport, err)
		}
		return ktpTransport{s}, nil
	}}
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
	return &Client{addr: addr, timeout: timeout, dial: func(context.Context) (transport, error) { return nil, err }}, err
}

func (c *Client) Addr() string { return c.addr }

func (c *Client) connect() error {
	if c.conn != nil {
		return nil
	}
	ctx := context.Background()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	t, err := c.dial(ctx)
	if err != nil {
		return err
	}
	c.conn = t
	c.r = bufio.NewReader(t)
	c.w = bufio.NewWriter(t)
	return nil
}

// reset abandons the connection after a failed exchange: whatever it still
// carries belongs to a request nobody waits for any more.
func (c *Client) reset() {
	if c.conn != nil {
		c.conn.abort()
	}
	c.conn = nil
	c.r = nil
	c.w = nil
}

func (c *Client) deadline() time.Time {
	if c.timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.timeout)
}

// withConn runs fn with the connection locked, reconnecting once if a
// previously-cached connection has gone stale.
func (c *Client) withConn(fn func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fresh := c.conn == nil
	if err := c.connect(); err != nil {
		return err
	}
	c.conn.SetDeadline(c.deadline())
	err := fn()
	if err == nil {
		return nil
	}
	c.reset()
	if fresh || !retryable(err) {
		return err
	}
	if err := c.connect(); err != nil {
		return err
	}
	c.conn.SetDeadline(c.deadline())
	if err := fn(); err != nil {
		c.reset()
		return err
	}
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
	return c.withConn(func() error {
		if err := proto.WriteU8(c.w, proto.OpPing); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
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
	err := c.withConn(func() error {
		if err := proto.WriteU8(c.w, proto.OpStat); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("stat: %s", proto.StatusString(s))
		}
		if st.Count, err = proto.ReadU32(c.r); err != nil {
			return err
		}
		if st.Capacity, err = proto.ReadU32(c.r); err != nil {
			return err
		}
		if st.MaxVersion, err = proto.ReadU64(c.r); err != nil {
			return err
		}
		st.Tombstones, err = proto.ReadU32(c.r)
		return err
	})
	return st, err
}

func (c *Client) Get(key []byte) (kv.ReadResult, error) {
	var res kv.ReadResult
	err := c.withConn(func() error {
		res = kv.ReadResult{}
		if err := proto.WriteU8(c.w, proto.OpGet); err != nil {
			return err
		}
		if err := proto.WriteU32(c.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(c.w, key); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
		if err != nil {
			return err
		}
		switch s {
		case proto.StatusOK:
			ver, err := proto.ReadU64(c.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(c.r)
			if err != nil {
				return err
			}
			n, err := proto.ReadU32(c.r)
			if err != nil {
				return err
			}
			val, err := proto.ReadBytes(c.r, n)
			if err != nil {
				return err
			}
			res = kv.ReadResult{State: kv.Present, Value: val, Version: ver, ExpiresAt: exp}
			return nil
		case proto.StatusDeleted:
			ver, err := proto.ReadU64(c.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(c.r)
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
	return c.withConn(func() error {
		if err := proto.WriteU8(c.w, proto.OpPutX); err != nil {
			return err
		}
		if err := proto.WriteU32(c.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(c.w, key); err != nil {
			return err
		}
		if err := proto.WriteU64(c.w, version); err != nil {
			return err
		}
		if err := proto.WriteU64(c.w, expiresAt); err != nil {
			return err
		}
		if err := proto.WriteU32(c.w, uint32(len(val))); err != nil {
			return err
		}
		if err := proto.WriteBytes(c.w, val); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
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
	err := c.withConn(func() error {
		existed = false
		if err := proto.WriteU8(c.w, proto.OpDel); err != nil {
			return err
		}
		if err := proto.WriteU32(c.w, uint32(len(key))); err != nil {
			return err
		}
		if err := proto.WriteBytes(c.w, key); err != nil {
			return err
		}
		if err := proto.WriteU64(c.w, version); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
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
	err := c.withConn(func() error {
		out = nil
		if err := proto.WriteU8(c.w, op); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("scan: %s", proto.StatusString(s))
		}
		for {
			klen, err := proto.ReadU32(c.r)
			if err != nil {
				return err
			}
			if klen == proto.ScanTerminator {
				return nil
			}
			key, err := proto.ReadBytes(c.r, klen)
			if err != nil {
				return err
			}
			ver, err := proto.ReadU64(c.r)
			if err != nil {
				return err
			}
			exp, err := proto.ReadU64(c.r)
			if err != nil {
				return err
			}
			var tomb byte
			if withTomb {
				tomb, err = proto.ReadU8(c.r)
				if err != nil {
					return err
				}
			}
			vlen, err := proto.ReadU32(c.r)
			if err != nil {
				return err
			}
			val, err := proto.ReadBytes(c.r, vlen)
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
	return c.withConn(func() error {
		if err := proto.WriteU8(c.w, proto.OpCompact); err != nil {
			return err
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		s, err := proto.ReadU8(c.r)
		if err != nil {
			return err
		}
		if s != proto.StatusOK {
			return fmt.Errorf("compact: %s", proto.StatusString(s))
		}
		return nil
	})
}

// Close releases the connection (or stream).
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.close()
	}
	c.conn, c.r, c.w = nil, nil, nil
}
