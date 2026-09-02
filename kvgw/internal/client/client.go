// Package client is a small synchronous TCP client for kvnode. One connection
// per node, reused across calls and re-dialed once if it has gone stale (the
// common failure mode when a node restarts between requests). It implements
// kv.Node so the quorum coordinator can drive it.
package client

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/internal/proto"
)

type Client struct {
	addr    string
	timeout time.Duration

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func New(addr string, timeout time.Duration) *Client {
	return &Client{addr: addr, timeout: timeout}
}

func (c *Client) Addr() string { return c.addr }

func (c *Client) connect() error {
	if c.conn != nil {
		return nil
	}
	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.r = bufio.NewReader(conn)
	c.w = bufio.NewWriter(conn)
	return nil
}

func (c *Client) reset() {
	if c.conn != nil {
		c.conn.Close()
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

	if err := c.connect(); err != nil {
		return err
	}
	c.conn.SetDeadline(c.deadline())
	if err := fn(); err != nil {
		c.reset()
		if err := c.connect(); err != nil {
			return err
		}
		c.conn.SetDeadline(c.deadline())
		if err := fn(); err != nil {
			c.reset()
			return err
		}
	}
	return nil
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

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reset()
}
