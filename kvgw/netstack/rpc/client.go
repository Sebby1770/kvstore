package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
)

// ClientOptions tunes a Client.
type ClientOptions struct {
	// MaxMessage bounds response frames (DefaultMaxMessage).
	MaxMessage int
	// Metadata, if set, adds metadata to every call (e.g. the caller's
	// hybrid-logical-clock reading).
	Metadata func(ctx context.Context) map[string]string
	// Retries is how many extra attempts an Idempotent call makes after an
	// Unavailable error (default 2), with Backoff between them (50ms,
	// doubling).
	Retries int
	Backoff time.Duration
}

// Client makes calls over an endpoint, sharing one connection per peer.
type Client struct {
	ep   *endpoint.Endpoint
	opts ClientOptions
}

// NewClient returns a client using ep for connections.
func NewClient(ep *endpoint.Endpoint, opts ClientOptions) *Client {
	if opts.MaxMessage <= 0 {
		opts.MaxMessage = DefaultMaxMessage
	}
	if opts.Retries == 0 {
		opts.Retries = 2
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 50 * time.Millisecond
	}
	return &Client{ep: ep, opts: opts}
}

// CallOption modifies one call.
type CallOption func(*callOpts)

type callOpts struct{ idempotent bool }

// Idempotent marks a call safe to retry when the peer is Unavailable.
func Idempotent() CallOption { return func(o *callOpts) { o.idempotent = true } }

// Call makes a unary call to method on the node at addr.
func (c *Client) Call(ctx context.Context, addr, method string, req []byte, opts ...CallOption) ([]byte, error) {
	var resp []byte
	err := c.do(ctx, addr, method, req, opts, func(b []byte) error {
		if resp != nil {
			return Errorf(Internal, "unary method %s returned more than one message", method)
		}
		resp = b
		return nil
	})
	if err == nil && resp == nil {
		resp = []byte{}
	}
	return resp, err
}

// CallStream makes a server-streaming call; recv sees each message in order.
// An error from recv cancels the call and is returned.
func (c *Client) CallStream(ctx context.Context, addr, method string, req []byte, recv func([]byte) error, opts ...CallOption) error {
	return c.do(ctx, addr, method, req, opts, recv)
}

func (c *Client) do(ctx context.Context, addr, method string, req []byte, opts []CallOption, recv func([]byte) error) error {
	var o callOpts
	for _, f := range opts {
		f(&o)
	}
	backoff := c.opts.Backoff
	for attempt := 0; ; attempt++ {
		err, sent := c.once(ctx, addr, method, req, recv)
		if err == nil || CodeOf(err) != Unavailable || attempt >= c.opts.Retries || (sent && !o.idempotent) {
			return err
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return ctxErr(ctx)
		}
	}
}

func ctxErr(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: DeadlineExceeded, Message: "deadline exceeded"}
	}
	return &Error{Code: Canceled, Message: "call canceled"}
}

// once makes one attempt. sent reports whether the request may have reached
// the server (after which only idempotent calls are retried).
func (c *Client) once(ctx context.Context, addr, method string, payload []byte, recv func([]byte) error) (err error, sent bool) {
	if err := ctx.Err(); err != nil {
		return ctxErr(ctx), false
	}
	conn, err := c.ep.Conn(ctx, addr)
	if err != nil {
		if ctx.Err() != nil {
			return ctxErr(ctx), false
		}
		return &Error{Code: Unavailable, Message: "connect " + addr + ": " + err.Error()}, false
	}
	st, err := conn.OpenStream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctxErr(ctx), false
		}
		return &Error{Code: Unavailable, Message: "open stream: " + err.Error()}, false
	}
	req := request{method: method, payload: payload}
	if dl, ok := ctx.Deadline(); ok {
		left := time.Until(dl)
		if left <= 0 {
			st.Reset(uint64(ErrCode))
			return ctxErr(ctx), false
		}
		req.deadline = uint64(left / time.Microsecond)
		st.SetDeadline(dl)
	}
	md := map[string]string{}
	if c.opts.Metadata != nil {
		for k, v := range c.opts.Metadata(ctx) {
			md[k] = v
		}
	}
	for k, v := range OutgoingMetadata(ctx) {
		md[k] = v
	}
	if len(md) > 0 {
		req.md = md
	}
	stop := context.AfterFunc(ctx, func() {
		st.Reset(uint64(ErrCode))
		st.CancelRead(uint64(ErrCode))
	})
	defer stop()

	if _, err := st.Write(req.encode()); err != nil {
		if ctx.Err() != nil {
			return ctxErr(ctx), true
		}
		return &Error{Code: Unavailable, Message: "send: " + err.Error()}, true
	}
	r := bufio.NewReader(st)
	for {
		kind, body, err := readFrame(r, c.opts.MaxMessage)
		if err != nil {
			st.Reset(uint64(ErrCode))
			if ctx.Err() != nil {
				return ctxErr(ctx), true
			}
			var e *Error
			if errors.As(err, &e) {
				return e, true
			}
			return &Error{Code: Unavailable, Message: "receive: " + err.Error()}, true
		}
		switch kind {
		case kindData:
			if err := recv(body); err != nil {
				st.Reset(uint64(ErrCode))
				st.CancelRead(uint64(ErrCode))
				return err, true
			}
		case kindEnd:
			st.Close()
			return nil, true
		case kindErr:
			st.Close()
			return parseErr(body), true
		default:
			st.Reset(uint64(ErrCode))
			return &Error{Code: DataLoss, Message: "unknown frame kind"}, true
		}
	}
}

// ---- typed helpers (JSON) ----

// Unary adapts a typed function to a Handler; payloads are JSON.
func Unary[Req, Resp any](f func(ctx context.Context, req *Req) (*Resp, error)) Handler {
	return func(ctx context.Context, b []byte) ([]byte, error) {
		var req Req
		if len(b) > 0 {
			if err := json.Unmarshal(b, &req); err != nil {
				return nil, Errorf(InvalidArgument, "decode request: %v", err)
			}
		}
		resp, err := f(ctx, &req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(resp)
	}
}

// Invoke makes a typed unary call; payloads are JSON.
func Invoke[Req, Resp any](ctx context.Context, c *Client, addr, method string, req *Req, opts ...CallOption) (*Resp, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, Errorf(InvalidArgument, "encode request: %v", err)
	}
	out, err := c.Call(ctx, addr, method, b, opts...)
	if err != nil {
		return nil, err
	}
	var resp Resp
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, Errorf(DataLoss, "decode response: %v", err)
	}
	return &resp, nil
}
