package rpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

type env struct {
	net    *simnet.Network
	cli    *endpoint.Endpoint
	srvEP  *endpoint.Endpoint
	server *Server
	client *Client
	cancel context.CancelFunc
}

func ep(t *testing.T, n *simnet.Network, addr string, seed byte) *endpoint.Endpoint {
	t.Helper()
	pc, err := n.Listen(addr)
	if err != nil {
		t.Fatal(err)
	}
	e, err := endpoint.New(pc, &ktp.Config{Identity: key(seed), ClusterKey: []byte("k"),
		HandshakeTimeout: 300 * time.Millisecond}, endpoint.Options{Resolve: simnet.Resolve})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func setup(t *testing.T) *env {
	n := simnet.New(1)
	n.SetDefaultLink(simnet.Link{Latency: 5 * time.Millisecond, Loss: 0.01})
	e := &env{net: n, cli: ep(t, n, "client", 1), srvEP: ep(t, n, "server", 2), server: NewServer()}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	go e.server.Serve(ctx, e.srvEP)
	e.client = NewClient(e.cli, ClientOptions{})
	return e
}

func (e *env) close() {
	e.cancel()
	e.cli.Close()
	e.srvEP.Close()
}

func TestUnaryMetadataAndPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		e.server.Register("echo", func(ctx context.Context, req []byte) ([]byte, error) {
			p, _ := PeerFrom(ctx)
			if !bytes.Equal(p.Identity, key(1).Public().(ed25519.PublicKey)) {
				return nil, Errorf(Unauthenticated, "unexpected caller")
			}
			return append([]byte(Metadata(ctx)["greeting"]+":"), req...), nil
		})
		ctx := WithMetadata(context.Background(), "greeting", "hi")
		got, err := e.client.Call(ctx, "server", "echo", []byte("there"))
		if err != nil || string(got) != "hi:there" {
			t.Fatalf("got %q %v", got, err)
		}
	})
}

type addReq struct{ A, B int }
type addResp struct{ Sum int }

func TestTypedHelpersAndErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		e.server.Register("add", Unary(func(ctx context.Context, r *addReq) (*addResp, error) {
			if r.A < 0 {
				return nil, Errorf(InvalidArgument, "negative")
			}
			return &addResp{Sum: r.A + r.B}, nil
		}))
		e.server.Register("panic", func(context.Context, []byte) ([]byte, error) { panic("boom") })
		ctx := context.Background()
		r, err := Invoke[addReq, addResp](ctx, e.client, "server", "add", &addReq{2, 3})
		if err != nil || r.Sum != 5 {
			t.Fatalf("add: %+v %v", r, err)
		}
		if _, err := Invoke[addReq, addResp](ctx, e.client, "server", "add", &addReq{-1, 0}); CodeOf(err) != InvalidArgument {
			t.Fatalf("error code lost: %v", err)
		}
		if _, err := e.client.Call(ctx, "server", "nope", nil); CodeOf(err) != Unimplemented {
			t.Fatalf("unknown method: %v", err)
		}
		if _, err := e.client.Call(ctx, "server", "panic", nil); CodeOf(err) != Internal {
			t.Fatalf("panic: %v", err)
		}
		// The server survived the panic.
		if r, err := Invoke[addReq, addResp](ctx, e.client, "server", "add", &addReq{1, 1}); err != nil || r.Sum != 2 {
			t.Fatalf("after panic: %v", err)
		}
	})
}

func TestDeadlinePropagatesAndCancelsHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		cancelled := make(chan error, 1)
		e.server.Register("slow", func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-time.After(time.Minute):
				return []byte("late"), nil
			case <-ctx.Done():
				cancelled <- ctx.Err()
				return nil, ctx.Err()
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := e.client.Call(ctx, "server", "slow", nil)
		if CodeOf(err) != DeadlineExceeded {
			t.Fatalf("got %v", err)
		}
		if el := time.Since(start); el > 250*time.Millisecond {
			t.Fatalf("call took %v past a 200ms deadline", el)
		}
		select {
		case err := <-cancelled:
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				t.Fatalf("handler ctx err %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("handler context was never cancelled")
		}
	})
}

func TestCallerCancellationReachesServer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		started := make(chan struct{})
		gone := make(chan struct{})
		e.server.Register("wait", func(ctx context.Context, _ []byte) ([]byte, error) {
			close(started)
			<-ctx.Done()
			close(gone)
			return nil, ctx.Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := e.client.Call(ctx, "server", "wait", nil); done <- err }()
		<-started
		cancel()
		if err := <-done; CodeOf(err) != Canceled {
			t.Fatalf("caller got %v", err)
		}
		select {
		case <-gone:
		case <-time.After(time.Second):
			t.Fatal("server handler not cancelled when the caller gave up")
		}
	})
}

func TestServerStreaming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		e.server.RegisterStream("count", func(ctx context.Context, req []byte, send func([]byte) error) error {
			for i := 0; i < 200; i++ {
				if err := send([]byte(fmt.Sprint(i))); err != nil {
					return err
				}
			}
			return nil
		})
		var got []string
		err := e.client.CallStream(context.Background(), "server", "count", nil, func(b []byte) error {
			got = append(got, string(b))
			return nil
		})
		if err != nil || len(got) != 200 || got[199] != "199" {
			t.Fatalf("stream: %d msgs, err %v", len(got), err)
		}
		stop := errors.New("enough")
		n := 0
		err = e.client.CallStream(context.Background(), "server", "count", nil, func([]byte) error {
			n++
			if n == 5 {
				return stop
			}
			return nil
		})
		if !errors.Is(err, stop) || n != 5 {
			t.Fatalf("early stop: n=%d err=%v", n, err)
		}
	})
}

func TestIdempotentRetryOnUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(2)
		cli := ep(t, n, "client", 1)
		defer cli.Close()
		client := NewClient(cli, ClientOptions{Retries: 6, Backoff: 100 * time.Millisecond})
		// Nobody is listening yet: fails fast as Unavailable without retries.
		if _, err := client.Call(context.Background(), "late", "x", nil); CodeOf(err) != Unavailable {
			t.Fatalf("got %v", err)
		}
		// The server comes up while an idempotent call is retrying.
		late := make(chan *endpoint.Endpoint, 1)
		go func() {
			time.Sleep(500 * time.Millisecond)
			srv := ep(t, n, "late", 2)
			s := NewServer()
			s.Register("x", func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil })
			go s.Serve(context.Background(), srv)
			late <- srv
		}()
		got, err := client.Call(context.Background(), "late", "x", nil, Idempotent())
		if err != nil || string(got) != "ok" {
			t.Fatalf("retry: %q %v", got, err)
		}
		(<-late).Close()
	})
}

func TestManyConcurrentCallsAndInterceptors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		var order []string
		var mu sync.Mutex
		for _, name := range []string{"outer", "inner"} {
			name := name
			e.server.Use(func(ctx context.Context, m string, req []byte, next Handler) ([]byte, error) {
				mu.Lock()
				if len(order) < 2 {
					order = append(order, name)
				}
				mu.Unlock()
				return next(ctx, req)
			})
		}
		var calls atomic.Int64
		e.server.Register("inc", func(context.Context, []byte) ([]byte, error) {
			return []byte(fmt.Sprint(calls.Add(1))), nil
		})
		var wg sync.WaitGroup
		errs := make(chan error, 300)
		for i := 0; i < 300; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.client.Call(context.Background(), "server", "inc", nil); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if calls.Load() != 300 {
			t.Fatalf("calls %d", calls.Load())
		}
		if strings.Join(order, ",") != "outer,inner" {
			t.Fatalf("interceptor order %v", order)
		}
		st := e.server.Stats()
		if len(st) != 1 || st[0].Calls != 300 || st[0].InFlight != 0 {
			t.Fatalf("stats %+v", st)
		}
	})
}

func TestMessageLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		e.server.MaxMessage = 1024
		e.server.Register("sink", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		_, err := e.client.Call(context.Background(), "server", "sink", make([]byte, 4096))
		if c := CodeOf(err); c != ResourceExhausted && c != Unknown {
			t.Fatalf("oversize request: %v", err)
		}
	})
}

func FuzzReadRequest(f *testing.F) {
	f.Add(request{method: "m", deadline: 5, md: map[string]string{"a": "b"}, payload: []byte("p")}.encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		req, err := readRequest(bufio.NewReader(bytes.NewReader(b)), 1<<16)
		if err != nil {
			return
		}
		again, err := readRequest(bufio.NewReader(bytes.NewReader(req.encode())), 1<<16)
		if err != nil || again.method != req.method || !bytes.Equal(again.payload, req.payload) {
			t.Fatalf("request does not round-trip: %v", err)
		}
	})
}
