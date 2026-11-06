package endpoint

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func identity(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

func cfg(seed byte) *ktp.Config {
	return &ktp.Config{Identity: identity(seed), ClusterKey: []byte("cluster"), IdleTimeout: 10 * time.Second}
}

func simEndpoint(t *testing.T, n *simnet.Network, addr string, seed byte) *Endpoint {
	t.Helper()
	pc, err := n.Listen(addr)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(pc, cfg(seed), Options{Resolve: simnet.Resolve})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// echoServer echoes every stream on every accepted connection.
func echoServer(e *Endpoint) {
	for {
		c, err := e.Accept(context.Background())
		if err != nil {
			return
		}
		go func() {
			for {
				s, err := c.AcceptStream(context.Background())
				if err != nil {
					return
				}
				go func() {
					io.Copy(s, s)
					s.Close()
				}()
			}
		}()
	}
}

func TestEchoManyStreamsOverLossyNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(1)
		n.SetDefaultLink(simnet.Link{Latency: 10 * time.Millisecond, Jitter: 3 * time.Millisecond,
			Loss: 0.03, Duplicate: 0.02, Reorder: 0.05})
		a, b := simEndpoint(t, n, "a", 1), simEndpoint(t, n, "b", 2)
		go echoServer(b)

		ctx := context.Background()
		const streams, size = 16, 48 << 10
		var wg sync.WaitGroup
		errs := make(chan error, streams)
		for k := 0; k < streams; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				c, err := a.Conn(ctx, "b")
				if err != nil {
					errs <- err
					return
				}
				s, err := c.OpenStream(ctx)
				if err != nil {
					errs <- err
					return
				}
				rng := rand.New(rand.NewPCG(uint64(k), 1))
				msg := make([]byte, size)
				for i := range msg {
					msg[i] = byte(rng.IntN(256))
				}
				go func() { s.Write(msg); s.Close() }()
				got, err := io.ReadAll(s)
				if err != nil {
					errs <- fmt.Errorf("stream %d: %w", k, err)
					return
				}
				if !bytes.Equal(got, msg) {
					errs <- fmt.Errorf("stream %d: echo mismatch (%d bytes)", k, len(got))
				}
			}(k)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if d := a.Stats().Dialed; d != 1 {
			t.Fatalf("Conn dialed %d connections for one peer, want 1", d)
		}
		a.Close()
		b.Close()
	})
}

func TestRealUDPLoopback(t *testing.T) {
	a, err := Listen("127.0.0.1:0", cfg(1), Options{})
	if err != nil {
		t.Skipf("no UDP: %v", err)
	}
	defer a.Close()
	b, err := Listen("127.0.0.1:0", cfg(2), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	go echoServer(b)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := a.Dial(ctx, b.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c.PeerIdentity(), identity(2).Public().(ed25519.PublicKey)) {
		t.Fatal("wrong peer identity")
	}
	s, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("kvstore over KTP over UDP "), 4000)
	go func() { s.Write(msg); s.Close() }()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo: %d bytes, err %v", len(got), err)
	}
	if st := c.Stats(); st.SRTT <= 0 || st.PacketsSent == 0 {
		t.Fatalf("stats not populated: %+v", st)
	}
}

func TestRawDatagrams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(2)
		a, b := simEndpoint(t, n, "a", 1), simEndpoint(t, n, "b", 2)
		// An outsider with another cluster key: its gossip must be ignored.
		pc, _ := n.Listen("evil")
		evilCfg := cfg(3)
		evilCfg.ClusterKey = []byte("other")
		evil, _ := New(pc, evilCfg, Options{Resolve: simnet.Resolve})

		evil.SendRaw("b", []byte("forged"))
		a.SendRaw("b", []byte("ping"))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p, err := b.RecvRaw(ctx)
		if err != nil || string(p.Payload) != "ping" || p.From != "a" {
			t.Fatalf("got %+v %v", p, err)
		}
		if _, err := b.RecvRaw(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("forged datagram delivered: %v", err)
		}
		a.Close()
		b.Close()
		evil.Close()
	})
}

func TestDialUnreachableHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(3)
		a := simEndpoint(t, n, "a", 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		start := time.Now()
		if _, err := a.Dial(ctx, "nobody"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial: %v", err)
		}
		if el := time.Since(start); el != 2*time.Second {
			t.Fatalf("dial returned after %v", el)
		}
		a.Close()
	})
}

// A peer that crashes and restarts on the same address loses its connection
// state; our stale connection must die fast (stateless reset) and Conn must
// transparently redial.
func TestPeerRestartIsDetectedAndRedialed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(4)
		n.SetDefaultLink(simnet.Link{Latency: 5 * time.Millisecond})
		a := simEndpoint(t, n, "a", 1)
		bpc, _ := n.Listen("b")
		b, _ := New(bpc, cfg(2), Options{Resolve: simnet.Resolve})
		go echoServer(b)
		ctx := context.Background()
		c1, err := a.Conn(ctx, "b")
		if err != nil {
			t.Fatal(err)
		}
		// Crash: the socket vanishes without CONNECTION_CLOSE.
		bpc.Close()
		b2 := simEndpoint(t, n, "b", 2)
		go echoServer(b2)

		s, _ := c1.OpenStream(ctx)
		s.Write([]byte("anyone?"))
		select {
		case <-c1.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("stale connection not torn down by the restarted peer's reset")
		}
		if !errors.Is(c1.Err(), ktp.ErrStatelessReset) {
			t.Fatalf("stale conn err = %v", c1.Err())
		}
		c2, err := a.Conn(ctx, "b")
		if err != nil || c2 == c1 {
			t.Fatalf("redial: %v (same conn %v)", err, c2 == c1)
		}
		a.Close()
		b2.Close()
		b.Close()
	})
}

func TestStreamReadDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(5)
		a, b := simEndpoint(t, n, "a", 1), simEndpoint(t, n, "b", 2)
		go func() {
			c, err := b.Accept(context.Background())
			if err != nil {
				return
			}
			c.AcceptStream(context.Background()) // accept, never answer
		}()
		ctx := context.Background()
		c, err := a.Dial(ctx, "b")
		if err != nil {
			t.Fatal(err)
		}
		s, _ := c.OpenStream(ctx)
		s.Write([]byte("hello"))
		s.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, err = s.Read(make([]byte, 10))
		var ne net.Error
		if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("read past deadline: %v", err)
		}
		a.Close()
		b.Close()
	})
}

func TestCloseWithErrorReachesPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(6)
		a, b := simEndpoint(t, n, "a", 1), simEndpoint(t, n, "b", 2)
		accepted := make(chan *Conn, 1)
		go func() {
			c, _ := b.Accept(context.Background())
			accepted <- c
		}()
		c, err := a.Dial(context.Background(), "b")
		if err != nil {
			t.Fatal(err)
		}
		peer := <-accepted
		c.CloseWithError(ktp.CodeApplication+1, "shutting down")
		<-peer.Done()
		var ce *ktp.CloseError
		if !errors.As(peer.Err(), &ce) || ce.Code != ktp.CodeApplication+1 || !ce.Remote {
			t.Fatalf("peer saw %v", peer.Err())
		}
		a.Close()
		b.Close()
	})
}
