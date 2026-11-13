package segroute

import (
	"context"
	"crypto/ed25519"
	"io"
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

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{Left: 2, Segments: []string{"r2", "dst:7000"}}
	b := h.Encode([]byte("payload"))
	got, inner, err := Decode(b)
	if err != nil || got.Left != 2 || len(got.Segments) != 2 || got.Segments[1] != "dst:7000" || string(inner) != "payload" {
		t.Fatalf("%+v %q %v", got, inner, err)
	}
	for i := 1; i < len(b)-len("payload"); i++ {
		if _, _, err := Decode(b[:i]); err == nil {
			t.Fatalf("truncated header (%d bytes) decoded", i)
		}
	}
}

// A KTP connection source-routed through two relays: the handshake, a
// stream both ways, and the server sees the last relay as its peer.
func TestRelayChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(1)
		n.SetDefaultLink(simnet.Link{Latency: 3 * time.Millisecond})
		listen := func(a string) *simnet.PacketConn {
			pc, err := n.Listen(a)
			if err != nil {
				t.Fatal(err)
			}
			return pc
		}
		r1, r2 := NewRelay(listen("r1"), simnet.Resolve), NewRelay(listen("r2"), simnet.Resolve)
		defer r1.Close()
		defer r2.Close()
		cfg := func(s byte) *ktp.Config {
			return &ktp.Config{Identity: key(s), ClusterKey: []byte("sr"), HandshakeTimeout: time.Second}
		}
		srv, err := endpoint.New(listen("server"), cfg(2), endpoint.Options{Resolve: simnet.Resolve})
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		wc := Wrap(listen("client"), simnet.Resolve)
		wc.SetRoute("server", "r1", "r2")
		cli, err := endpoint.New(wc, cfg(1), endpoint.Options{Resolve: simnet.Resolve})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		peer := make(chan string, 1)
		go func() {
			c, err := srv.Accept(ctx)
			if err != nil {
				return
			}
			peer <- c.RemoteAddrString()
			st, _ := c.AcceptStream(ctx)
			io.Copy(st, st)
			st.Close()
		}()
		c, err := cli.Dial(ctx, "server")
		if err != nil {
			t.Fatalf("dial through the relays: %v", err)
		}
		st, _ := c.OpenStream(ctx)
		go func() { st.Write([]byte("via two relays")); st.Close() }()
		got, _ := io.ReadAll(st)
		if string(got) != "via two relays" {
			t.Fatalf("echo %q", got)
		}
		if p := <-peer; p != "r2" {
			t.Fatalf("server saw the client at %s, want the last relay r2", p)
		}
		s1, s2 := r1.Stats(), r2.Stats()
		if s1.Forwarded == 0 || s1.Returned == 0 || s2.Forwarded == 0 || s2.Returned == 0 {
			t.Fatalf("relays: r1 %+v r2 %+v", s1, s2)
		}
		// Round trip time reflects the longer path (3 hops each way).
		if rtt := c.Stats().SRTT; rtt < 15*time.Millisecond {
			t.Fatalf("srtt %v is shorter than the routed path", rtt)
		}
	})
}

func FuzzDecode(f *testing.F) {
	f.Add(Header{Left: 1, Segments: []string{"x"}}.Encode([]byte{1}))
	f.Fuzz(func(t *testing.T, b []byte) {
		if h, inner, err := Decode(b); err == nil {
			if _, again, err := Decode(h.Encode(inner)); err != nil || string(again) != string(inner) {
				t.Fatal("round trip")
			}
		}
	})
}
