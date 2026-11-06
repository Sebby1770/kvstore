// ktpcat — netcat and a benchmark for KTP, KVStore's transport protocol.
//
//	ktpcat -l 127.0.0.1:9000                 # server: print what arrives (or -echo)
//	ktpcat 127.0.0.1:9000 < file             # client: send stdin, print replies
//	ktpcat -bench 64MiB 127.0.0.1:9000       # client: measure goodput against a server
//	ktpcat -sim -loss 0.02 -latency 40ms -bw 10MB -cc cubic -bench 32MiB
//	                                         # both ends in-process over a simulated link,
//	                                         # tracing cwnd / RTT / loss as it runs
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		m      int64
	}{{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}} {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.m
			break
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	return int64(v * float64(mult)), err
}

func config(key string, insecure bool, cc string) *ktp.Config {
	_, id, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	return &ktp.Config{Identity: id, ClusterKey: []byte(key), Insecure: insecure, Congestion: cc,
		InitialMaxData: 16 << 20, InitialMaxStreamData: 8 << 20, MaxSendBuffer: 8 << 20}
}

func main() {
	listen := flag.String("l", "", "listen on this UDP address (server mode)")
	echo := flag.Bool("echo", false, "server: echo streams back instead of printing")
	bench := flag.String("bench", "", "client: send this many bytes and report goodput (e.g. 64MiB)")
	key := flag.String("key", "", "cluster key (both ends must match)")
	insecure := flag.Bool("insecure", false, "no AEAD (integrity only)")
	cc := flag.String("cc", "bbr", "congestion controller: bbr, cubic or newreno")
	sim := flag.Bool("sim", false, "run client and server in-process over a simulated link")
	loss := flag.Float64("loss", 0, "sim: packet loss probability")
	latency := flag.Duration("latency", 20*time.Millisecond, "sim: one-way latency")
	jitter := flag.Duration("jitter", 0, "sim: one-way jitter")
	bw := flag.String("bw", "0", "sim: bandwidth per direction (e.g. 10MB = 10 MB/s; 0 = unlimited)")
	queue := flag.String("queue", "256KB", "sim: router queue on a limited link")
	flag.Parse()

	switch {
	case *sim:
		size := int64(16 << 20)
		if *bench != "" {
			var err error
			if size, err = parseSize(*bench); err != nil {
				log.Fatalf("ktpcat: -bench: %v", err)
			}
		}
		b, err := parseSize(*bw)
		if err != nil {
			log.Fatalf("ktpcat: -bw: %v", err)
		}
		q, _ := parseSize(*queue)
		runSim(size, simnet.Link{Latency: *latency, Jitter: *jitter, Loss: *loss, Bandwidth: int(b), QueueBytes: int(q)},
			*key, *insecure, *cc)
	case *listen != "":
		serve(*listen, *echo, config(*key, *insecure, *cc))
	case flag.NArg() == 1:
		ep, err := endpoint.Listen("0.0.0.0:0", config(*key, *insecure, *cc), endpoint.Options{})
		if err != nil {
			log.Fatal(err)
		}
		defer ep.Close()
		if *bench != "" {
			size, err := parseSize(*bench)
			if err != nil {
				log.Fatalf("ktpcat: -bench: %v", err)
			}
			if err := runBench(ep, flag.Arg(0), size, nil); err != nil {
				log.Fatal(err)
			}
			return
		}
		cat(ep, flag.Arg(0))
	default:
		fmt.Fprintln(os.Stderr, "usage: ktpcat -l ADDR [-echo] | ktpcat [-bench SIZE] ADDR | ktpcat -sim [-loss P -latency D -bw RATE -bench SIZE]")
		os.Exit(2)
	}
}

func serve(addr string, echo bool, cfg *ktp.Config) {
	ep, err := endpoint.Listen(addr, cfg, endpoint.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ktpcat: listening on %s (identity %x)", ep.LocalAddr(), cfg.Identity.Public().(ed25519.PublicKey)[:8])
	serveEndpoint(ep, echo, os.Stdout, true)
}

func serveEndpoint(ep *endpoint.Endpoint, echo bool, out io.Writer, verbose bool) {
	for {
		c, err := ep.Accept(context.Background())
		if err != nil {
			return
		}
		if verbose {
			log.Printf("ktpcat: connection from %s (peer %x)", c.RemoteAddr(), c.PeerIdentity()[:8])
		}
		go func() {
			for {
				s, err := c.AcceptStream(context.Background())
				if err != nil {
					return
				}
				go func() {
					defer s.Close()
					if echo {
						io.Copy(s, s)
						return
					}
					n, _ := io.Copy(out, s)
					s.Write([]byte(strconv.FormatInt(n, 10))) // byte count back to benchmarks
				}()
			}
		}()
	}
}

func cat(ep *endpoint.Endpoint, addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := ep.Dial(ctx, addr)
	if err != nil {
		log.Fatalf("ktpcat: dial %s: %v", addr, err)
	}
	s, err := c.OpenStream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		io.Copy(s, os.Stdin)
		s.Close()
	}()
	io.Copy(os.Stdout, s)
}

// runBench sends size bytes on one stream and reports goodput. trace, if
// set, is called periodically with the connection's stats.
func runBench(ep *endpoint.Endpoint, addr string, size int64, trace func(*endpoint.Conn, time.Duration, int64)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	c, err := ep.Dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	hs := time.Since(start)
	s, err := c.OpenStream(context.Background())
	if err != nil {
		return err
	}
	var sent atomic.Int64
	done := make(chan struct{})
	if trace != nil {
		go func() {
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					trace(c, time.Since(start), sent.Load())
				}
			}
		}()
	}
	t0 := time.Now()
	buf := make([]byte, 64<<10)
	for i := range buf {
		buf[i] = byte(i)
	}
	for sent.Load() < size {
		n := min(int64(len(buf)), size-sent.Load())
		w, err := s.Write(buf[:n])
		sent.Add(int64(w))
		if err != nil {
			return err
		}
	}
	s.Close()
	ack, err := io.ReadAll(s) // the server reports how many bytes it got
	elapsed := time.Since(t0)
	close(done)
	if err != nil {
		return err
	}
	got, _ := strconv.ParseInt(string(ack), 10, 64)
	st := c.Stats()
	fmt.Printf("\nsent %s in %v  goodput %.1f Mbit/s  (server received %s)\n", human(size), elapsed.Round(time.Millisecond),
		float64(size)*8/elapsed.Seconds()/1e6, human(got))
	fmt.Printf("handshake %v  srtt %v  min_rtt %v  cwnd %s  congestion %s\n", hs.Round(time.Microsecond),
		st.SRTT.Round(time.Microsecond), st.MinRTT.Round(time.Microsecond), human(int64(st.CWND)), st.Congestion)
	fmt.Printf("packets sent %d  lost %d  retransmitted %s  PTOs %d  congestion events %d\n",
		st.PacketsSent, st.PacketsLost, human(int64(st.StreamBytesRetx)), st.PTOCount, st.CongestionEvents)
	if got != size {
		return fmt.Errorf("server received %d of %d bytes", got, size)
	}
	return nil
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func runSim(size int64, link simnet.Link, key string, insecure bool, cc string) {
	n := simnet.New(time.Now().UnixNano())
	n.SetDefaultLink(link)
	pcA, _ := n.Listen("client")
	pcB, _ := n.Listen("server")
	opts := endpoint.Options{Resolve: simnet.Resolve}
	a, err := endpoint.New(pcA, config(key, insecure, cc), opts)
	if err != nil {
		log.Fatal(err)
	}
	b, err := endpoint.New(pcB, config(key, insecure, cc), opts)
	if err != nil {
		log.Fatal(err)
	}
	defer a.Close()
	defer b.Close()
	go serveEndpoint(b, false, io.Discard, false)
	fmt.Printf("simulated link: latency %v ± %v, loss %.2f%%, bandwidth %s/s, queue %s, congestion %s\n",
		link.Latency, link.Jitter, link.Loss*100, map[bool]string{true: "unlimited", false: human(int64(link.Bandwidth))}[link.Bandwidth == 0],
		human(int64(link.QueueBytes)), cc)
	fmt.Printf("%8s  %10s  %10s  %10s  %8s  %8s\n", "time", "sent", "cwnd", "srtt", "lost", "ptos")
	err = runBench(a, "server", size, func(c *endpoint.Conn, t time.Duration, sent int64) {
		st := c.Stats()
		fmt.Printf("%7.2fs  %10s  %10s  %10v  %8d  %8d\n", t.Seconds(), human(sent), human(int64(st.CWND)),
			st.SRTT.Round(time.Millisecond), st.PacketsLost, st.PTOCount)
	})
	if err != nil {
		log.Fatal(err)
	}
}
