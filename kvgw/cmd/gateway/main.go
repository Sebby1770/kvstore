// gateway — HTTP front door for a fleet of kvnodes. Each key is replicated to
// RF nodes chosen by rendezvous hashing; reads and writes use quorums (R, W).
// This is the single-process 1.x deployment; for a highly available mesh of
// coordinators with a Raft control plane, run cmd/kvmesh instead.
//
//	gateway --listen :8080 --nodes n0,n1,n2 --rf 3 --read 2 --write 2
//
// Nodes given as ktp://host:port are reached over KTP (encrypted and
// authenticated, keyed to --cluster-key); host:port over plain TCP.
//
//	gateway --nodes ktp://10.0.0.1:7170,ktp://10.0.0.2:7170,ktp://10.0.0.3:7170 --cluster-key s3cret
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/client"
	"github.com/Sebby1770/kvstore/kvgw/internal/cluster"
	"github.com/Sebby1770/kvstore/kvgw/internal/httpapi"
	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	nodesCSV := flag.String("nodes", "127.0.0.1:7070", "comma-separated kvnode addresses")
	timeout := flag.Duration("timeout", 2*time.Second, "per-call dial/read timeout")
	rf := flag.Int("rf", 3, "replication factor (clamped to node count)")
	rq := flag.Int("read", 2, "read quorum R")
	wq := flag.Int("write", 2, "write quorum W")
	ae := flag.Duration("anti-entropy", 0, "background repair sweep interval (0 = off)")
	hb := flag.Duration("heartbeat", 2*time.Second, "failure-detection ping interval (0 = off)")
	key := flag.String("cluster-key", os.Getenv("KVMESH_CLUSTER_KEY"), "cluster secret for ktp:// nodes (or $KVMESH_CLUSTER_KEY)")
	ktpListen := flag.String("ktp-listen", ":0", "UDP address of the KTP endpoint used for ktp:// nodes")
	flag.Parse()

	addrs := strings.Split(*nodesCSV, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	if len(addrs) == 0 || addrs[0] == "" {
		log.Fatal("--nodes is required")
	}

	// A KTP endpoint for ktp:// storage nodes: one encrypted connection per
	// node, shared by every request (and by nodes added at runtime).
	var ep *endpoint.Endpoint
	for _, a := range addrs {
		if client.IsKTP(a) {
			ep = ktpEndpoint(*ktpListen, *key)
			defer ep.Close()
			break
		}
	}
	newNode := func(addr string) kv.Node {
		c, err := client.Open(addr, ep, *timeout)
		if err != nil {
			log.Printf("gateway: %v", err)
		}
		return c
	}
	nodes := make(map[string]kv.Node, len(addrs))
	for _, a := range addrs {
		c, err := client.Open(a, ep, *timeout)
		if err != nil {
			log.Fatalf("gateway: --nodes: %v", err)
		}
		nodes[a] = c
	}
	cl := cluster.New(nodes, addrs, *rf, *rq, *wq)

	api := httpapi.New(cl, httpapi.Options{Timeout: *timeout, NodeFactory: newNode})

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 3 * time.Second,
	}

	stopBG := make(chan struct{})
	defer close(stopBG)

	if *ae > 0 {
		go func() {
			tk := time.NewTicker(*ae)
			defer tk.Stop()
			for {
				select {
				case <-stopBG:
					return
				case <-tk.C:
					if n, errs := cl.RepairSweep(); n > 0 || len(errs) > 0 {
						log.Printf("anti-entropy: %d repairs, errs=%v", n, errs)
					}
				}
			}
		}()
		log.Printf("anti-entropy: sweeping every %s", *ae)
	}

	if *hb > 0 {
		go cl.RunHeartbeat(*hb, stopBG)
		log.Printf("heartbeat: pinging every %s", *hb)
	}

	go func() {
		log.Printf("gateway: listening on %s, nodes=%v, RF=%d R=%d W=%d (consistent=%v)",
			*listen, addrs, cl.RF(), cl.R(), cl.W(), cl.Consistent())
		if !cl.Consistent() {
			log.Printf("gateway: warning — R+W <= RF, reads may miss recent writes")
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("gateway: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

// ktpEndpoint starts the client endpoint for ktp:// nodes. Its identity is
// fresh per process: storage nodes authenticate coordinators by the cluster
// key (or pin identities with kvnode --allow-peer).
func ktpEndpoint(listen, key string) *endpoint.Endpoint {
	if key == "" {
		log.Printf("gateway: warning: no --cluster-key; ktp:// traffic is encrypted but any kvnode without a key will accept it")
	}
	_, id, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatalf("gateway: identity: %v", err)
	}
	cfg := &ktp.Config{Identity: id, ClusterKey: []byte(key), Congestion: "cubic",
		KeepAlive: 5 * time.Second, IdleTimeout: 30 * time.Second, HandshakeTimeout: 3 * time.Second}
	ep, err := endpoint.Listen(listen, cfg, endpoint.Options{Logf: log.Printf})
	if err != nil {
		log.Fatalf("gateway: ktp endpoint: %v", err)
	}
	log.Printf("gateway: ktp endpoint on %s (identity %x)", ep.LocalAddr(), id.Public().(ed25519.PublicKey)[:8])
	return ep
}
