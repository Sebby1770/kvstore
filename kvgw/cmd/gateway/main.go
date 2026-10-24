// gateway — HTTP front door for a fleet of kvnodes. Each key is replicated to
// RF nodes chosen by rendezvous hashing; reads and writes use quorums (R, W).
// This is the single-process 1.x deployment; for a highly available mesh of
// coordinators with a Raft control plane, run cmd/kvmesh instead.
//
//	gateway --listen :8080 --nodes n0,n1,n2 --rf 3 --read 2 --write 2
package main

import (
	"context"
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
	flag.Parse()

	addrs := strings.Split(*nodesCSV, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	if len(addrs) == 0 || addrs[0] == "" {
		log.Fatal("--nodes is required")
	}

	nodes := make(map[string]kv.Node, len(addrs))
	for _, a := range addrs {
		nodes[a] = client.New(a, *timeout)
	}
	cl := cluster.New(nodes, addrs, *rf, *rq, *wq)

	api := httpapi.New(cl, httpapi.Options{Timeout: *timeout})

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
