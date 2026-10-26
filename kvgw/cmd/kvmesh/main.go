// kvmesh — one node of a KVStore mesh: KTP transport, SWIM membership, the
// Raft control plane, virtual actors and the KV coordinator in one process.
// Every node serves the full HTTP API; there is no single gateway.
//
//	# three founding nodes
//	kvmesh --name n1 --listen 127.0.0.1:7101 --http 127.0.0.1:8101 --data /tmp/n1 \
//	       --bootstrap n1=127.0.0.1:7101,n2=127.0.0.1:7102,n3=127.0.0.1:7103 \
//	       --kvnodes 127.0.0.1:7070,127.0.0.1:7071,127.0.0.1:7072 --cluster-key s3cret
//	# a fourth joins later
//	kvmesh --name n4 --listen 127.0.0.1:7104 --http 127.0.0.1:8104 --join 127.0.0.1:7101 --cluster-key s3cret
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/runtime/node"
)

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	name := flag.String("name", "", "unique node name (required)")
	listen := flag.String("listen", "127.0.0.1:7946", "UDP address for KTP and gossip")
	advertise := flag.String("advertise", "", "address peers dial (default --listen)")
	httpAddr := flag.String("http", "127.0.0.1:8090", "HTTP listen address")
	data := flag.String("data", "", "data directory (identity key, Raft WAL); empty = ephemeral")
	bootstrap := flag.String("bootstrap", "", "founding control-plane voters: name=addr,name=addr,...")
	join := flag.String("join", "", "comma-separated addresses of existing members")
	kvnodes := flag.String("kvnodes", "", "storage ring to install if none exists (kvnode host:port list)")
	rf := flag.Int("rf", 3, "replication factor")
	rq := flag.Int("read", 2, "read quorum R")
	wq := flag.Int("write", 2, "write quorum W")
	hb := flag.Duration("heartbeat", 2*time.Second, "kvnode heartbeat interval (0 = off)")
	ae := flag.Duration("anti-entropy", 0, "KV repair sweep interval (0 = off)")
	key := flag.String("cluster-key", os.Getenv("KVMESH_CLUSTER_KEY"), "cluster secret (or $KVMESH_CLUSTER_KEY)")
	insecure := flag.Bool("insecure", false, "KTP without encryption (debugging only)")
	maxVoters := flag.Int("max-voters", 5, "promote joining nodes to voters up to this many")
	quiet := flag.Bool("quiet", false, "less logging")
	flag.Parse()

	if *name == "" {
		log.Fatal("kvmesh: --name is required")
	}
	if *key == "" {
		log.Printf("kvmesh: warning: no --cluster-key; any host that can reach this port can join")
	}
	logf := log.Printf
	if *quiet {
		logf = func(string, ...any) {}
	}
	n, err := node.Start(node.Config{
		Name: *name, Listen: *listen, Advertise: *advertise, HTTP: *httpAddr, DataDir: *data,
		ClusterKey: []byte(*key), Insecure: *insecure, Bootstrap: splitList(*bootstrap), Join: splitList(*join),
		MaxVoters: *maxVoters, KVNodes: splitList(*kvnodes), RF: *rf, R: *rq, W: *wq,
		Heartbeat: *hb, AntiEntropy: *ae, Logf: logf,
	})
	if err != nil {
		log.Fatalf("kvmesh: %v", err)
	}
	log.Printf("kvmesh: %s up — mesh %s, http://%s (dashboard /mesh)", n.Name(), n.Addr(), n.HTTPAddr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("kvmesh: %s leaving the mesh", n.Name())
	n.Close()
}
