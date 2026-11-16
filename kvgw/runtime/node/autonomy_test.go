package node

import (
	"encoding/json"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func decode(t *testing.T, body string) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("bad JSON %q: %v", body, err)
	}
	return v
}

// Every autonomy layer, through the HTTP API of a three-node mesh.
func TestMeshAutonomy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newMesh(t, 3)
		defer m.closeAll()
		m.leader()
		time.Sleep(3 * time.Second) // membership, time sync and digests settle

		// CRDTs: concurrent updates on every node converge everywhere.
		for _, n := range []string{"n1", "n2", "n3"} {
			if code, body := m.do(n, "POST", "/replica/counter/visits", `{"action":"add","amount":5}`); code != 200 {
				t.Fatalf("crdt add on %s: %d %s", n, code, body)
			}
			m.do(n, "POST", "/replica/set/tags", `{"action":"add","value":"`+n+`"}`)
		}
		// A remove deletes only the adds it has observed (add-wins): let n1's
		// add reach n2 first.
		time.Sleep(500 * time.Millisecond)
		m.do("n2", "POST", "/replica/set/tags", `{"action":"remove","value":"n1"}`)
		time.Sleep(2 * time.Second)
		for _, n := range []string{"n1", "n2", "n3"} {
			_, body := m.do(n, "GET", "/replica/visits", "")
			if v := decode(t, body)["value"]; v != 15.0 {
				_, all := m.do(n, "GET", "/replica", "")
				t.Fatalf("%s sees counter %v, want 15 (%s; all: %s)", n, v, body, all)
			}
			_, body = m.do(n, "GET", "/replica/tags", "")
			if v := decode(t, body)["value"]; len(v.([]any)) != 2 {
				t.Fatalf("%s sees set %v, want [n2 n3]", n, v)
			}
		}
		if code, _ := m.do("n1", "POST", "/replica/set/visits", `{"action":"add","value":"x"}`); code != 409 {
			t.Fatalf("kind clash accepted: %d", code)
		}

		// Time: n1 (lowest name) is grandmaster; followers are disciplined
		// with a bounded error.
		_, body := m.do("n3", "GET", "/mesh/time", "")
		ts := decode(t, body)
		clock := ts["clock"].(map[string]any)
		if ts["grandmaster"] != "n1:7000" || clock["synced"] != true || clock["samples"].(float64) == 0 {
			t.Fatalf("n3 time status: %s", body)
		}

		// Speculative read through any coordinator.
		if code, _ := m.do("n1", "PUT", "/kv/fast", "lane"); code != 204 {
			t.Fatal("kv put failed")
		}
		code, body := m.do("n2", "GET", "/spec/kv/fast", "")
		if code != 200 || decode(t, body)["value"] != "lane" {
			t.Fatalf("speculative get: %d %s", code, body)
		}
		if code, _ := m.do("n2", "GET", "/spec/kv/absent", ""); code != 404 {
			t.Fatalf("speculative get of a missing key: %d", code)
		}

		// Fabric: n3 reads n1's index with one-sided verbs.
		if code, body := m.do("n1", "PUT", "/fabric/greeting", "hello from n1's memory"); code != 200 {
			t.Fatalf("fabric put: %d %s", code, body)
		}
		code, body = m.do("n3", "GET", "/fabric/n1/greeting", "")
		if code != 200 || decode(t, body)["value"] != "hello from n1's memory" {
			t.Fatalf("one-sided read: %d %s", code, body)
		}
		_, body = m.do("n1", "GET", "/mesh/fabric", "")
		if served := decode(t, body)["stats"].(map[string]any)["verbs_served"].(float64); served == 0 {
			t.Fatal("n1 served no verbs")
		}

		// Data path: heartbeat program answered probes; foresight sees
		// healthy peers; backpressure state is reported.
		_, body = m.do("n2", "GET", "/mesh/offload", "")
		if !strings.Contains(body, `"heartbeat"`) || decode(t, body)["filter"].(map[string]any)["TX"].(float64) == 0 {
			t.Fatalf("heartbeat responder idle: %s", body)
		}
		_, body = m.do("n2", "GET", "/mesh/foresight", "")
		var fs []map[string]any
		json.Unmarshal([]byte(body), &fs)
		if len(fs) != 2 {
			t.Fatalf("foresight peers: %s", body)
		}
		for _, f := range fs {
			if f["state"] != "healthy" {
				t.Fatalf("a healthy mesh forecast as %v: %v", f["state"], f)
			}
		}
		_, body = m.do("n1", "GET", "/mesh/pressure", "")
		if !strings.Contains(body, `"max_concurrent":512`) {
			t.Fatalf("pressure: %s", body)
		}
	})
}

// A leader whose network is going grey hands leadership over before its
// links fail, instead of leaving the cluster to time it out.
func TestForesightHandsLeadershipOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newMesh(t, 3)
		defer m.closeAll()
		lead := m.leader()
		time.Sleep(15 * time.Second) // let foresight learn the healthy links
		var others []string
		for name := range m.nodes {
			if name != lead.Name() {
				others = append(others, name+":7000")
			}
		}
		// The leader's network degrades over ~4 seconds.
		for step := 1; step <= 20; step++ {
			f := float64(step) / 20
			l := simnet.Link{Latency: 2*time.Millisecond + time.Duration(f*80)*time.Millisecond, Jitter: time.Millisecond, Loss: f * 0.12}
			for _, o := range others {
				m.net.SetLink(lead.Addr(), o, l)
				m.net.SetLink(o, lead.Addr(), l)
			}
			time.Sleep(200 * time.Millisecond)
			if !lead.Control().IsLeader() {
				break
			}
		}
		// A transfer can be lost on the degraded links; the leader retries
		// every ten probe intervals until it takes.
		for i := 0; i < 50 && lead.Control().IsLeader(); i++ {
			time.Sleep(200 * time.Millisecond)
		}
		handedOver := false
		for _, e := range lead.events.recent(200) {
			if e.Kind == "foresight.handover" {
				handedOver = true
			}
		}
		if !handedOver {
			_, body := m.do(lead.Name(), "GET", "/mesh/foresight", "")
			t.Fatalf("leader never handed over; its forecasts: %s", body)
		}
		if lead.Control().IsLeader() {
			t.Fatal("handover was announced but the old leader still leads")
		}
	})
}
