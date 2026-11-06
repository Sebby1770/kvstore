package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// meshCmd implements the commands that talk to a kvmesh node's runtime
// routes: mesh status, leases, job queues and actors.
func meshCmd(c *http.Client, gw, cmd string, args []string) {
	switch cmd {
	case "mesh":
		meshStatus(c, gw)
	case "lease":
		leaseCmd(c, gw, args)
	case "jobs":
		jobsCmd(c, gw, args)
	case "actor":
		if len(args) < 2 {
			fail("actor TYPE KEY [MESSAGE]   e.g. actor counter visits inc")
		}
		msg := "get"
		if len(args) > 2 {
			msg = strings.Join(args[2:], " ")
		}
		var r struct {
			Reply, Node, Error string
		}
		status := call(c, "POST", gw+"/actor/"+url.PathEscape(args[0])+"/"+url.PathEscape(args[1]), msg, &r)
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "%s (owner %s)\n", r.Error, r.Node)
			os.Exit(1)
		}
		fmt.Printf("%s  (on %s)\n", r.Reply, r.Node)
	}
}

// call sends a request and decodes a JSON reply into out (if non-nil).
func call(c *http.Client, method, u, body string, out any) int {
	var rd *bytes.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	b, _, status, err := do(c, method, u, rd, nil)
	check(err)
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil && status < 300 {
			fmt.Fprintf(os.Stderr, "unexpected reply (%d): %s\n", status, strings.TrimSpace(string(b)))
			os.Exit(1)
		}
	}
	return status
}

func meshStatus(c *http.Client, gw string) {
	var s struct {
		Node    map[string]any `json:"node"`
		Members []struct {
			Name, Addr, State string
			Incarnation       uint64
		} `json:"members"`
		Raft struct {
			Term     uint64   `json:"term"`
			Leader   string   `json:"leader"`
			Role     string   `json:"role"`
			Commit   uint64   `json:"commit"`
			Voters   []string `json:"voters"`
			Learners []string `json:"learners"`
		} `json:"raft"`
		Control struct {
			Ring struct {
				Epoch    uint64
				Nodes    []string
				RF, R, W int
			} `json:"ring"`
			Leases int `json:"leases"`
		} `json:"control"`
		Transport []struct {
			Peer        string  `json:"peer"`
			SRTT        float64 `json:"srtt_ms"`
			CWND        int     `json:"cwnd"`
			PacketsLost uint64  `json:"packets_lost"`
			Congestion  string  `json:"congestion"`
		} `json:"transport"`
	}
	if status := call(c, "GET", gw+"/mesh/status", "", &s); status != http.StatusOK {
		fmt.Fprintln(os.Stderr, "not a kvmesh node (no /mesh/status); is --gw pointing at a gateway?")
		os.Exit(1)
	}
	fmt.Printf("node      %v at %v (identity %v)\n", s.Node["name"], s.Node["addr"], s.Node["identity"])
	fmt.Printf("raft      %s, leader %s, term %d, commit %d\n", s.Raft.Role, s.Raft.Leader, s.Raft.Term, s.Raft.Commit)
	fmt.Printf("          voters %s  learners %s\n", strings.Join(s.Raft.Voters, ","), orDash(s.Raft.Learners))
	r := s.Control.Ring
	fmt.Printf("ring      epoch %d  RF=%d R=%d W=%d  %s\n", r.Epoch, r.RF, r.R, r.W, strings.Join(r.Nodes, ","))
	fmt.Printf("leases    %d\n", s.Control.Leases)
	fmt.Println("members")
	for _, m := range s.Members {
		fmt.Printf("  %-8s %-22s %-8s inc %d\n", m.Name, m.Addr, m.State, m.Incarnation)
	}
	fmt.Println("transport")
	for _, t := range s.Transport {
		fmt.Printf("  %-8s srtt %6.2fms  cwnd %6dB  lost %d  %s\n", t.Peer, t.SRTT, t.CWND, t.PacketsLost, t.Congestion)
	}
}

func orDash(xs []string) string {
	if len(xs) == 0 {
		return "-"
	}
	return strings.Join(xs, ",")
}

func leaseCmd(c *http.Client, gw string, args []string) {
	usage := "lease acquire NAME HOLDER [TTL] | lease renew NAME HOLDER TOKEN [TTL] | lease release NAME HOLDER TOKEN | lease list"
	if len(args) < 1 {
		fail(usage)
	}
	var r struct {
		Holder, Error, Code string
		Token               uint64
		ExpiresAt           int64 `json:"expires_at"`
	}
	q := url.Values{}
	method := "POST"
	switch args[0] {
	case "list":
		var l struct {
			Leases []struct {
				Name, Holder string
				Token        uint64
				ExpiresAt    int64 `json:"expires_at"`
			}
		}
		call(c, "GET", gw+"/leases", "", &l)
		for _, x := range l.Leases {
			left := time.Until(time.Unix(0, x.ExpiresAt)).Round(time.Second)
			state := "expires in " + left.String()
			if left <= 0 {
				state = "expired"
			}
			fmt.Printf("%-24s holder %-12s token %-6d %s\n", x.Name, x.Holder, x.Token, state)
		}
		return
	case "acquire":
		if len(args) < 3 {
			fail(usage)
		}
		if len(args) > 3 {
			q.Set("ttl", args[3])
		}
	case "renew":
		if len(args) < 4 {
			fail(usage)
		}
		q.Set("renew", "1")
		q.Set("token", args[3])
		if len(args) > 4 {
			q.Set("ttl", args[4])
		}
	case "release":
		if len(args) < 4 {
			fail(usage)
		}
		method = "DELETE"
		q.Set("token", args[3])
	default:
		fail(usage)
	}
	q.Set("holder", args[2])
	status := call(c, method, gw+"/lease/"+url.PathEscape(args[1])+"?"+q.Encode(), "", &r)
	if status != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%s: %s (held by %s, token %d)\n", r.Code, r.Error, r.Holder, r.Token)
		os.Exit(1)
	}
	if method == "DELETE" {
		fmt.Printf("released %s\n", args[1])
		return
	}
	fmt.Printf("%s held by %s, fencing token %d, expires %s\n", args[1], r.Holder, r.Token,
		time.Unix(0, r.ExpiresAt).Format(time.TimeOnly))
}

func jobsCmd(c *http.Client, gw string, args []string) {
	usage := "jobs enqueue QUEUE PAYLOAD [ID] | jobs claim QUEUE [WORKER] | jobs complete QUEUE ID TOKEN | jobs fail QUEUE ID TOKEN [ERROR] | jobs list QUEUE"
	if len(args) < 2 {
		fail(usage)
	}
	queue := url.PathEscape(args[1])
	var r map[string]any
	switch args[0] {
	case "enqueue":
		if len(args) < 3 {
			fail(usage)
		}
		u := gw + "/jobs/" + queue
		if len(args) > 3 {
			u += "?id=" + url.QueryEscape(args[3])
		}
		if call(c, "POST", u, args[2], &r) != http.StatusOK {
			fmt.Fprintln(os.Stderr, r["error"])
			os.Exit(1)
		}
		fmt.Printf("enqueued %v (existing=%v)\n", r["id"], r["existing"])
	case "claim":
		worker := "cli"
		if len(args) > 2 {
			worker = args[2]
		}
		status := call(c, "POST", gw+"/jobs/"+queue+"/claim?worker="+url.QueryEscape(worker), "", &r)
		if status == http.StatusNoContent {
			fmt.Println("nothing ready")
			return
		}
		if status != http.StatusOK {
			fmt.Fprintln(os.Stderr, r["error"])
			os.Exit(1)
		}
		fmt.Printf("job %v attempt %v token %v: %v\n", r["id"], r["attempt"], r["token"], r["payload"])
	case "complete", "fail":
		if len(args) < 4 {
			fail(usage)
		}
		u := fmt.Sprintf("%s/jobs/%s/%s/%s?token=%s", gw, queue, url.PathEscape(args[2]), args[0], url.QueryEscape(args[3]))
		if args[0] == "fail" && len(args) > 4 {
			u += "&error=" + url.QueryEscape(strings.Join(args[4:], " "))
		}
		if call(c, "POST", u, "", &r) != http.StatusOK {
			fmt.Fprintf(os.Stderr, "%v: %v\n", r["code"], r["error"])
			os.Exit(1)
		}
		fmt.Printf("%v is %v (dead-lettered=%v)\n", r["id"], r["state"], r["dead_lettered"])
	case "list":
		var l struct {
			Stats map[string]any
			Jobs  []map[string]any
		}
		call(c, "GET", gw+"/jobs/"+queue, "", &l)
		fmt.Printf("pending %v  claimed %v  dead %v  completed %v\n", l.Stats["pending"], l.Stats["claimed"], l.Stats["dead"], l.Stats["completed"])
		for _, j := range l.Jobs {
			fmt.Printf("  %-20v %-10v attempts %v  %v\n", j["id"], j["state"], j["attempts"], j["last_error"])
		}
	default:
		fail(usage)
	}
}
