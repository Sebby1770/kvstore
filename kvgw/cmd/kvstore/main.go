// kvstore — small CLI that talks to the gateway over HTTP.
//
//	kvstore get     KEY
//	kvstore put     KEY VALUE [--ttl 10s]
//	kvstore cas     KEY EXPECTED_VERSION VALUE
//	kvstore del     KEY
//	kvstore expire  KEY DURATION
//	kvstore scan    [prefix]
//	kvstore route   KEY
//	kvstore health
//	kvstore metrics
//	kvstore export
//	kvstore import          (JSON on stdin)
//	kvstore compact
//	kvstore watch   KEY
//	kvstore rm-prefix PREFIX
//	kvstore fault   ADDR on|off
//	kvstore bench   [N]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func hdrKV(h http.Header, name string) string {
	if v := h.Get("X-KV-" + name); v != "" {
		return v
	}
	return h.Get("X-Triglot-" + name)
}

func main() {
	gw := flag.String("gw", envOr("KVSTORE_GW", envOr("TRIGLOT_GW", "http://127.0.0.1:8080")), "gateway base URL")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "kvstore [--gw URL] [--timeout D] <cmd> [args...]\n")
		fmt.Fprintf(os.Stderr, "  get KEY | put KEY VALUE [--ttl 10s] | cas KEY VER VALUE | del KEY\n")
		fmt.Fprintf(os.Stderr, "  expire KEY DURATION | scan [prefix] | rm-prefix PREFIX | route KEY\n")
		fmt.Fprintf(os.Stderr, "  health | metrics | export | import | compact | watch KEY\n")
		fmt.Fprintf(os.Stderr, "  fault ADDR on|off | bench [N]\n")
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	c := &http.Client{Timeout: *timeout}
	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	switch cmd {
	case "get":
		if len(args) != 1 {
			fail("get KEY")
		}
		body, hdr, status, err := do(c, "GET", *gw+"/kv/"+url.PathEscape(args[0]), nil, nil)
		check(err)
		if status == http.StatusNotFound {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "%s: %s\n", http.StatusText(status), strings.TrimSpace(string(body)))
			os.Exit(1)
		}
		os.Stdout.Write(body)
		if !bytes.HasSuffix(body, []byte{'\n'}) {
			fmt.Println()
		}
		note := fmt.Sprintf("v%s from [%s]", hdrKV(hdr, "Version"), hdrKV(hdr, "Responders"))
		if rep := hdrKV(hdr, "Repaired"); rep != "" {
			note += " (read-repaired " + rep + ")"
		}
		if exp := hdrKV(hdr, "Expires-At"); exp != "" {
			note += " expires=" + exp
		}
		fmt.Fprintf(os.Stderr, "(%s)\n", note)

	case "put":
		args, ttl, err := peelTTL(args)
		check(err)
		if len(args) != 2 {
			fail("put KEY VALUE [--ttl 10s]")
		}
		hdr := http.Header{}
		if ttl > 0 {
			secs := int(math.Ceil(ttl.Seconds()))
			if secs < 1 {
				secs = 1
			}
			hdr.Set("X-KV-TTL", strconv.Itoa(secs))
		}
		_, rh, status, err := do(c, "PUT", *gw+"/kv/"+url.PathEscape(args[0]), bytes.NewReader([]byte(args[1])), hdr)
		check(err)
		if status != http.StatusNoContent {
			fmt.Fprintf(os.Stderr, "put: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ok v%s acks=%s replicas=[%s]\n",
			hdrKV(rh, "Version"), hdrKV(rh, "Acks"), hdrKV(rh, "Replicas"))

	case "cas":
		if len(args) != 3 {
			fail("cas KEY EXPECTED_VERSION VALUE")
		}
		if _, err := strconv.ParseUint(args[1], 10, 64); err != nil {
			fail("EXPECTED_VERSION must be an integer")
		}
		hdr := http.Header{}
		hdr.Set("If-Match", args[1])
		_, rh, status, err := do(c, "PUT", *gw+"/kv/"+url.PathEscape(args[0]), bytes.NewReader([]byte(args[2])), hdr)
		check(err)
		if status == http.StatusConflict {
			fmt.Fprintf(os.Stderr, "conflict current v%s\n", hdrKV(rh, "Version"))
			os.Exit(1)
		}
		if status != http.StatusNoContent {
			fmt.Fprintf(os.Stderr, "cas: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ok v%s acks=%s replicas=[%s]\n",
			hdrKV(rh, "Version"), hdrKV(rh, "Acks"), hdrKV(rh, "Replicas"))

	case "del":
		if len(args) != 1 {
			fail("del KEY")
		}
		_, hdr, status, err := do(c, "DELETE", *gw+"/kv/"+url.PathEscape(args[0]), nil, nil)
		check(err)
		if status == http.StatusNotFound {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		if status != http.StatusNoContent {
			fmt.Fprintf(os.Stderr, "del: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ok v%s acks=%s\n", hdrKV(hdr, "Version"), hdrKV(hdr, "Acks"))

	case "expire":
		if len(args) != 2 {
			fail("expire KEY DURATION")
		}
		d, err := time.ParseDuration(args[1])
		if err != nil || d <= 0 {
			fail("DURATION must look like 10s or 5m")
		}
		body, _, status, err := do(c, "GET", *gw+"/kv/"+url.PathEscape(args[0]), nil, nil)
		check(err)
		if status == http.StatusNotFound {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "expire get: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		secs := int(math.Ceil(d.Seconds()))
		if secs < 1 {
			secs = 1
		}
		hdr := http.Header{}
		hdr.Set("X-KV-TTL", strconv.Itoa(secs))
		_, rh, status, err := do(c, "PUT", *gw+"/kv/"+url.PathEscape(args[0]), bytes.NewReader(body), hdr)
		check(err)
		if status != http.StatusNoContent {
			fmt.Fprintf(os.Stderr, "expire: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ok v%s ttl=%ds acks=%s\n",
			hdrKV(rh, "Version"), secs, hdrKV(rh, "Acks"))

	case "scan":
		u := *gw + "/scan"
		if len(args) >= 1 {
			u += "?prefix=" + url.QueryEscape(args[0])
		}
		body, _, status, err := do(c, "GET", u, nil, nil)
		check(err)
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "scan: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		var out struct {
			Count   int `json:"count"`
			Entries []struct {
				Key       string `json:"key"`
				Value     string `json:"value"`
				Version   uint64 `json:"version"`
				Replicas  int    `json:"replicas"`
				ExpiresAt *int64 `json:"expires_at"`
			} `json:"entries"`
			Errors []string `json:"errors"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			fail(err.Error())
		}
		for _, e := range out.Entries {
			exp := ""
			if e.ExpiresAt != nil {
				exp = fmt.Sprintf("  exp=%d", *e.ExpiresAt)
			}
			fmt.Printf("%-16s = %-16s  v%-4d  x%d replicas%s\n", e.Key, e.Value, e.Version, e.Replicas, exp)
		}
		fmt.Fprintf(os.Stderr, "(%d keys)\n", out.Count)
		for _, e := range out.Errors {
			fmt.Fprintf(os.Stderr, "  ! %s\n", e)
		}

	case "route":
		if len(args) != 1 {
			fail("route KEY")
		}
		body, _, status, err := do(c, "GET", *gw+"/route/"+url.PathEscape(args[0]), nil, nil)
		check(err)
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "route: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		var out struct {
			Key      string   `json:"key"`
			Replicas []string `json:"replicas"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			fail(err.Error())
		}
		fmt.Printf("%s -> %s\n", out.Key, strings.Join(out.Replicas, ", "))

	case "health":
		body, _, status, err := do(c, "GET", *gw+"/health", nil, nil)
		check(err)
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err != nil {
			os.Stdout.Write(body)
		} else {
			fmt.Println(pretty.String())
		}
		if status != http.StatusOK {
			os.Exit(1)
		}

	case "metrics":
		body, _, _, err := do(c, "GET", *gw+"/health", nil, nil)
		if err == nil {
			var h struct {
				Metrics json.RawMessage `json:"metrics"`
			}
			if json.Unmarshal(body, &h) == nil && len(h.Metrics) > 0 {
				var pretty bytes.Buffer
				if json.Indent(&pretty, h.Metrics, "", "  ") == nil {
					fmt.Println(pretty.String())
					return
				}
			}
		}
		body, _, status, err := do(c, "GET", *gw+"/metrics", nil, nil)
		check(err)
		os.Stdout.Write(body)
		if !bytes.HasSuffix(body, []byte{'\n'}) {
			fmt.Println()
		}
		if status != http.StatusOK {
			os.Exit(1)
		}

	case "export":
		body, _, status, err := do(c, "GET", *gw+"/export", nil, nil)
		check(err)
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "export: %s\n", http.StatusText(status))
			os.Exit(1)
		}
		var pretty bytes.Buffer
		if json.Indent(&pretty, body, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			os.Stdout.Write(body)
		}

	case "import":
		in, err := io.ReadAll(os.Stdin)
		check(err)
		body, _, status, err := do(c, "POST", *gw+"/import", bytes.NewReader(in), http.Header{
			"Content-Type": []string{"application/json"},
		})
		check(err)
		if status != http.StatusOK {
			fmt.Fprintf(os.Stderr, "import: %s %s\n", http.StatusText(status), strings.TrimSpace(string(body)))
			os.Exit(1)
		}
		var pretty bytes.Buffer
		if json.Indent(&pretty, body, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			os.Stdout.Write(body)
		}

	case "rm-prefix":
		if len(args) != 1 {
			fail("rm-prefix PREFIX")
		}
		body, _, status, err := do(c, "DELETE", *gw+"/scan?prefix="+url.QueryEscape(args[0]), nil, nil)
		check(err)
		var pretty bytes.Buffer
		if json.Indent(&pretty, body, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			os.Stdout.Write(body)
			if !bytes.HasSuffix(body, []byte{'\n'}) {
				fmt.Println()
			}
		}
		if status != http.StatusOK {
			os.Exit(1)
		}

	case "fault":
		if len(args) != 2 || (args[1] != "on" && args[1] != "off") {
			fail("fault ADDR on|off")
		}
		down := "1"
		if args[1] == "off" {
			down = "0"
		}
		u := *gw + "/admin/fault?addr=" + url.QueryEscape(args[0]) + "&down=" + down
		body, _, status, err := do(c, "POST", u, nil, nil)
		check(err)
		var pretty bytes.Buffer
		if json.Indent(&pretty, body, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			os.Stdout.Write(body)
			if !bytes.HasSuffix(body, []byte{'\n'}) {
				fmt.Println()
			}
		}
		if status != http.StatusOK {
			os.Exit(1)
		}

	case "compact":
		body, _, status, err := do(c, "POST", *gw+"/admin/compact", nil, nil)
		check(err)
		var pretty bytes.Buffer
		if json.Indent(&pretty, body, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			os.Stdout.Write(body)
			if !bytes.HasSuffix(body, []byte{'\n'}) {
				fmt.Println()
			}
		}
		if status != http.StatusOK {
			os.Exit(1)
		}

	case "watch":
		if len(args) != 1 {
			fail("watch KEY")
		}
		key := args[0]
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		poll := func() {
			body, hdr, status, err := do(c, "GET", *gw+"/kv/"+url.PathEscape(key), nil, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "watch: %v\n", err)
				return
			}
			if status == http.StatusNotFound {
				fmt.Println("(missing)")
				return
			}
			if status != http.StatusOK {
				fmt.Fprintf(os.Stderr, "watch: %s %s\n", http.StatusText(status), strings.TrimSpace(string(body)))
				return
			}
			val := strings.TrimRight(string(body), "\n")
			fmt.Printf("v%s %s\n", hdrKV(hdr, "Version"), val)
		}
		poll()
		for {
			select {
			case <-sig:
				return
			case <-ticker.C:
				poll()
			}
		}

	case "bench":
		n := 200
		if len(args) >= 1 {
			var err error
			n, err = strconv.Atoi(args[0])
			if err != nil || n < 1 {
				fail("bench [N]  (N default 200)")
			}
		}
		start := time.Now()
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("bench-%d", i)
			_, _, status, err := do(c, "PUT", *gw+"/kv/"+url.PathEscape(k), bytes.NewReader([]byte("x")), nil)
			check(err)
			if status != http.StatusNoContent {
				fmt.Fprintf(os.Stderr, "bench put %s: %s\n", k, http.StatusText(status))
				os.Exit(1)
			}
			_, _, status, err = do(c, "GET", *gw+"/kv/"+url.PathEscape(k), nil, nil)
			check(err)
			if status != http.StatusOK {
				fmt.Fprintf(os.Stderr, "bench get %s: %s\n", k, http.StatusText(status))
				os.Exit(1)
			}
		}
		elapsed := time.Since(start)
		ops := float64(n*2) / elapsed.Seconds()
		fmt.Printf("%d put/get pairs (%d ops) in %s — %.0f ops/sec\n",
			n, n*2, elapsed.Round(time.Millisecond), ops)

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func do(c *http.Client, method, u string, body io.Reader, hdr http.Header) ([]byte, http.Header, int, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, nil, 0, err
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.Header, resp.StatusCode, err
}

func peelTTL(args []string) ([]string, time.Duration, error) {
	out := make([]string, 0, len(args))
	var ttl time.Duration
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--ttl":
			if i+1 >= len(args) {
				return nil, 0, fmt.Errorf("--ttl needs a duration (e.g. 10s)")
			}
			i++
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return nil, 0, fmt.Errorf("ttl: %w", err)
			}
			ttl = d
		case strings.HasPrefix(a, "--ttl="):
			d, err := time.ParseDuration(strings.TrimPrefix(a, "--ttl="))
			if err != nil {
				return nil, 0, fmt.Errorf("ttl: %w", err)
			}
			ttl = d
		default:
			out = append(out, a)
		}
	}
	return out, ttl, nil
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "kvstore:", msg)
	os.Exit(2)
}
