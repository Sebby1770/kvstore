package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// A saturated server throttles its clients: callers block in admit instead
// of piling calls into the server's queue, the server never runs more than
// MaxConcurrent handlers, and nothing is rejected -- every call completes.
func TestPressureThrottlesCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		const slots, queue = 4, 2
		e.server.SetLimits(Limits{MaxConcurrent: slots, MaxQueue: queue})
		defer e.server.Close()
		var running, peak atomic.Int64
		e.server.Register("work", func(ctx context.Context, req []byte) ([]byte, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			running.Add(-1)
			return req, nil
		})
		// Warm up: the first call announces pressure support and learns the
		// server's state.
		if _, err := e.client.Call(context.Background(), "server", "work", []byte("hi")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		var wg sync.WaitGroup
		var failed atomic.Int64
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if _, err := e.client.Call(ctx, "server", "work", []byte(fmt.Sprint(i))); err != nil {
					failed.Add(1)
					t.Errorf("call %d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		if failed.Load() > 0 {
			return
		}
		if p := peak.Load(); p > slots {
			t.Fatalf("%d handlers ran at once, limit %d", p, slots)
		}
		adm := e.server.Admission()
		if adm.Rejected != 0 {
			t.Fatalf("%d calls rejected; pressure should have held clients back", adm.Rejected)
		}
		ps := e.client.PressureStats()
		if ps.Throttled == 0 || ps.Waited == 0 {
			t.Fatalf("client was never throttled: %+v", ps)
		}
		if adm.Updates == 0 {
			t.Fatal("server sent no pressure updates")
		}
		// Credits are cumulative: 41 calls made, plus the whole idle pool.
		if p, ok := e.client.Pressure("server"); !ok || p.Credits != 41+slots+queue || p.Load != 0 {
			t.Fatalf("idle server should grant its whole pool beyond the 41 calls made: %+v %v", p, ok)
		}
	})
}

// Without pressure-aware clients, the queue bound turns excess calls away
// with ResourceExhausted instead of letting them wait forever.
func TestAdmissionRejectsBeyondQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		e.server.SetLimits(Limits{MaxConcurrent: 1, MaxQueue: 1})
		defer e.server.Close()
		release := make(chan struct{})
		e.server.Register("block", func(ctx context.Context, req []byte) ([]byte, error) {
			<-release
			return nil, nil
		})
		// Drive the admission directly (as a pressure-unaware peer would):
		// one running, one queued, the third is rejected.
		ctx := context.Background()
		r1, err := e.server.adm.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan error, 1)
		go func() {
			r2, err := e.server.adm.acquire(ctx)
			if err == nil {
				r2()
			}
			got <- err
		}()
		synctest.Wait()
		if _, err := e.server.adm.acquire(ctx); CodeOf(err) != ResourceExhausted {
			t.Fatalf("third call: %v, want resource_exhausted", err)
		}
		r1()
		if err := <-got; err != nil {
			t.Fatalf("queued call: %v", err)
		}
		close(release)
		if st := e.server.Admission(); st.Active != 0 || st.Queued != 0 || st.Rejected != 1 {
			t.Fatalf("pool not drained: %+v", st)
		}
	})
}

// A queued caller that gives up leaves the queue without leaking its slot.
func TestAdmissionCancelWhileQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer()
		s.SetLimits(Limits{MaxConcurrent: 1, MaxQueue: 4})
		defer s.Close()
		r1, _ := s.adm.acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := s.adm.acquire(ctx); done <- err }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
		r1()
		if st := s.Admission(); st.Active != 0 || st.Queued != 0 {
			t.Fatalf("slot leaked: %+v", st)
		}
	})
}

// Demand(n) bounds a streaming handler to n unconsumed messages: a slow
// consumer holds the producer back in messages, not bytes.
func TestStreamingDemand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		defer e.close()
		const total, window = 64, 4
		var produced atomic.Int64
		e.server.RegisterStream("count", func(ctx context.Context, req []byte, send func([]byte) error) error {
			for i := 0; i < total; i++ {
				if err := send([]byte(fmt.Sprint(i))); err != nil {
					return err
				}
				produced.Add(1)
			}
			return nil
		})
		var consumed int64
		var maxAhead int64
		err := e.client.CallStream(context.Background(), "server", "count", nil, func(b []byte) error {
			consumed++
			if ahead := produced.Load() - consumed; ahead > maxAhead {
				maxAhead = ahead
			}
			if want := fmt.Sprint(consumed - 1); string(b) != want {
				return fmt.Errorf("got %q want %q", b, want)
			}
			time.Sleep(10 * time.Millisecond) // a slow consumer
			return nil
		}, Demand(window))
		if err != nil {
			t.Fatal(err)
		}
		if consumed != total {
			t.Fatalf("consumed %d of %d", consumed, total)
		}
		if maxAhead > window {
			t.Fatalf("producer ran %d messages ahead of the consumer, window %d", maxAhead, window)
		}
	})
}
