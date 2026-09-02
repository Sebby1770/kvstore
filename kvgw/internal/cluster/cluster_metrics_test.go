package cluster

import "testing"

func TestMetricsCount(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put([]byte("k2"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := c.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Del([]byte("k2")); err != nil {
		t.Fatal(err)
	}
	m := c.Metrics()
	if m.Puts != 2 || m.Gets != 1 || m.Dels != 1 {
		t.Fatalf("counters wrong: %+v", m)
	}

	// A write that can't reach a quorum bumps QuorumFailures.
	fakes[0].setDown(true)
	fakes[1].setDown(true)
	if _, err := c.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected quorum failure with 2/3 down")
	}
	if c.Metrics().QuorumFailures != 1 {
		t.Fatalf("QuorumFailures = %d, want 1", c.Metrics().QuorumFailures)
	}
}
