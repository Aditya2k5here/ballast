package breaker

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) Now() time.Time      { return c.t }
func (c *clock) Add(d time.Duration) { c.t = c.t.Add(d) }

func newBreaker() (*Breaker, *clock) {
	cfg := Config{FailureThreshold: 3, SuccessThreshold: 2,
		OpenFor: time.Second, MinSamples: 0}
	b := New(cfg)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b.SetClock(c.Now)
	return b, c
}

func TestConsecutiveFailuresTripIt(t *testing.T) {
	b, _ := newBreaker()
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("refused before tripping, at %d", i)
		}
		b.Report(false)
	}
	if b.State() != Open {
		t.Fatalf("state %v, want Open", b.State())
	}
	if b.Allow() {
		t.Fatal("an open breaker admitted a request")
	}
}

func TestASuccessResetsTheFailureRun(t *testing.T) {
	b, _ := newBreaker()
	b.Allow()
	b.Report(false)
	b.Allow()
	b.Report(false)
	b.Allow()
	b.Report(true) // the run is broken
	b.Allow()
	b.Report(false)
	if b.State() != Closed {
		t.Fatalf("state %v: two failures either side of a success should not trip", b.State())
	}
}

// The point of half-open is to spend exactly one request finding out. A burst
// of probes against a still-broken upstream is the problem, not the test.
func TestHalfOpenAdmitsExactlyOneProbe(t *testing.T) {
	b, c := newBreaker()
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}
	c.Add(2 * time.Second)
	if !b.Allow() {
		t.Fatal("no probe was admitted after the open period")
	}
	if b.Allow() {
		t.Fatal("a second probe was admitted while the first was in flight")
	}
	if b.State() != HalfOpen {
		t.Fatalf("state %v, want HalfOpen", b.State())
	}
}

func TestAFailedProbeReopensImmediately(t *testing.T) {
	b, c := newBreaker()
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}
	c.Add(2 * time.Second)
	b.Allow()
	b.Report(false)
	if b.State() != Open {
		t.Fatalf("state %v, want Open after a failed probe", b.State())
	}
}

func TestEnoughGoodProbesCloseIt(t *testing.T) {
	b, c := newBreaker()
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}
	c.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("probe %d refused", i)
		}
		b.Report(true)
	}
	if b.State() != Closed {
		t.Fatalf("state %v, want Closed", b.State())
	}
}

func TestMinSamplesStopsItTrippingOnTheFirstTwoRequests(t *testing.T) {
	b := New(Config{FailureThreshold: 2, SuccessThreshold: 1,
		OpenFor: time.Second, MinSamples: 10})
	for i := 0; i < 5; i++ {
		b.Allow()
		b.Report(false)
	}
	if b.State() != Closed {
		t.Fatal("tripped before it had enough evidence")
	}
}
