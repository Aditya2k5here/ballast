package limiter

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets the controller be driven through hundreds of windows without
// sleeping. A control loop that can only be tested in real time is a control
// loop nobody tests the interesting cases of.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// drive runs one window's worth of traffic at a given round-trip latency,
// pressing in-flight up to the current limit so the limit counts as binding.
func drive(g *Gradient, c *fakeClock, rtt time.Duration, windows int) {
	for w := 0; w < windows; w++ {
		var held []func(time.Duration, bool)
		for {
			rel, ok := g.Acquire()
			if !ok {
				break
			}
			held = append(held, rel)
			if len(held) > 4000 {
				break
			}
		}
		c.Advance(g.cfg.Window + time.Millisecond)
		for _, rel := range held {
			rel(rtt, false)
		}
		// A window with too few samples is ignored by design; make sure the
		// controller always has enough to act on.
		for i := 0; i < g.cfg.MinSamples; i++ {
			if rel, ok := g.Acquire(); ok {
				rel(rtt, false)
			}
		}
	}
}

func newTestGradient() (*Gradient, *fakeClock) {
	cfg := DefaultGradient()
	cfg.Initial = 20
	g := NewGradient(cfg)
	c := newClock()
	g.SetClock(c.Now)
	return g, c
}

func TestFixedNeverExceedsItsLimit(t *testing.T) {
	f := NewFixed(3)
	var rels []func(time.Duration, bool)
	for i := 0; i < 10; i++ {
		if rel, ok := f.Acquire(); ok {
			rels = append(rels, rel)
		}
	}
	if len(rels) != 3 {
		t.Fatalf("acquired %d slots, want 3", len(rels))
	}
	if f.InFlight() != 3 {
		t.Fatalf("in flight %d, want 3", f.InFlight())
	}
	rels[0](time.Millisecond, false)
	if _, ok := f.Acquire(); !ok {
		t.Fatal("a released slot was not reusable")
	}
}

func TestGradientShrinksWhenLatencyRises(t *testing.T) {
	g, c := newTestGradient()
	drive(g, c, 10*time.Millisecond, 4) // establish the floor
	before := g.Limit()
	drive(g, c, 200*time.Millisecond, 12) // queueing appears
	after := g.Limit()
	if after >= before {
		t.Fatalf("limit %d -> %d: should shrink when latency rises 20x",
			before, after)
	}
}

func TestGradientGrowsWhenCapacityAppears(t *testing.T) {
	g, c := newTestGradient()
	drive(g, c, 10*time.Millisecond, 30)
	if got := g.Limit(); got <= 20 {
		t.Fatalf("limit %d: should probe upwards when nothing is queueing", got)
	}
}

func TestGradientRecoversAfterTheUpstreamHeals(t *testing.T) {
	g, c := newTestGradient()
	drive(g, c, 10*time.Millisecond, 6)
	healthy := g.Limit()
	drive(g, c, 300*time.Millisecond, 20)
	degraded := g.Limit()
	if degraded >= healthy {
		t.Fatalf("limit did not fall under load: %d -> %d", healthy, degraded)
	}
	// The RTT floor must decay, or the controller believes the slow period is
	// the new normal and never grows back.
	c.Advance(2 * g.cfg.FloorDecay)
	drive(g, c, 10*time.Millisecond, 30)
	if recovered := g.Limit(); recovered <= degraded {
		t.Fatalf("limit did not recover: %d -> %d -> %d",
			healthy, degraded, recovered)
	}
}

func TestGradientNeverLeavesItsBounds(t *testing.T) {
	cfg := DefaultGradient()
	cfg.Min, cfg.Max, cfg.Initial = 5, 40, 20
	g := NewGradient(cfg)
	c := newClock()
	g.SetClock(c.Now)

	drive(g, c, 5*time.Second, 60)
	if got := g.Limit(); got < cfg.Min {
		t.Fatalf("limit %d fell below Min %d", got, cfg.Min)
	}
	drive(g, c, time.Microsecond, 60)
	if got := g.Limit(); got > cfg.Max {
		t.Fatalf("limit %d rose above Max %d", got, cfg.Max)
	}
}

func TestGradientBacksOffWhenEverythingFails(t *testing.T) {
	g, c := newTestGradient()
	drive(g, c, 10*time.Millisecond, 4)
	before := g.Limit()
	for w := 0; w < 3; w++ {
		var held []func(time.Duration, bool)
		for {
			rel, ok := g.Acquire()
			if !ok {
				break
			}
			held = append(held, rel)
		}
		c.Advance(g.cfg.Window + time.Millisecond)
		for _, rel := range held {
			rel(0, true) // every request dropped
		}
	}
	if after := g.Limit(); after >= before {
		t.Fatalf("limit %d -> %d: should back off when nothing succeeds",
			before, after)
	}
}

// A limit that is not being reached is not the constraint, so raising it is
// pointless, and worse, it is a larger number to fall from when load arrives.
func TestGradientDoesNotGrowWhenItsLimitIsNotBinding(t *testing.T) {
	g, c := newTestGradient()
	before := g.Limit()
	for w := 0; w < 30; w++ {
		for i := 0; i < g.cfg.MinSamples+2; i++ {
			if rel, ok := g.Acquire(); ok {
				rel(time.Millisecond, false)
			}
		}
		c.Advance(g.cfg.Window + time.Millisecond)
		if rel, ok := g.Acquire(); ok {
			rel(time.Millisecond, false)
		}
	}
	if after := g.Limit(); after > before+2 {
		t.Fatalf("limit grew %d -> %d while barely used", before, after)
	}
}

func TestGradientIsSafeUnderConcurrentUse(t *testing.T) {
	g := NewGradient(DefaultGradient())
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if rel, ok := g.Acquire(); ok {
					rel(time.Duration(j%50)*time.Microsecond, j%17 == 0)
				}
			}
		}()
	}
	wg.Wait()
	if g.InFlight() != 0 {
		t.Fatalf("in flight %d after everything was released", g.InFlight())
	}
}
