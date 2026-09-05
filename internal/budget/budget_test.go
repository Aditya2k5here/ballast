package budget

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) Now() time.Time      { return c.t }
func (c *clock) Add(d time.Duration) { c.t = c.t.Add(d) }

func newBudget(ratio float64) (*Budget, *clock) {
	b := New(ratio, 0, 10*time.Second, 10)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b.SetClock(c.Now)
	return b, c
}

func TestRetriesAreCappedAtTheRatio(t *testing.T) {
	b, _ := newBudget(0.1)
	for i := 0; i < 100; i++ {
		b.Original()
	}
	granted := 0
	for i := 0; i < 50; i++ {
		if b.Withdraw() {
			granted++
		}
	}
	if granted != 10 {
		t.Fatalf("granted %d retries against 100 originals at 10%%, want 10", granted)
	}
}

// The property that matters during an incident: as failures rise, originals
// stay flat, so the allowance stays flat, so retry load cannot compound.
func TestTheAllowanceDoesNotGrowWithFailures(t *testing.T) {
	b, _ := newBudget(0.1)
	for i := 0; i < 100; i++ {
		b.Original()
	}
	first := 0
	for b.Withdraw() {
		first++
	}
	second := 0
	for i := 0; i < 1000; i++ {
		if b.Withdraw() {
			second++
		}
	}
	if second != 0 {
		t.Fatalf("a spent budget granted %d more retries", second)
	}
	if first == 0 {
		t.Fatal("no retries were granted at all")
	}
}

func TestTheWindowSlidesRatherThanResetting(t *testing.T) {
	b, c := newBudget(0.1)
	for i := 0; i < 100; i++ {
		b.Original()
	}
	for b.Withdraw() {
	}
	// Slide most of the way through the window; the old originals age out
	// gradually, so the allowance recovers gradually rather than in a step.
	c.Add(11 * time.Second)
	for i := 0; i < 100; i++ {
		b.Original()
	}
	granted := 0
	for b.Withdraw() {
		granted++
	}
	if granted == 0 {
		t.Fatal("the budget never recovered as the window slid")
	}
	if granted > 12 {
		t.Fatalf("granted %d after the window slid: the ratio was not enforced", granted)
	}
}

func TestAMinimumFloorLetsQuietServicesRetryAtAll(t *testing.T) {
	b := New(0.1, 1, 10*time.Second, 10)
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b.SetClock(c.Now)
	b.Original()
	if !b.Withdraw() {
		t.Fatal("a service with one request could not retry it once")
	}
}
