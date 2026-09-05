package pqueue

import (
	"testing"
	"time"
)

func mk(p Priority, deadline time.Time) *Item {
	return &Item{Priority: p, Deadline: deadline, Ready: make(chan Outcome, 1)}
}

type clock struct{ t time.Time }

func (c *clock) Now() time.Time      { return c.t }
func (c *clock) Add(d time.Duration) { c.t = c.t.Add(d) }

func newClock() *clock {
	return &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func TestCriticalIsServedBeforeBulk(t *testing.T) {
	q := New(10, 1000)
	bulk, std, crit := mk(Bulk, time.Time{}), mk(Standard, time.Time{}), mk(Critical, time.Time{})
	for _, it := range []*Item{bulk, std, crit} {
		if !q.Enqueue(it) {
			t.Fatal("enqueue failed with room to spare")
		}
	}
	want := []*Item{crit, std, bulk}
	for i, w := range want {
		if got := q.Dequeue(); got != w {
			t.Fatalf("position %d: served the wrong tier", i)
		}
	}
}

// The queue must not let a flood of unimportant traffic lock out important
// traffic simply by arriving first.
func TestAFullQueueShedsTheLeastImportantWaiter(t *testing.T) {
	q := New(2, 1000)
	b1, b2 := mk(Bulk, time.Time{}), mk(Bulk, time.Time{})
	q.Enqueue(b1)
	q.Enqueue(b2)

	crit := mk(Critical, time.Time{})
	if !q.Enqueue(crit) {
		t.Fatal("critical request was refused by a queue full of bulk")
	}
	if got := <-b1.Ready; got != Shed {
		t.Fatalf("oldest bulk waiter got %v, want Shed", got)
	}
	if got := q.Dequeue(); got != crit {
		t.Fatal("critical was not served first")
	}
}

func TestAFullQueueRefusesArrivalsNothingIsLessImportantThan(t *testing.T) {
	q := New(2, 1000)
	q.Enqueue(mk(Critical, time.Time{}))
	q.Enqueue(mk(Critical, time.Time{}))
	late := mk(Bulk, time.Time{})
	if q.Enqueue(late) {
		t.Fatal("bulk was admitted to a queue full of critical")
	}
	if got := <-late.Ready; got != Shed {
		t.Fatalf("got %v, want Shed", got)
	}
}

func TestExpiredRequestsAreDroppedAtTheHeadOfTheQueue(t *testing.T) {
	c := newClock()
	q := New(10, 1000)
	q.SetClock(c.Now)

	dead := mk(Standard, c.Now().Add(time.Millisecond))
	live := mk(Standard, c.Now().Add(time.Hour))
	q.Enqueue(dead)
	q.Enqueue(live)

	c.Add(time.Second) // the first one's client has given up
	got := q.Dequeue()
	if got != live {
		t.Fatal("an expired request was handed to an upstream")
	}
	if o := <-dead.Ready; o != Expired {
		t.Fatalf("expired waiter got %v", o)
	}
}

func TestOverdueWaitersAreSweptEvenWhenNothingIsDraining(t *testing.T) {
	c := newClock()
	q := New(10, 1000)
	q.SetClock(c.Now)
	for i := 0; i < 5; i++ {
		q.Enqueue(mk(Standard, c.Now().Add(time.Millisecond)))
	}
	c.Add(time.Second)
	if n := q.ExpireOverdue(); n != 5 {
		t.Fatalf("swept %d, want 5", n)
	}
	if q.Depth() != 0 {
		t.Fatalf("depth %d after sweeping everything", q.Depth())
	}
}

// Under pressure the newest waiter is served first: the oldest is closest to
// timing out, so FIFO spends capacity on answers nobody is waiting for.
func TestDeepQueuesServeNewestFirst(t *testing.T) {
	q := New(100, 2)
	var items []*Item
	for i := 0; i < 5; i++ {
		it := mk(Standard, time.Time{})
		items = append(items, it)
		q.Enqueue(it)
	}
	if got := q.Dequeue(); got != items[4] {
		t.Fatal("a deep queue did not serve the newest waiter first")
	}
	if q.Stats().LIFOSwaps == 0 {
		t.Fatal("LIFO was not recorded")
	}
}

func TestShallowQueuesStayFair(t *testing.T) {
	q := New(100, 10)
	var items []*Item
	for i := 0; i < 3; i++ {
		it := mk(Standard, time.Time{})
		items = append(items, it)
		q.Enqueue(it)
	}
	if got := q.Dequeue(); got != items[0] {
		t.Fatal("a shallow queue should serve oldest-first")
	}
}

func TestCloseReleasesEveryWaiter(t *testing.T) {
	q := New(10, 1000)
	var items []*Item
	for i := 0; i < 4; i++ {
		it := mk(Standard, time.Time{})
		items = append(items, it)
		q.Enqueue(it)
	}
	q.Close()
	for i, it := range items {
		select {
		case o := <-it.Ready:
			if o != Closed {
				t.Fatalf("waiter %d got %v, want Closed", i, o)
			}
		default:
			t.Fatalf("waiter %d was left hanging", i)
		}
	}
}

func TestPriorityParsingDefaultsToStandard(t *testing.T) {
	cases := map[string]Priority{
		"0": Critical, "critical": Critical,
		"2": Bulk, "bulk": Bulk,
		"1": Standard, "": Standard, "nonsense": Standard,
	}
	for in, want := range cases {
		if got := ParsePriority(in); got != want {
			t.Fatalf("ParsePriority(%q) = %v, want %v", in, got, want)
		}
	}
}
