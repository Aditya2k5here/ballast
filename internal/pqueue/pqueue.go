// Package pqueue is the admission queue: which waiting request runs next, and
// which one is told no.
//
// Three decisions live here, and each is counter-intuitive on first reading.
//
// Priority first. When there is not enough capacity for everyone, the choice is
// not whether to fail requests but which ones. A queue that admits by arrival
// order fails a payment because a metrics scrape got there first. Tiers make
// that choice explicit and reviewable.
//
// LIFO within a tier, under load. Serving oldest-first is fair and, past the
// knee, wrong: the oldest waiter is the one closest to giving up, so FIFO
// spends the capacity you have on responses nobody is still waiting for. Newest
// -first keeps the freshest deadlines alive and the number of *useful*
// responses higher. Below the pressure threshold the queue returns to FIFO,
// because when everyone is going to be served anyway, order should be fair.
//
// Deadlines are checked at dequeue, not only at enqueue. A request whose
// deadline has already passed by the time capacity frees up is dropped without
// ever reaching the upstream. Work done for a client that has gone is not
// merely wasted: it is load with negative value, because it delays the requests
// that could still have been answered.
package pqueue

import (
	"container/list"
	"sync"
	"time"
)

// Priority is a request class. Lower is more important.
type Priority int

const (
	Critical Priority = iota // user-facing, pays the bills
	Standard                 // normal API traffic
	Bulk                     // batch jobs, scrapers, prefetch
	numTiers
)

func (p Priority) String() string {
	switch p {
	case Critical:
		return "critical"
	case Standard:
		return "standard"
	case Bulk:
		return "bulk"
	}
	return "unknown"
}

// ParsePriority maps a header value to a tier, defaulting to Standard. An
// unrecognised value must not be an error: a client that sends nonsense should
// get ordinary service, not a 400.
func ParsePriority(s string) Priority {
	switch s {
	case "0", "critical":
		return Critical
	case "2", "bulk":
		return Bulk
	default:
		return Standard
	}
}

// Item is one waiting request.
type Item struct {
	Priority Priority
	Deadline time.Time
	Enqueued time.Time
	Ready    chan Outcome // buffered, size 1; the waiter reads exactly one
}

// Outcome tells a waiter what happened.
type Outcome int

const (
	Admitted Outcome = iota
	Shed             // capacity exhausted, this tier gave way
	Expired          // deadline passed while queued
	Closed           // shutting down
)

func (o Outcome) String() string {
	switch o {
	case Admitted:
		return "admitted"
	case Shed:
		return "shed"
	case Expired:
		return "expired"
	}
	return "closed"
}

// Stats is what the metrics endpoint and the tests both read.
type Stats struct {
	Depth     [numTiers]int
	Admitted  [numTiers]uint64
	Shed      [numTiers]uint64
	Expired   [numTiers]uint64
	MaxDepth  int
	LIFOSwaps uint64
	WaitNanos [numTiers]uint64
	Dequeues  [numTiers]uint64
}

// Queue is a bounded multi-tier queue.
type Queue struct {
	mu        sync.Mutex
	tiers     [numTiers]*list.List // of *Item
	capacity  int
	lifoAbove int
	closed    bool
	stats     Stats
	now       func() time.Time
}

// New builds a queue holding at most capacity waiters in total. Requests
// arriving at a full queue shed the lowest-priority waiter rather than being
// rejected themselves, so a flood of bulk traffic cannot lock out critical
// traffic by getting there first.
//
// lifoAbove is the total depth past which each tier switches to newest-first.
func New(capacity, lifoAbove int) *Queue {
	q := &Queue{capacity: capacity, lifoAbove: lifoAbove, now: time.Now}
	for i := range q.tiers {
		q.tiers[i] = list.New()
	}
	return q
}

func (q *Queue) SetClock(now func() time.Time) {
	q.mu.Lock()
	q.now = now
	q.mu.Unlock()
}

// Enqueue adds an item, shedding to make room if necessary. It returns false
// (with the item's Ready channel already carrying Shed) when the item itself is
// the one that has to give way.
func (q *Queue) Enqueue(it *Item) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		it.Ready <- Closed
		return false
	}

	if q.depthLocked() >= q.capacity {
		victim, tier := q.lowestLocked()
		if victim == nil || tier < it.Priority ||
			(tier == it.Priority && q.tiers[tier].Len() == 0) {
			// Nothing less important than the arriving request: it sheds.
			q.stats.Shed[it.Priority]++
			it.Ready <- Shed
			return false
		}
		if tier < it.Priority {
			q.stats.Shed[it.Priority]++
			it.Ready <- Shed
			return false
		}
		q.tiers[tier].Remove(victim)
		victim.Value.(*Item).Ready <- Shed
		q.stats.Shed[tier]++
	}

	it.Enqueued = q.now()
	q.tiers[it.Priority].PushBack(it)
	if d := q.depthLocked(); d > q.stats.MaxDepth {
		q.stats.MaxDepth = d
	}
	return true
}

// Dequeue returns the next item to run, skipping any whose deadline has passed.
// It returns nil when the queue is empty.
func (q *Queue) Dequeue() *Item {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	lifo := q.depthLocked() > q.lifoAbove

	for tier := Priority(0); tier < numTiers; tier++ {
		l := q.tiers[tier]
		for l.Len() > 0 {
			var el *list.Element
			if lifo {
				el = l.Back()
			} else {
				el = l.Front()
			}
			l.Remove(el)
			it := el.Value.(*Item)

			if !it.Deadline.IsZero() && now.After(it.Deadline) {
				// Dead on arrival at the head of the queue. Drop it here rather
				// than spend an upstream call on a client that has gone.
				q.stats.Expired[tier]++
				it.Ready <- Expired
				continue
			}
			if lifo {
				q.stats.LIFOSwaps++
			}
			q.stats.Admitted[tier]++
			q.stats.Dequeues[tier]++
			q.stats.WaitNanos[tier] += uint64(now.Sub(it.Enqueued))
			it.Ready <- Admitted
			return it
		}
	}
	return nil
}

// ExpireOverdue drops every waiter whose deadline has passed. Dequeue already
// does this lazily; a periodic sweep matters because a queue that is not being
// drained (upstream stalled) would otherwise hold clients until they time out
// on their own, which is exactly when telling them early is most useful.
func (q *Queue) ExpireOverdue() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	dropped := 0
	for tier := Priority(0); tier < numTiers; tier++ {
		l := q.tiers[tier]
		for el := l.Front(); el != nil; {
			next := el.Next()
			it := el.Value.(*Item)
			if !it.Deadline.IsZero() && now.After(it.Deadline) {
				l.Remove(el)
				q.stats.Expired[tier]++
				it.Ready <- Expired
				dropped++
			}
			el = next
		}
	}
	return dropped
}

// Close drains the queue, telling every waiter to give up.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for tier := range q.tiers {
		l := q.tiers[tier]
		for l.Len() > 0 {
			el := l.Front()
			l.Remove(el)
			el.Value.(*Item).Ready <- Closed
		}
	}
}

func (q *Queue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depthLocked()
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	for i := range q.tiers {
		s.Depth[i] = q.tiers[i].Len()
	}
	return s
}

func (q *Queue) depthLocked() int {
	n := 0
	for _, l := range q.tiers {
		n += l.Len()
	}
	return n
}

// lowestLocked finds the least important waiter, which is the one to shed.
func (q *Queue) lowestLocked() (*list.Element, Priority) {
	for tier := Priority(numTiers - 1); tier >= 0; tier-- {
		if q.tiers[tier].Len() > 0 {
			// Shed the *oldest* of the least important: it has waited longest
			// and is closest to timing out anyway.
			return q.tiers[tier].Front(), tier
		}
	}
	return nil, 0
}
