// Package budget caps retries as a fraction of traffic, rather than per call.
//
// Per-call retry policies are the standard way to turn a partial outage into a
// total one. Every caller independently decides that three attempts is
// reasonable, and when the upstream starts failing, offered load triples at
// exactly the moment capacity has fallen, so more requests fail, so more are
// retried. The system is now doing its maximum possible work and completing
// almost none of it.
//
// A budget removes the coupling. Retries draw from a shared allowance.
// typically ten percent of original requests over a sliding window, and when
// the allowance is gone, retries simply stop. The important property is what
// happens at the worst moment: as failures rise, originals stay flat, so the
// allowance stays flat, so retry load is bounded no matter how bad it gets.
//
// This is the shape gRPC and Envoy both settled on, and it is one of the very
// few resilience mechanisms whose behaviour improves as the incident worsens.
package budget

import (
	"sync"
	"time"
)

// Budget is a sliding-window ratio limiter over retries.
type Budget struct {
	mu sync.Mutex

	ratio     float64 // retries permitted per original request
	minPerSec float64 // a floor, so a low-traffic service can still retry once
	window    time.Duration
	buckets   int

	origin  []int64 // ring of original-request counts
	retry   []int64 // ring of retry counts
	idx     int
	lastAt  time.Time
	granted uint64
	denied  uint64
	now     func() time.Time
}

// New builds a budget over a sliding window, divided into buckets so the window
// slides smoothly rather than resetting in steps. A step reset lets a burst of
// retries land in the first instant of each window, which is the behaviour a
// budget exists to prevent.
func New(ratio float64, minPerSec float64, window time.Duration, buckets int) *Budget {
	if buckets < 2 {
		buckets = 10
	}
	b := &Budget{
		ratio: ratio, minPerSec: minPerSec, window: window, buckets: buckets,
		origin: make([]int64, buckets), retry: make([]int64, buckets),
		now: time.Now,
	}
	b.lastAt = b.now()
	return b
}

func Default() *Budget { return New(0.1, 1, 10*time.Second, 10) }

func (b *Budget) SetClock(now func() time.Time) {
	b.mu.Lock()
	b.now = now
	b.lastAt = now()
	b.mu.Unlock()
}

// Original records a first attempt. Every request must be reported here, or the
// allowance has nothing to be a fraction of.
func (b *Budget) Original() {
	b.mu.Lock()
	b.advance()
	b.origin[b.idx]++
	b.mu.Unlock()
}

// Withdraw asks permission to retry. It returns false when the allowance is
// spent, and the caller must then fail rather than try again.
func (b *Budget) Withdraw() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()

	var originals, retries int64
	for i := range b.origin {
		originals += b.origin[i]
		retries += b.retry[i]
	}
	allowance := float64(originals)*b.ratio +
		b.minPerSec*b.window.Seconds()
	if float64(retries)+1 > allowance {
		b.denied++
		return false
	}
	b.retry[b.idx]++
	b.granted++
	return true
}

// advance rolls the ring forward, zeroing buckets that have aged out.
func (b *Budget) advance() {
	per := b.window / time.Duration(b.buckets)
	now := b.now()
	steps := int(now.Sub(b.lastAt) / per)
	if steps <= 0 {
		return
	}
	if steps > b.buckets {
		steps = b.buckets
	}
	for i := 0; i < steps; i++ {
		b.idx = (b.idx + 1) % b.buckets
		b.origin[b.idx] = 0
		b.retry[b.idx] = 0
	}
	b.lastAt = b.lastAt.Add(time.Duration(steps) * per)
}

type Stats struct {
	Granted   uint64
	Denied    uint64
	Originals int64
	Retries   int64
}

func (b *Budget) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	var s Stats
	s.Granted, s.Denied = b.granted, b.denied
	for i := range b.origin {
		s.Originals += b.origin[i]
		s.Retries += b.retry[i]
	}
	return s
}
