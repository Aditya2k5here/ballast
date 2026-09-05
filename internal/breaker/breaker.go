// Package breaker stops sending requests to an upstream that is not answering.
//
// The value of a circuit breaker is not that it protects the upstream, the
// upstream is already down. It is that it protects the *caller*: an open
// breaker turns a ten-second timeout into an immediate error, which frees the
// concurrency slot, which keeps the queue behind it from growing, which is what
// stops one failing dependency taking the whole gateway with it.
//
// The state machine is the usual three, with one detail that is easy to get
// wrong: the half-open state admits exactly one probe at a time, not a burst.
// A breaker that admits ten probes to a still-broken upstream has reinvented
// the problem it exists to solve.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned instead of calling an upstream that is known to be down.
var ErrOpen = errors.New("circuit breaker open")

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	}
	return "half-open"
}

type Config struct {
	// FailureThreshold is how many consecutive failures trip the breaker.
	FailureThreshold int
	// SuccessThreshold is how many consecutive successes in half-open close it.
	SuccessThreshold int
	// OpenFor is how long to stay open before admitting a probe.
	OpenFor time.Duration
	// MinSamples avoids tripping on the first two requests after a deploy.
	MinSamples int
}

func Default() Config {
	return Config{FailureThreshold: 20, SuccessThreshold: 3,
		OpenFor: 5 * time.Second, MinSamples: 10}
}

type Breaker struct {
	cfg Config

	mu          sync.Mutex
	state       State
	consecFail  int
	consecOK    int
	samples     int
	openedAt    time.Time
	probing     bool
	transitions int
	rejected    uint64
	now         func() time.Time
}

func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg, state: Closed, now: time.Now}
}

func (b *Breaker) SetClock(now func() time.Time) {
	b.mu.Lock()
	b.now = now
	b.mu.Unlock()
}

// Allow reports whether a request may proceed. When it returns true in the
// half-open state, the caller holds the single probe slot and must report the
// outcome, or the breaker stays half-open forever.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.openedAt) >= b.cfg.OpenFor {
			b.state = HalfOpen
			b.consecOK = 0
			b.probing = true
			b.transitions++
			return true
		}
		b.rejected++
		return false
	default: // HalfOpen
		if b.probing {
			// A probe is already in flight. One at a time is the whole point.
			b.rejected++
			return false
		}
		b.probing = true
		return true
	}
}

// Report records the outcome of a request that Allow permitted.
func (b *Breaker) Report(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.samples++

	if b.state == HalfOpen {
		b.probing = false
		if ok {
			b.consecOK++
			if b.consecOK >= b.cfg.SuccessThreshold {
				b.state = Closed
				b.consecFail, b.consecOK = 0, 0
				b.transitions++
			}
		} else {
			b.state = Open
			b.openedAt = b.now()
			b.consecOK = 0
			b.transitions++
		}
		return
	}

	if ok {
		b.consecFail = 0
		return
	}
	b.consecFail++
	if b.state == Closed && b.samples >= b.cfg.MinSamples &&
		b.consecFail >= b.cfg.FailureThreshold {
		b.state = Open
		b.openedAt = b.now()
		b.transitions++
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Transitions() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.transitions
}

func (b *Breaker) Rejected() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rejected
}
