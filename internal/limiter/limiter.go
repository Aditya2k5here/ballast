// Package limiter decides how many requests may be in flight at once, by
// measuring latency rather than by being told a number.
//
// The number it is looking for is the concurrency at which the upstream sits at
// the knee of its throughput curve. Little's Law says that concurrency is
// throughput times latency, so if the system's capacity is fixed, pushing
// concurrency past the knee cannot raise throughput and can only raise latency
// . Every extra request in flight is a request queued behind the others.
//
// A fixed MaxConns is a guess at that number. It is right for one backend on
// one day, and it is wrong in both directions: too low leaves capacity unused,
// too high converts a slow dependency into an outage, because the limit no
// longer limits anything and the queue grows without bound.
//
// The Gradient limiter here follows the approach Netflix's concurrency-limits
// library uses. It tracks the shortest round-trip latency it has recently seen
// as an estimate of "no queueing", compares the current latency against it, and
// scales the limit by that ratio, with a headroom term that keeps probing
// upwards so the limit can grow when the upstream gets faster.
package limiter

import (
	"math"
	"sync"
	"time"
)

// Limiter is the interface the proxy uses. Both implementations are here so the
// experiment can compare adaptive control against the fixed limit it replaces
// without the proxy knowing which it has.
type Limiter interface {
	// Acquire takes a slot if one is free. The returned bool says whether it
	// succeeded; the caller must call the returned release func if it did.
	Acquire() (release func(rtt time.Duration, dropped bool), ok bool)
	Limit() int
	InFlight() int
	Name() string
}

// -- fixed -------------------------------------------------------------------

// Fixed is the baseline: a constant number of slots. It exists to be beaten,
// and to make the comparison in the results a comparison rather than a claim.
type Fixed struct {
	mu       sync.Mutex
	limit    int
	inFlight int
}

func NewFixed(limit int) *Fixed {
	if limit < 1 {
		limit = 1
	}
	return &Fixed{limit: limit}
}

func (f *Fixed) Acquire() (func(time.Duration, bool), bool) {
	f.mu.Lock()
	if f.inFlight >= f.limit {
		f.mu.Unlock()
		return nil, false
	}
	f.inFlight++
	f.mu.Unlock()
	return func(time.Duration, bool) {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}, true
}

func (f *Fixed) Limit() int    { return f.limit }
func (f *Fixed) InFlight() int { f.mu.Lock(); defer f.mu.Unlock(); return f.inFlight }
func (f *Fixed) Name() string  { return "fixed" }

// -- gradient ----------------------------------------------------------------

// GradientConfig tunes the controller. The defaults are the ones the results
// were produced with.
type GradientConfig struct {
	Min          int           // never go below this; 1 would deadlock a slow upstream
	Max          int           // an upper bound, so a broken RTT estimate cannot run away
	Initial      int           // starting limit
	Smoothing    float64       // how fast the limit may move, 0..1
	RTTTolerance float64       // treat RTT within this factor of the floor as "no queueing"
	Window       time.Duration // how often the limit is recomputed
	MinSamples   int           // do not adjust on fewer than this many samples
	FloorDecay   time.Duration // how long the long-term RTT floor is trusted
}

func DefaultGradient() GradientConfig {
	return GradientConfig{
		Min: 4, Max: 2000, Initial: 20,
		Smoothing: 0.2, RTTTolerance: 1.5,
		Window: 500 * time.Millisecond, MinSamples: 8,
		FloorDecay: 10 * time.Second,
	}
}

// Gradient adjusts its limit from observed round-trip latency.
//
// Each window it compares the shortest RTT seen in that window against a
// longer-lived floor. The ratio is the "gradient": 1.0 means no queueing has
// appeared, below 1.0 means latency has grown and the limit should shrink in
// proportion. A square-root headroom term is added so the limit keeps probing
// upwards. Without it the controller settles at whatever it first found and
// never discovers that the upstream recovered.
type Gradient struct {
	cfg GradientConfig

	mu        sync.Mutex
	limit     float64
	inFlight  int
	winMinRTT time.Duration
	winCount  int
	winDrops  int
	winMaxUse int
	floorRTT  time.Duration
	floorAt   time.Time
	nextAdj   time.Time
	adjust    int
	now       func() time.Time // injectable, so tests do not sleep
}

func NewGradient(cfg GradientConfig) *Gradient {
	if cfg.Initial < cfg.Min {
		cfg.Initial = cfg.Min
	}
	g := &Gradient{cfg: cfg, limit: float64(cfg.Initial), now: time.Now}
	g.nextAdj = g.now().Add(cfg.Window)
	return g
}

// SetClock replaces the time source. Tests drive the controller through many
// windows in microseconds; a controller that can only be tested in real time is
// a controller that does not get tested.
func (g *Gradient) SetClock(now func() time.Time) {
	g.mu.Lock()
	g.now = now
	g.nextAdj = now().Add(g.cfg.Window)
	g.mu.Unlock()
}

func (g *Gradient) Acquire() (func(time.Duration, bool), bool) {
	g.mu.Lock()
	if float64(g.inFlight) >= g.limit {
		g.mu.Unlock()
		return nil, false
	}
	g.inFlight++
	if g.inFlight > g.winMaxUse {
		g.winMaxUse = g.inFlight
	}
	g.mu.Unlock()
	return g.release, true
}

func (g *Gradient) release(rtt time.Duration, dropped bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inFlight--

	if dropped {
		// A failed request tells us nothing about latency under load. It may
		// have been fast precisely because it failed, but it is strong
		// evidence about capacity, so it is counted separately.
		g.winDrops++
	} else {
		if g.winCount == 0 || rtt < g.winMinRTT {
			g.winMinRTT = rtt
		}
		g.winCount++
	}

	now := g.now()
	if now.Before(g.nextAdj) {
		return
	}
	g.nextAdj = now.Add(g.cfg.Window)
	g.recompute(now)
}

func (g *Gradient) recompute(now time.Time) {
	defer func() {
		g.winCount, g.winDrops, g.winMinRTT, g.winMaxUse = 0, 0, 0, 0
	}()

	if g.winDrops > 0 && g.winCount == 0 {
		// Nothing but failures. Back off hard rather than guess.
		g.limit = math.Max(float64(g.cfg.Min), g.limit*0.5)
		g.adjust++
		return
	}
	if g.winCount < g.cfg.MinSamples {
		return
	}

	// The floor is the best RTT seen recently. It has to decay, or one lucky
	// measurement during a quiet period permanently convinces the controller
	// that the upstream is faster than it is.
	if g.floorRTT == 0 || g.winMinRTT < g.floorRTT ||
		now.Sub(g.floorAt) > g.cfg.FloorDecay {
		g.floorRTT = g.winMinRTT
		g.floorAt = now
	}

	// Gradient below 1 means queueing has appeared. Tolerance keeps ordinary
	// jitter from being mistaken for congestion.
	gradient := (float64(g.floorRTT) * g.cfg.RTTTolerance) / float64(g.winMinRTT)
	if gradient > 1 {
		gradient = 1
	}
	if gradient < 0.5 {
		gradient = 0.5 // never more than halve in one window
	}

	// Probing upwards. The additive sqrt(limit) step is the safe default, but on
	// its own it is far too slow to follow capacity that arrives suddenly: in the
	// capacity-swing experiment the backend went from 8 workers to 64 and this
	// controller crawled from 17 to 41 before the capacity went away again,
	// leaving most of it unused.
	//
	// So when there is no sign of queueing *and* the limit is actually binding.
	// in-flight has been pressed right up against it, the step becomes
	// multiplicative. Both conditions matter: raising a limit that nothing is
	// reaching achieves nothing except a larger number to fall from later.
	headroom := math.Sqrt(g.limit)
	binding := float64(g.winMaxUse) >= 0.9*g.limit

	// Downward movement is always allowed: latency can rise because of load this
	// limiter is not causing, and the limit should still come down. Upward
	// movement requires evidence that the limit is what is holding traffic back
	// -- otherwise the controller inflates a number nothing is reaching, and the
	// only effect is a larger height to fall from when load does arrive.
	target := g.limit * gradient
	if binding {
		target += headroom
		if gradient >= 1 {
			target = g.limit*1.5 + headroom
		}
	}
	g.limit = g.limit*(1-g.cfg.Smoothing) + target*g.cfg.Smoothing
	g.limit = math.Min(math.Max(g.limit, float64(g.cfg.Min)), float64(g.cfg.Max))
	g.adjust++
}

func (g *Gradient) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int(g.limit)
}

func (g *Gradient) InFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
}

func (g *Gradient) Adjustments() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.adjust
}

// FloorRTT is the controller's current estimate of the upstream's unqueued
// latency. Exported because it is the number to look at first when the limiter
// is behaving strangely.
func (g *Gradient) FloorRTT() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.floorRTT
}

func (g *Gradient) Name() string { return "gradient" }
