// Package upstream is a backend whose behaviour you can program.
//
// Overload experiments need a dependency that misbehaves on cue and identically
// every run. A real service will not do that, and a sleep(50ms) handler is not
// a service: its latency does not depend on how many requests are already in
// flight, so it never queues, so it never exhibits the behaviour the whole
// gateway exists to handle.
//
// This one has a fixed number of workers. Requests beyond that wait, exactly as
// they would behind a real connection pool or thread pool, which means latency
// rises with offered load on its own rather than because the simulator was told
// to raise it. That single property is what makes the resulting latency-versus-
// load curve worth plotting.
//
// On top of that, service time is drawn from a bimodal distribution — most
// requests fast, a programmable fraction slow — because a unimodal latency
// distribution has no tail, and a tail is the thing being measured.
package upstream

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Profile describes how the simulated backend behaves.
type Profile struct {
	// Workers is the concurrency the backend can actually serve. Requests
	// beyond this queue, which is where latency under load comes from.
	Workers int `json:"workers"`
	// Fast and Slow are the two modes of the service-time distribution.
	Fast time.Duration `json:"fast"`
	Slow time.Duration `json:"slow"`
	// SlowFraction is how often the slow mode is drawn, 0..1.
	SlowFraction float64 `json:"slow_fraction"`
	// Jitter is added uniformly to whichever mode was drawn.
	Jitter time.Duration `json:"jitter"`
	// ErrorRate is the fraction of requests answered with 503.
	ErrorRate float64 `json:"error_rate"`
	// StallFor, when non-zero, multiplies every service time. This is the knob
	// a scenario turns to simulate a dependency going bad.
	LatencyMultiplier float64 `json:"latency_multiplier"`
	// QueueCap bounds the internal wait queue; beyond it the backend itself
	// returns 503, which is what a real server's accept queue does.
	QueueCap int `json:"queue_cap"`
}

func DefaultProfile() Profile {
	return Profile{
		Workers: 32, Fast: 5 * time.Millisecond, Slow: 60 * time.Millisecond,
		SlowFraction: 0.05, Jitter: 2 * time.Millisecond,
		LatencyMultiplier: 1, QueueCap: 4096,
	}
}

// Server is an http.Handler implementing the profile.
type Server struct {
	mu      sync.RWMutex
	profile Profile
	sem     chan struct{}

	served   atomic.Uint64
	rejected atomic.Uint64
	errors   atomic.Uint64
	inFlight atomic.Int64
	queued   atomic.Int64
	maxSeen  atomic.Int64

	rndMu sync.Mutex
	rnd   *rand.Rand
}

func New(p Profile, seed int64) *Server {
	if p.Workers < 1 {
		p.Workers = 1
	}
	if p.LatencyMultiplier == 0 {
		p.LatencyMultiplier = 1
	}
	return &Server{
		profile: p,
		sem:     make(chan struct{}, p.Workers),
		rnd:     rand.New(rand.NewSource(seed)),
	}
}

// SetProfile changes behaviour at runtime. Scenario scripts call this to make a
// dependency go bad at a known second and recover at another.
func (s *Server) SetProfile(p Profile) {
	if p.Workers < 1 {
		p.Workers = 1
	}
	if p.LatencyMultiplier == 0 {
		p.LatencyMultiplier = 1
	}
	s.mu.Lock()
	// The worker semaphore is resized by replacing it; in-flight requests hold
	// slots on the old channel and release them harmlessly.
	if p.Workers != s.profile.Workers {
		s.sem = make(chan struct{}, p.Workers)
	}
	s.profile = p
	s.mu.Unlock()
}

func (s *Server) Profile() Profile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.profile
}

func (s *Server) serviceTime() (time.Duration, bool) {
	s.mu.RLock()
	p := s.profile
	s.mu.RUnlock()

	s.rndMu.Lock()
	slow := s.rnd.Float64() < p.SlowFraction
	jit := time.Duration(0)
	if p.Jitter > 0 {
		jit = time.Duration(s.rnd.Int63n(int64(p.Jitter)))
	}
	bad := s.rnd.Float64() < p.ErrorRate
	s.rndMu.Unlock()

	base := p.Fast
	if slow {
		base = p.Slow
	}
	d := time.Duration(float64(base+jit) * p.LatencyMultiplier)
	return d, bad
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	sem, cap_ := s.sem, s.profile.QueueCap
	s.mu.RUnlock()

	q := s.queued.Add(1)
	if m := s.maxSeen.Load(); q > m {
		s.maxSeen.Store(q)
	}
	if cap_ > 0 && int(q) > cap_ {
		s.queued.Add(-1)
		s.rejected.Add(1)
		http.Error(w, "backend queue full", http.StatusServiceUnavailable)
		return
	}

	select {
	case sem <- struct{}{}:
	case <-r.Context().Done():
		// The client gave up while queued. Releasing here is what makes
		// cancellation visible as reclaimed capacity rather than as a leak.
		s.queued.Add(-1)
		return
	}
	s.queued.Add(-1)
	s.inFlight.Add(1)
	defer func() {
		s.inFlight.Add(-1)
		<-sem
	}()

	d, bad := s.serviceTime()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
		return
	}

	if bad {
		s.errors.Add(1)
		http.Error(w, "simulated upstream error", http.StatusServiceUnavailable)
		return
	}
	s.served.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true, "service_ns": d.Nanoseconds(),
	})
}

type Stats struct {
	Served     uint64  `json:"served"`
	Rejected   uint64  `json:"rejected"`
	Errors     uint64  `json:"errors"`
	InFlight   int64   `json:"in_flight"`
	Queued     int64   `json:"queued"`
	MaxQueued  int64   `json:"max_queued"`
	Workers    int     `json:"workers"`
	Multiplier float64 `json:"latency_multiplier"`
}

func (s *Server) Stats() Stats {
	p := s.Profile()
	return Stats{
		Served: s.served.Load(), Rejected: s.rejected.Load(),
		Errors: s.errors.Load(), InFlight: s.inFlight.Load(),
		Queued: s.queued.Load(), MaxQueued: s.maxSeen.Load(),
		Workers: p.Workers, Multiplier: p.LatencyMultiplier,
	}
}

// AdminHandler exposes stats and profile changes, so a scenario script can make
// the backend misbehave without restarting anything.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Stats())
	})
	mux.HandleFunc("/profile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(s.Profile())
			return
		}
		var p Profile
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetProfile(p)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
