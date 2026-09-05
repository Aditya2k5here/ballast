// Package loadgen issues requests on a schedule and measures from the moment
// each request was *due*, not the moment it was sent.
//
// This is the single most important detail in the whole project, and it is the
// one most load tests get wrong.
//
// A closed-loop generator runs N workers in a loop: send, wait for the reply,
// send again. When the server slows down, the generator slows down with it — it
// simply issues fewer requests. So the offered load is not the load you
// configured, it is whatever the server felt like accepting, and the recorded
// latencies are only the latencies of requests the server had capacity for. The
// requests that would have been slowest are the ones that were never sent.
//
// Gil Tene named this coordinated omission. Its signature is a load test that
// reports a healthy p99 for a system that is visibly falling over, and it is
// why so many published latency numbers are fiction.
//
// The generator here is open-loop. Requests are scheduled at a fixed arrival
// rate and issued whether or not earlier ones have returned, each in its own
// goroutine. Latency is recorded as (completed − intended send time), so a
// request delayed by two seconds because the generator itself was busy is
// reported as a two-second request, which is what the client would have
// experienced. When the system cannot keep up, that shows up in the numbers
// rather than disappearing from them.
package loadgen

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Aditya2k5here/ballast/internal/hist"
)

// Class is one stream of traffic: a share of the offered rate, sent with a
// given priority header and deadline.
type Class struct {
	Name     string
	Priority string        // sent as X-Priority
	Share    float64       // fraction of total rate
	Deadline time.Duration // sent as X-Deadline-Ms; zero means none
}

type Config struct {
	Target      string        // URL to hit
	Rate        float64       // requests per second, offered regardless of replies
	Duration    time.Duration //
	Classes     []Class
	Timeout     time.Duration // client-side give-up
	Seed        int64
	MaxInFlight int // safety valve; a hard cap on outstanding goroutines
}

// Result is one traffic class's outcome.
type Result struct {
	Class     string
	Sent      uint64
	OK        uint64
	Shed      uint64 // 429 or 503 from the gateway: a fast, honest refusal
	Failed    uint64 // timeouts, connection errors, 5xx
	Latency   hist.Snapshot
	OKLatency hist.Snapshot
	Abandoned uint64 // never issued because MaxInFlight was hit
}

type Summary struct {
	Results     []Result
	OfferedRate float64
	Achieved    float64
	Wall        time.Duration
	Overall     hist.Snapshot
}

type classState struct {
	class     Class
	sent      atomic.Uint64
	ok        atomic.Uint64
	shed      atomic.Uint64
	failed    atomic.Uint64
	abandoned atomic.Uint64
	all       hist.Histogram
	okOnly    hist.Histogram
}

// Run issues the load and returns what happened.
func Run(ctx context.Context, cfg Config) (Summary, error) {
	if cfg.Rate <= 0 {
		return Summary{}, fmt.Errorf("rate must be positive")
	}
	if len(cfg.Classes) == 0 {
		cfg.Classes = []Class{{Name: "default", Priority: "1", Share: 1}}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 200000
	}

	// One connection pool, sized generously: the generator must not become the
	// bottleneck it is trying to measure.
	client := &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        20000,
			MaxIdleConnsPerHost: 20000,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	states := make([]*classState, len(cfg.Classes))
	cum := make([]float64, len(cfg.Classes))
	var total float64
	for i, c := range cfg.Classes {
		states[i] = &classState{class: c}
		total += c.Share
	}
	acc := 0.0
	for i, c := range cfg.Classes {
		acc += c.Share / total
		cum[i] = acc
	}

	rnd := rand.New(rand.NewSource(cfg.Seed))
	interval := time.Duration(float64(time.Second) / cfg.Rate)
	start := time.Now()
	end := start.Add(cfg.Duration)

	var wg sync.WaitGroup
	var inFlight atomic.Int64
	var issued uint64

	// The schedule is computed from the start time, never from "now", so a slow
	// iteration cannot push later requests back. That is the property that makes
	// this open-loop.
	for n := int64(0); ; n++ {
		due := start.Add(time.Duration(n) * interval)
		if !due.Before(end) {
			break
		}
		if d := time.Until(due); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				goto done
			}
		}
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		idx := pick(cum, rnd.Float64())
		st := states[idx]
		issued++

		if inFlight.Load() >= int64(cfg.MaxInFlight) {
			// Recorded, not silently skipped: an omission the report can see.
			st.abandoned.Add(1)
			continue
		}
		inFlight.Add(1)
		wg.Add(1)
		go func(st *classState, due time.Time) {
			defer wg.Done()
			defer inFlight.Add(-1)
			issue(ctx, client, cfg, st, due)
		}(st, due)
	}

done:
	wg.Wait()
	wall := time.Since(start)

	sum := Summary{OfferedRate: cfg.Rate, Wall: wall}
	var overall hist.Histogram
	var completed uint64
	for _, st := range states {
		overall.Merge(&st.all)
		completed += st.ok.Load() + st.shed.Load() + st.failed.Load()
		sum.Results = append(sum.Results, Result{
			Class: st.class.Name, Sent: st.sent.Load(), OK: st.ok.Load(),
			Shed: st.shed.Load(), Failed: st.failed.Load(),
			Abandoned: st.abandoned.Load(),
			Latency:   st.all.Snapshot(), OKLatency: st.okOnly.Snapshot(),
		})
	}
	sum.Overall = overall.Snapshot()
	sum.Achieved = float64(completed) / wall.Seconds()
	sort.Slice(sum.Results, func(i, j int) bool {
		return sum.Results[i].Class < sum.Results[j].Class
	})
	return sum, nil
}

func issue(ctx context.Context, client *http.Client, cfg Config,
	st *classState, due time.Time) {
	st.sent.Add(1)

	reqCtx := ctx
	var cancel context.CancelFunc
	if st.class.Deadline > 0 {
		// The deadline is measured from when the request was *due*, which is
		// what the client's own clock would say.
		reqCtx, cancel = context.WithDeadline(ctx, due.Add(st.class.Deadline))
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, cfg.Target, nil)
	if err != nil {
		st.failed.Add(1)
		return
	}
	if st.class.Priority != "" {
		req.Header.Set("X-Priority", st.class.Priority)
	}
	if st.class.Deadline > 0 {
		req.Header.Set("X-Deadline-Ms",
			fmt.Sprintf("%d", st.class.Deadline.Milliseconds()))
	}

	resp, err := client.Do(req)
	// Measured from the intended send time. This is the whole point.
	elapsed := time.Since(due).Nanoseconds()
	st.all.Record(elapsed)

	if err != nil {
		st.failed.Add(1)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode == http.StatusServiceUnavailable:
		st.shed.Add(1)
	case resp.StatusCode >= 500:
		st.failed.Add(1)
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		st.ok.Add(1)
		st.okOnly.Record(elapsed)
	default:
		st.failed.Add(1)
	}
}

func pick(cum []float64, u float64) int {
	for i, c := range cum {
		if u <= c {
			return i
		}
	}
	return len(cum) - 1
}

// Report renders a summary the way it should be read: per class, with sheds
// counted separately from failures, because a fast 429 and a ten-second timeout
// are the same row in most load-test output and are not remotely the same
// event.
func (s Summary) Report() string {
	out := fmt.Sprintf("offered %.0f rps for %s, completed %.0f rps\n",
		s.OfferedRate, s.Wall.Round(time.Millisecond), s.Achieved)
	out += fmt.Sprintf("  %-10s %8s %8s %8s %8s   %s\n",
		"class", "sent", "ok", "shed", "failed", "latency from intended send")
	for _, r := range s.Results {
		out += fmt.Sprintf("  %-10s %8d %8d %8d %8d   %s\n",
			r.Class, r.Sent, r.OK, r.Shed, r.Failed, r.Latency.Percentiles())
		if r.Abandoned > 0 {
			out += fmt.Sprintf("  %-10s %8s generator could not issue %d requests\n",
				"", "!", r.Abandoned)
		}
	}
	return out
}
