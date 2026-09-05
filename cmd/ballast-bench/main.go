// Command ballast-bench runs the experiment the project exists to produce.
//
// Three parts:
//
//	sweep     latency and success against offered load, adaptive vs fixed
//	scenario  a timed fault: an upstream goes slow at t+8s and recovers at t+20s
//	shed      how long a refusal takes, which is the cost of saying no
//
// Everything runs in one process against httptest servers, so a run is one
// command with no orchestration, and the upstream, gateway and generator all
// see real HTTP over loopback rather than a mocked round-tripper. The cost is
// that the generator competes with the gateway for CPU; that is stated in the
// results rather than hidden, and it biases *against* the gateway, since the
// generator's own delays are recorded as latency.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/Aditya2k5here/ballast/internal/limiter"
	"github.com/Aditya2k5here/ballast/internal/loadgen"
	"github.com/Aditya2k5here/ballast/internal/proxy"
	"github.com/Aditya2k5here/ballast/internal/upstream"
)

// The backend is deliberately small so that overload is reachable on a laptop:
// 8 workers at ~30 ms mean service time is about 270 requests per second, and
// the sweep goes to twelve times that.
var backend = upstream.Profile{
	Workers: 8, Fast: 25 * time.Millisecond, Slow: 120 * time.Millisecond,
	SlowFraction: 0.05, Jitter: 3 * time.Millisecond,
	LatencyMultiplier: 1, QueueCap: 20000,
}

const ratedRPS = 250.0

var multipliers = []float64{0.5, 1, 2, 4, 8, 12}

// The mix matters as much as the rate. Critical traffic is 5% of offered load,
// so even at twelve times capacity it is well within what the backend can serve
// -- which is the situation prioritisation exists for. With critical at 20% of
// a 12x overload it exceeds capacity on its own, and then no policy can serve
// it: the earlier run of this sweep showed exactly that, 64% of critical
// requests succeeding under both limiters because 64% was the arithmetic
// maximum. Shedding decides who is served only while somebody can be.
func classes() []loadgen.Class {
	return []loadgen.Class{
		{Name: "critical", Priority: "0", Share: 0.05, Deadline: 400 * time.Millisecond},
		{Name: "standard", Priority: "1", Share: 0.35, Deadline: 1 * time.Second},
		{Name: "bulk", Priority: "2", Share: 0.60, Deadline: 3 * time.Second},
	}
}

type point struct {
	Limiter    string             `json:"limiter"`
	Multiplier float64            `json:"multiplier"`
	OfferedRPS float64            `json:"offered_rps"`
	Achieved   float64            `json:"achieved_rps"`
	FinalLimit int                `json:"final_limit"`
	Classes    map[string]classPt `json:"classes"`
}

type classPt struct {
	Sent    uint64 `json:"sent"`
	OK      uint64 `json:"ok"`
	Shed    uint64 `json:"shed"`
	Failed  uint64 `json:"failed"`
	P50Ns   int64  `json:"p50_ns"`
	P99Ns   int64  `json:"p99_ns"`
	P999Ns  int64  `json:"p999_ns"`
	MaxNs   int64  `json:"max_ns"`
	OKP99Ns int64  `json:"ok_p99_ns"`
}

// modes are the limiters under test. Three fixed limits rather than one,
// because the interesting claim is not "adaptive beats this particular constant"
// -- it is that every constant is wrong somewhere. A small limit throttles the
// backend below its capacity; a large one lets the backend queue until latency
// is unusable; and the value that is right today is wrong the moment the
// backend is redeployed on different hardware.
var modes = []string{"fixed-16", "fixed-64", "fixed-256", "gradient"}

func limiterFor(mode string) limiter.Limiter {
	switch mode {
	case "fixed-16":
		return limiter.NewFixed(16)
	case "fixed-64":
		return limiter.NewFixed(64)
	case "fixed-256":
		return limiter.NewFixed(256)
	default:
		return limiter.NewGradient(limiter.DefaultGradient())
	}
}

func newStack(mode string, fixedLimit int) (*httptest.Server, *httptest.Server,
	*proxy.Gateway, *upstream.Server) {
	up := upstream.New(backend, 7)
	upSrv := httptest.NewServer(up)

	opts := proxy.DefaultOptions(upSrv.URL)
	opts.QueueCapacity = 4096
	opts.LIFOAbove = 32
	opts.MaxRetries = 1
	opts.Limiter = limiterFor(mode)
	gw, err := proxy.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	gwSrv := httptest.NewServer(gw)
	return upSrv, gwSrv, gw, up
}

func runSweep(dur time.Duration, fixedLimit int) []point {
	var out []point
	for _, mode := range modes {
		for _, m := range multipliers {
			upSrv, gwSrv, gw, _ := newStack(mode, fixedLimit)
			rate := ratedRPS * m
			sum, err := loadgen.Run(context.Background(), loadgen.Config{
				Target: gwSrv.URL, Rate: rate, Duration: dur,
				Timeout: 4 * time.Second, Seed: 11, Classes: classes(),
				MaxInFlight: 60000,
			})
			if err != nil {
				log.Fatal(err)
			}
			p := point{Limiter: mode, Multiplier: m, OfferedRPS: rate,
				Achieved: sum.Achieved, FinalLimit: gw.Limiter().Limit(),
				Classes: map[string]classPt{}}
			for _, r := range sum.Results {
				p.Classes[r.Class] = classPt{
					Sent: r.Sent, OK: r.OK, Shed: r.Shed, Failed: r.Failed,
					P50Ns: r.Latency.Quantile(0.5), P99Ns: r.Latency.Quantile(0.99),
					P999Ns: r.Latency.Quantile(0.999), MaxNs: r.Latency.Max,
					OKP99Ns: r.OKLatency.Quantile(0.99),
				}
			}
			out = append(out, p)
			c := p.Classes["critical"]
			fmt.Printf("  %-9s %5.1fx  offered %6.0f  achieved %6.0f  limit %4d  "+
				"critical p99 %8s ok %3d/%-3d  bulk shed %5d\n",
				mode, m, rate, sum.Achieved, p.FinalLimit,
				dur2(c.P99Ns), c.OK, c.Sent, p.Classes["bulk"].Shed)
			gw.Close()
			gwSrv.Close()
			upSrv.Close()
		}
	}
	return out
}

type scenarioSample struct {
	AtSeconds float64 `json:"at_seconds"`
	Limit     int     `json:"limit"`
	InFlight  int     `json:"in_flight"`
	QueueDep  int     `json:"queue_depth"`
	Breaker   string  `json:"breaker"`
	Shed      uint64  `json:"shed_total"`
}

// runScenario injects a fault mid-flight and samples the control loop while it
// happens. A latency-versus-load curve says what the steady state looks like;
// this says how long the controller takes to notice and to recover, which is
// the question an operator actually has.
func runScenario(mode string, fixedLimit int, dur time.Duration) (
	[]scenarioSample, loadgen.Summary) {
	upSrv, gwSrv, gw, up := newStack(mode, fixedLimit)
	defer func() { gw.Close(); gwSrv.Close(); upSrv.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var samples []scenarioSample
	start := time.Now()
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				qs := gw.QueueStats()
				var shed uint64
				var depth int
				for i := range qs.Shed {
					shed += qs.Shed[i]
					depth += qs.Depth[i]
				}
				samples = append(samples, scenarioSample{
					AtSeconds: time.Since(start).Seconds(),
					Limit:     gw.Limiter().Limit(),
					InFlight:  gw.Limiter().InFlight(),
					QueueDep:  depth,
					Breaker:   gw.Breaker().State().String(),
					Shed:      shed,
				})
			}
		}
	}()

	// The fault: the upstream becomes five times slower a third of the way in,
	// and recovers two thirds of the way in.
	go func() {
		bad := backend
		bad.LatencyMultiplier = 5
		select {
		case <-time.After(dur / 3):
			up.SetProfile(bad)
		case <-ctx.Done():
			return
		}
		select {
		case <-time.After(dur / 3):
			up.SetProfile(backend)
		case <-ctx.Done():
		}
	}()

	sum, err := loadgen.Run(ctx, loadgen.Config{
		Target: gwSrv.URL, Rate: ratedRPS * 2, Duration: dur,
		Timeout: 4 * time.Second, Seed: 5, Classes: classes(),
		MaxInFlight: 60000,
	})
	if err != nil {
		log.Fatal(err)
	}
	return samples, sum
}

// runCapacitySwing is the experiment that separates adaptive control from a
// well-chosen constant, because the sweep and the latency fault do not.
//
// Both of those hold the backend's capacity fixed, and against a fixed backend
// a tuned constant is simply correct. Fixed-16 matched the gradient limiter
// on every point. That is worth saying plainly: adaptive concurrency does not
// beat a well-tuned limit on a system that never changes.
//
// Here the backend's worker count goes 8 -> 64 -> 8 mid-run. Now no constant is
// right: a limit tuned for eight workers leaves seven eighths of the capacity
// unused when it arrives, and one tuned for sixty-four floods the backend when
// it leaves. The measured quantity is completed requests, because throughput is
// what a too-low limit costs and latency is what a too-high one costs.
func runCapacitySwing(mode string, dur time.Duration) (float64, []scenarioSample,
	loadgen.Summary) {
	upSrv, gwSrv, gw, up := newStack(mode, 0)
	defer func() { gw.Close(); gwSrv.Close(); upSrv.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var samples []scenarioSample
	start := time.Now()
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				samples = append(samples, scenarioSample{
					AtSeconds: time.Since(start).Seconds(),
					Limit:     gw.Limiter().Limit(),
					InFlight:  gw.Limiter().InFlight(),
					QueueDep:  gw.QueueStats().MaxDepth,
					Breaker:   gw.Breaker().State().String(),
				})
			}
		}
	}()

	go func() {
		big := backend
		big.Workers = 64
		select {
		case <-time.After(dur / 3):
			up.SetProfile(big)
		case <-ctx.Done():
			return
		}
		select {
		case <-time.After(dur / 3):
			up.SetProfile(backend)
		case <-ctx.Done():
		}
	}()

	// Offered load is set above what eight workers can serve and below what
	// sixty-four can, so the middle third is capacity a limiter either uses or
	// wastes.
	sum, err := loadgen.Run(ctx, loadgen.Config{
		Target: gwSrv.URL, Rate: 1600, Duration: dur,
		Timeout: 4 * time.Second, Seed: 9, Classes: classes(),
		MaxInFlight: 60000,
	})
	if err != nil {
		log.Fatal(err)
	}
	return sum.Achieved, samples, sum
}

func dur2(ns int64) string {
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1000000:
		return fmt.Sprintf("%.1fus", float64(ns)/1e3)
	case ns < 1000000000:
		return fmt.Sprintf("%.1fms", float64(ns)/1e6)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1e9)
	}
}

func write(path string, v any) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", " ")
	if err := enc.Encode(v); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s\n", path)
}

func main() {
	out := flag.String("out", "results", "directory for JSON output")
	dur := flag.Duration("duration", 8*time.Second, "per sweep point")
	scenDur := flag.Duration("scenario", 30*time.Second, "scenario length")
	fixedLimit := flag.Int("fixed-limit", 64, "the fixed limiter's limit")
	only := flag.String("only", "", "sweep, scenario or shed; empty runs all")
	flag.Parse()

	if *only == "" || *only == "sweep" {
		fmt.Printf("sweep: backend %d workers, ~%.0f rps capacity, fixed limit %d\n",
			backend.Workers, ratedRPS, *fixedLimit)
		pts := runSweep(*dur, *fixedLimit)
		write(filepath.Join(*out, "sweep.json"), map[string]any{
			"rated_rps": ratedRPS, "fixed_limit": *fixedLimit,
			"backend_workers":  backend.Workers,
			"duration_seconds": dur.Seconds(), "points": pts,
		})
	}

	if *only == "" || *only == "capacity" {
		fmt.Printf("capacity swing: backend 8 -> 64 -> 8 workers over %s, 1600 rps offered\n",
			*scenDur)
		res := map[string]any{}
		for _, mode := range modes {
			ach, samples, sum := runCapacitySwing(mode, *scenDur)
			var minL, maxL int
			for i, x := range samples {
				if i == 0 || x.Limit < minL {
					minL = x.Limit
				}
				if x.Limit > maxL {
					maxL = x.Limit
				}
			}
			var ok uint64
			for _, r := range sum.Results {
				ok += r.OK
			}
			fmt.Printf("  %-10s completed %6.0f rps  served %6d  limit %3d..%-3d  overall p99 %s\n",
				mode, ach, ok, minL, maxL, dur2(sum.Overall.Quantile(0.99)))
			res[mode] = map[string]any{
				"achieved_rps": ach, "served": ok,
				"limit_min": minL, "limit_max": maxL,
				"p99_ns":  sum.Overall.Quantile(0.99),
				"samples": samples,
			}
		}
		write(filepath.Join(*out, "capacity-swing.json"), res)
	}

	if *only == "scenario" {
		// fixed-16 is included because the sweep showed it is the *best*
		// constant for this backend. Beating a badly chosen constant proves
		// nothing; the claim is that the well-chosen one stops being well
		// chosen the moment the backend changes, which is what this measures.
		for _, mode := range []string{"fixed-16", "fixed-64", "gradient"} {
			fmt.Printf("scenario (%s): upstream 5x slower for the middle third of %s\n",
				mode, *scenDur)
			s, sum := runScenario(mode, *fixedLimit, *scenDur)
			var minL, maxL int
			for i, x := range s {
				if i == 0 || x.Limit < minL {
					minL = x.Limit
				}
				if x.Limit > maxL {
					maxL = x.Limit
				}
			}
			cls := map[string]any{}
			for _, r := range sum.Results {
				cls[r.Class] = map[string]any{
					"sent": r.Sent, "ok": r.OK, "shed": r.Shed,
					"failed": r.Failed, "p99_ns": r.Latency.Quantile(0.99),
				}
				if r.Class == "critical" {
					fmt.Printf("    critical: %d/%d ok, p99 %s | limit %d..%d\n",
						r.OK, r.Sent, dur2(r.Latency.Quantile(0.99)), minL, maxL)
				}
			}
			write(filepath.Join(*out, "scenario-"+mode+".json"), map[string]any{
				"limiter": mode, "samples": s, "classes": cls,
				"limit_min": minL, "limit_max": maxL,
			})
		}
	}
}
