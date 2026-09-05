package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aditya2k5here/ballast/internal/breaker"
	"github.com/Aditya2k5here/ballast/internal/limiter"
)

// slowBackend serves at most `workers` requests at a time, each taking `d`.
// Everything else waits, which is what makes overload behave like overload.
func slowBackend(workers int, d time.Duration) (*httptest.Server, *atomic.Int64) {
	sem := make(chan struct{}, workers)
	var served atomic.Int64
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
			case <-r.Context().Done():
				return
			}
			defer func() { <-sem }()
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
			served.Add(1)
			w.WriteHeader(http.StatusOK)
		})), &served
}

func newGateway(t *testing.T, upstream string, tune func(*Options)) *Gateway {
	t.Helper()
	opts := DefaultOptions(upstream)
	opts.Limiter = limiter.NewFixed(2)
	opts.QueueCapacity = 8
	opts.LIFOAbove = 1000
	opts.MaxRetries = 0
	if tune != nil {
		tune(&opts)
	}
	g, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	return g
}

func get(t *testing.T, url, priority string, deadlineMs int) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if priority != "" {
		req.Header.Set("X-Priority", priority)
	}
	if deadlineMs > 0 {
		req.Header.Set("X-Deadline-Ms", fmt.Sprint(deadlineMs))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestAHealthyRequestIsProxied(t *testing.T) {
	up, _ := slowBackend(4, time.Millisecond)
	defer up.Close()
	gw := httptest.NewServer(newGateway(t, up.URL, nil))
	defer gw.Close()

	if code := get(t, gw.URL, "1", 0); code != http.StatusOK {
		t.Fatalf("got %d, want 200", code)
	}
}

// The central claim: when capacity runs out, the traffic that survives is the
// traffic that was declared important, not the traffic that happened to arrive
// first.
func TestUnderOverloadCriticalSurvivesAndBulkIsShed(t *testing.T) {
	up, _ := slowBackend(2, 120*time.Millisecond)
	defer up.Close()
	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(2)
		o.QueueCapacity = 4
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	var wg sync.WaitGroup
	var bulkShed, critOK atomic.Int64

	// Flood with bulk first so it owns every queue slot.
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if get(t, gw.URL, "2", 3000) == http.StatusTooManyRequests {
				bulkShed.Add(1)
			}
		}()
	}
	time.Sleep(30 * time.Millisecond)
	// As many critical requests as the queue can hold at once. More than that
	// and some must be refused however they are prioritised, the queue is a
	// bounded resource, and priority decides who occupies it, not how big it is.
	const critical = 4
	for i := 0; i < critical; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if get(t, gw.URL, "0", 3000) == http.StatusOK {
				critOK.Add(1)
			}
		}()
	}
	wg.Wait()

	if bulkShed.Load() == 0 {
		t.Fatal("nothing was shed under a 40-deep flood of a 4-slot queue")
	}
	if critOK.Load() != critical {
		t.Fatalf("only %d/%d critical requests survived a bulk flood",
			critOK.Load(), critical)
	}
	// The point is not just that critical survived: it is that bulk paid for it.
	qs := g.QueueStats()
	if qs.Shed[0] > 0 {
		t.Fatalf("%d critical requests were shed while bulk was still queued",
			qs.Shed[0])
	}
}

func TestARequestWhoseDeadlineHasPassedIsNeverForwarded(t *testing.T) {
	up, served := slowBackend(1, 300*time.Millisecond)
	defer up.Close()
	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(1)
		o.QueueCapacity = 16
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	var wg sync.WaitGroup
	var timedOut atomic.Int64
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A deadline far shorter than the queue could possibly clear.
			if get(t, gw.URL, "1", 40) == http.StatusGatewayTimeout {
				timedOut.Add(1)
			}
		}()
	}
	wg.Wait()

	if timedOut.Load() == 0 {
		t.Fatal("no request was dropped for a deadline that could not be met")
	}
	if got := g.Metrics().Expired.Load(); got == 0 {
		t.Fatal("expiries were not recorded")
	}
	// The upstream must not have been asked to do work nobody was waiting for.
	if int64(served.Load()) > 4 {
		t.Fatalf("upstream served %d requests whose clients had gone", served.Load())
	}
}

func TestAFailingUpstreamOpensTheBreakerAndThenFailsFast(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
	defer up.Close()

	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(4)
		o.Breaker = breaker.Config{FailureThreshold: 5, SuccessThreshold: 1,
			OpenFor: time.Minute, MinSamples: 0}
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	for i := 0; i < 12; i++ {
		get(t, gw.URL, "1", 2000)
	}
	if g.Breaker().State() != breaker.Open {
		t.Fatalf("breaker %v after 12 consecutive failures", g.Breaker().State())
	}

	before := g.Metrics().Forwarded.Load()
	start := time.Now()
	code := get(t, gw.URL, "1", 2000)
	elapsed := time.Since(start)

	if code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 from an open breaker", code)
	}
	if g.Metrics().Forwarded.Load() != before {
		t.Fatal("an open breaker still forwarded to the upstream")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("failing fast took %s", elapsed)
	}
}

// The budget is what stops a failing upstream from tripling its own load.
func TestRetriesAreBoundedByTheBudgetNotByTheRequestCount(t *testing.T) {
	var hits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
	defer up.Close()

	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(4)
		o.MaxRetries = 1
		o.RetryRatio = 0.1
		o.Breaker = breaker.Config{FailureThreshold: 100000,
			SuccessThreshold: 1, OpenFor: time.Minute, MinSamples: 100000}
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	const n = 60
	for i := 0; i < n; i++ {
		get(t, gw.URL, "1", 2000)
	}

	// Without a budget this would be 2n. With one it is n plus a small
	// allowance, and crucially the allowance does not grow with the failures.
	if hits.Load() >= 2*n {
		t.Fatalf("upstream saw %d attempts for %d requests: retries were unbounded",
			hits.Load(), n)
	}
	if g.Metrics().RetryDenied.Load() == 0 {
		t.Fatal("the budget never refused a retry")
	}
}

func TestCapacityIsReturnedWhenAQueuedClientDisconnects(t *testing.T) {
	up, _ := slowBackend(1, 150*time.Millisecond)
	defer up.Close()
	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(1)
		o.QueueCapacity = 32
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	client := &http.Client{Timeout: 20 * time.Millisecond}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, gw.URL, nil)
			req.Header.Set("X-Deadline-Ms", "5000")
			if resp, err := client.Do(req); err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// Everything that abandoned its slot must have given the capacity back, or
	// the gateway is now permanently smaller than it was.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if g.Limiter().InFlight() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := g.Limiter().InFlight(); got != 0 {
		t.Fatalf("%d concurrency slots leaked after clients disconnected", got)
	}
	if get(t, gw.URL, "1", 5000) != http.StatusOK {
		t.Fatal("the gateway could not serve a request afterwards")
	}
}

func TestClosingReleasesEveryWaiter(t *testing.T) {
	up, _ := slowBackend(1, 400*time.Millisecond)
	defer up.Close()
	g := newGateway(t, up.URL, func(o *Options) {
		o.Limiter = limiter.NewFixed(1)
		o.QueueCapacity = 16
	})
	gw := httptest.NewServer(g)
	defer gw.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(t, gw.URL, "1", 10000)
		}()
	}
	time.Sleep(60 * time.Millisecond)
	g.Close()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("requests were left waiting for capacity that would never come")
	}
}
