// Package proxy is the data plane: four stages between a client and an
// upstream, each answering a different question.
//
//	admission   is this request still worth doing?
//	queue       whose request is it, and should it wait or give way?
//	limiter     how many may run at once?
//	breaker     is the upstream healthy enough to try?
//
// The order matters. Admission is first because the cheapest request is the one
// never made: a request whose deadline has already passed is dropped before it
// costs a queue slot. The breaker is last because it is per-upstream, and a
// request that never got a concurrency slot should not count as evidence about
// the upstream's health.
//
// Capacity is handed out as tokens. A request that cannot get one immediately
// waits in the queue; whoever releases a token pumps the queue and hands it to
// the next waiter the queue chooses. That indirection is what lets the queue's
// policy. Priority, LIFO under pressure, deadline expiry. Decide *which*
// waiter proceeds, rather than the runtime's arbitrary wakeup order.
package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Aditya2k5here/ballast/internal/breaker"
	"github.com/Aditya2k5here/ballast/internal/budget"
	"github.com/Aditya2k5here/ballast/internal/hist"
	"github.com/Aditya2k5here/ballast/internal/limiter"
	"github.com/Aditya2k5here/ballast/internal/pqueue"
)

// Options configure one route.
type Options struct {
	Upstream        string
	Limiter         limiter.Limiter
	QueueCapacity   int
	LIFOAbove       int
	DefaultDeadline time.Duration
	MaxRetries      int
	Breaker         breaker.Config
	RetryRatio      float64
	UpstreamTimeout time.Duration
}

func DefaultOptions(upstream string) Options {
	return Options{
		Upstream:        upstream,
		Limiter:         limiter.NewGradient(limiter.DefaultGradient()),
		QueueCapacity:   2048,
		LIFOAbove:       64,
		DefaultDeadline: 2 * time.Second,
		MaxRetries:      1,
		Breaker:         breaker.Default(),
		RetryRatio:      0.1,
		UpstreamTimeout: 10 * time.Second,
	}
}

// Metrics is everything the gateway knows about itself.
type Metrics struct {
	Received      atomic.Uint64
	DeadOnArrival atomic.Uint64
	Shed          atomic.Uint64
	Expired       atomic.Uint64
	Forwarded     atomic.Uint64
	Retried       atomic.Uint64
	RetryDenied   atomic.Uint64
	BreakerOpen   atomic.Uint64
	Upstream5xx   atomic.Uint64
	OK            atomic.Uint64
	ClientGone    atomic.Uint64

	Total  hist.Histogram
	Queue  hist.Histogram
	Up     hist.Histogram
	ShedNs hist.Histogram
}

// Gateway is one route's data plane.
type Gateway struct {
	opts    Options
	target  *url.URL
	rp      *httputil.ReverseProxy
	lim     limiter.Limiter
	queue   *pqueue.Queue
	brk     *breaker.Breaker
	bud     *budget.Budget
	pending chan func(time.Duration, bool)

	m       Metrics
	closeMu sync.Mutex
	closed  bool
}

func New(opts Options) (*Gateway, error) {
	u, err := url.Parse(opts.Upstream)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", opts.Upstream, err)
	}
	if opts.Limiter == nil {
		opts.Limiter = limiter.NewGradient(limiter.DefaultGradient())
	}
	g := &Gateway{
		opts:    opts,
		target:  u,
		lim:     opts.Limiter,
		queue:   pqueue.New(opts.QueueCapacity, opts.LIFOAbove),
		brk:     breaker.New(opts.Breaker),
		bud:     budget.New(opts.RetryRatio, 1, 10*time.Second, 10),
		pending: make(chan func(time.Duration, bool), opts.QueueCapacity+64),
	}
	g.rp = &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = u.Scheme
			r.URL.Host = u.Host
			r.Host = u.Host
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			w.WriteHeader(http.StatusBadGateway)
		},
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 10000,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
	return g, nil
}

func (g *Gateway) Metrics() *Metrics         { return &g.m }
func (g *Gateway) QueueStats() pqueue.Stats  { return g.queue.Stats() }
func (g *Gateway) BudgetStats() budget.Stats { return g.bud.Stats() }
func (g *Gateway) Limiter() limiter.Limiter  { return g.lim }
func (g *Gateway) Breaker() *breaker.Breaker { return g.brk }

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	arrived := time.Now()
	g.m.Received.Add(1)

	prio := pqueue.ParsePriority(r.Header.Get("X-Priority"))
	deadline := g.deadlineFor(r, arrived)

	// -- stage 1: admission ------------------------------------------------
	// A request whose deadline has already gone is dropped before it costs
	// anything. This is rare on arrival and common at the head of a deep queue,
	// which is exactly why the check lives in both places.
	if !deadline.IsZero() && time.Now().After(deadline) {
		g.m.DeadOnArrival.Add(1)
		g.refuse(w, http.StatusGatewayTimeout, "deadline already passed", arrived)
		return
	}

	// -- stage 2: capacity, via the queue ----------------------------------
	release, ok := g.fastPath()
	if !ok {
		item := &pqueue.Item{
			Priority: prio, Deadline: deadline,
			Ready: make(chan pqueue.Outcome, 1),
		}
		if !g.queue.Enqueue(item) {
			g.m.Shed.Add(1)
			g.refuseShed(w, prio, arrived)
			return
		}
		queuedAt := time.Now()
		select {
		case outcome := <-item.Ready:
			g.m.Queue.Record(time.Since(queuedAt).Nanoseconds())
			switch outcome {
			case pqueue.Admitted:
				release = <-g.pending
			case pqueue.Expired:
				g.m.Expired.Add(1)
				g.refuse(w, http.StatusGatewayTimeout,
					"deadline passed while queued", arrived)
				return
			case pqueue.Shed:
				g.m.Shed.Add(1)
				g.refuseShed(w, prio, arrived)
				return
			default:
				g.refuse(w, http.StatusServiceUnavailable,
					"gateway shutting down", arrived)
				return
			}
		case <-r.Context().Done():
			// The client hung up while queued. It will still be dequeued later
			// and told Admitted or Expired; the token it is handed is returned
			// by the drain goroutine below, so capacity is not leaked.
			g.m.ClientGone.Add(1)
			go g.drainAbandoned(item)
			return
		}
	}

	// -- stages 3 and 4: forward, with the breaker and the retry budget ----
	defer g.pump()
	g.forward(w, r, release, deadline, arrived)
}

// fastPath takes a token directly when nothing is waiting. Going through the
// queue when the queue is empty would add a channel round trip to every request
// on the happy path, which is most of them.
func (g *Gateway) fastPath() (func(time.Duration, bool), bool) {
	if g.queue.Depth() > 0 {
		return nil, false
	}
	return g.lim.Acquire()
}

// pump hands freed capacity to whoever the queue says should have it.
func (g *Gateway) pump() {
	for {
		release, ok := g.lim.Acquire()
		if !ok {
			return
		}
		// The token is published before the waiter is woken, so the waiter
		// always finds one waiting for it.
		select {
		case g.pending <- release:
		default:
			release(0, true)
			return
		}
		if item := g.queue.Dequeue(); item == nil {
			// Nobody wanted it: take the token back.
			select {
			case r := <-g.pending:
				r(0, true)
			default:
			}
			return
		}
	}
}

func (g *Gateway) drainAbandoned(item *pqueue.Item) {
	if outcome := <-item.Ready; outcome == pqueue.Admitted {
		release := <-g.pending
		release(0, true)
		g.pump()
	}
}

func (g *Gateway) forward(w http.ResponseWriter, r *http.Request,
	release func(time.Duration, bool), deadline time.Time, arrived time.Time) {

	if !g.brk.Allow() {
		release(0, true)
		g.m.BreakerOpen.Add(1)
		g.refuse(w, http.StatusServiceUnavailable, "upstream circuit open", arrived)
		return
	}

	ctx := r.Context()
	var cancel context.CancelFunc
	if !deadline.IsZero() {
		// Deadline propagation: the upstream call inherits the client's
		// deadline, so work stops the moment it can no longer be delivered.
		ctx, cancel = context.WithDeadline(ctx, deadline)
	} else {
		ctx, cancel = context.WithTimeout(ctx, g.opts.UpstreamTimeout)
	}
	defer cancel()

	g.bud.Original()
	attempts := 0
	for {
		attempts++
		start := time.Now()
		rec := &recorder{}
		g.m.Forwarded.Add(1)
		g.rp.ServeHTTP(rec, r.WithContext(ctx))
		rtt := time.Since(start)
		g.m.Up.Record(rtt.Nanoseconds())

		failed := rec.status >= 500
		if failed {
			g.m.Upstream5xx.Add(1)
		}
		g.brk.Report(!failed)

		retryable := failed && attempts <= g.opts.MaxRetries && ctx.Err() == nil
		if !retryable {
			release(rtt, failed)
			if !failed {
				g.m.OK.Add(1)
			}
			rec.flushTo(w)
			g.m.Total.Record(time.Since(arrived).Nanoseconds())
			return
		}

		// The budget is what stops a failing upstream from tripling its own
		// load. When it is exhausted the answer is the error, not another try.
		if !g.bud.Withdraw() {
			g.m.RetryDenied.Add(1)
			release(rtt, true)
			rec.flushTo(w)
			g.m.Total.Record(time.Since(arrived).Nanoseconds())
			return
		}
		g.m.Retried.Add(1)
	}
}

func (g *Gateway) deadlineFor(r *http.Request, arrived time.Time) time.Time {
	if v := r.Header.Get("X-Deadline-Ms"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return arrived.Add(time.Duration(ms) * time.Millisecond)
		}
	}
	if d, ok := r.Context().Deadline(); ok {
		return d
	}
	if g.opts.DefaultDeadline > 0 {
		return arrived.Add(g.opts.DefaultDeadline)
	}
	return time.Time{}
}

func (g *Gateway) refuse(w http.ResponseWriter, code int, why string,
	arrived time.Time) {
	w.Header().Set("X-Ballast", why)
	http.Error(w, why, code)
	g.m.Total.Record(time.Since(arrived).Nanoseconds())
}

// refuseShed is the shed path, and it is deliberately the cheapest path in the
// gateway. A refusal that takes as long as a success has not saved anything.
func (g *Gateway) refuseShed(w http.ResponseWriter, p pqueue.Priority,
	arrived time.Time) {
	took := time.Since(arrived).Nanoseconds()
	w.Header().Set("Retry-After", "1")
	w.Header().Set("X-Ballast", "shed:"+p.String())
	http.Error(w, "overloaded", http.StatusTooManyRequests)
	g.m.ShedNs.Record(took)
	g.m.Total.Record(took)
}

// Close drains the queue so nothing is left waiting for capacity that will
// never arrive.
func (g *Gateway) Close() {
	g.closeMu.Lock()
	defer g.closeMu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	g.queue.Close()
}

// recorder buffers one attempt's response instead of streaming it.
//
// A retry is only honest if the failed attempt never reached the client. Once
// a byte of a 502 has been written the retry is a lie, the client has already
// seen the error, so the whole response is held until the attempt is known to
// be the final one. That costs a copy of the body, which for an API gateway is
// a few kilobytes and worth it; a streaming gateway would have to give up
// retries instead, and the trade should be made deliberately rather than by
// forgetting to buffer.
type recorder struct {
	status int
	header http.Header
	body   []byte
}

func (r *recorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body = append(r.body, b...)
	return len(b), nil
}

func (r *recorder) flushTo(w http.ResponseWriter) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	dst := w.Header()
	for k, vs := range r.header {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
	w.WriteHeader(r.status)
	if len(r.body) > 0 {
		_, _ = w.Write(r.body)
	}
}
