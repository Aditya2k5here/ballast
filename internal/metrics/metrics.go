// Package metrics renders the gateway's state in Prometheus text format.
//
// Written by hand rather than with a client library, for one reason: this
// project has no third-party dependencies, so it builds anywhere with a Go
// toolchain and nothing else, including on a machine with no network. The
// exposition format is a dozen lines of text; a dependency that pulls in a
// registry, a collector interface and a gRPC transport to produce it is not a
// trade worth making here.
//
// What is exported is chosen to answer the questions an operator actually has
// at three in the morning: how much is being refused, how long are requests
// waiting before they run, what does the limiter currently believe, and is the
// breaker open. Request counts without those are decoration.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/Aditya2k5here/ballast/internal/limiter"
	"github.com/Aditya2k5here/ballast/internal/pqueue"
	"github.com/Aditya2k5here/ballast/internal/proxy"
)

type Registry struct {
	routes map[string]*proxy.Gateway
}

func New() *Registry { return &Registry{routes: map[string]*proxy.Gateway{}} }

func (r *Registry) Add(name string, g *proxy.Gateway) { r.routes[name] = g }

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(r.Render()))
	})
}

type metric struct {
	name, help, kind string
	samples          []sample
}

type sample struct {
	labels map[string]string
	value  float64
}

func (r *Registry) Render() string {
	var ms []*metric
	add := func(name, help, kind string) *metric {
		m := &metric{name: "ballast_" + name, help: help, kind: kind}
		ms = append(ms, m)
		return m
	}

	received := add("requests_total", "Requests accepted by the gateway.", "counter")
	ok := add("responses_ok_total", "Requests answered by an upstream without error.", "counter")
	shed := add("shed_total", "Requests refused because capacity was exhausted.", "counter")
	doa := add("dead_on_arrival_total", "Requests whose deadline had already passed when they arrived.", "counter")
	expired := add("expired_in_queue_total", "Requests whose deadline passed while queued.", "counter")
	gone := add("client_gone_total", "Requests whose client disconnected while queued.", "counter")
	retried := add("retries_total", "Retry attempts the budget permitted.", "counter")
	denied := add("retries_denied_total", "Retries refused because the budget was spent.", "counter")
	brkOpen := add("breaker_rejected_total", "Requests refused by an open circuit breaker.", "counter")
	up5xx := add("upstream_errors_total", "5xx responses received from an upstream.", "counter")

	limit := add("concurrency_limit", "Current in-flight limit the limiter allows.", "gauge")
	inflight := add("concurrency_in_flight", "Requests currently forwarded to an upstream.", "gauge")
	depth := add("queue_depth", "Requests currently waiting for capacity.", "gauge")
	maxDepth := add("queue_depth_max", "Deepest the queue has been since start.", "gauge")
	brkState := add("breaker_state", "0 closed, 1 open, 2 half-open.", "gauge")
	floor := add("limiter_rtt_floor_seconds", "Limiter's estimate of unqueued upstream latency.", "gauge")

	lat := add("request_duration_seconds", "End-to-end latency, including time queued.", "summary")
	qwait := add("queue_wait_seconds", "Time spent waiting for capacity.", "summary")
	upLat := add("upstream_duration_seconds", "Time spent in the upstream call.", "summary")
	shedLat := add("shed_duration_seconds", "How long a refusal takes; this should stay tiny.", "summary")

	names := make([]string, 0, len(r.routes))
	for name := range r.routes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		g := r.routes[name]
		m := g.Metrics()
		l := map[string]string{"route": name}

		received.push(l, float64(m.Received.Load()))
		ok.push(l, float64(m.OK.Load()))
		doa.push(l, float64(m.DeadOnArrival.Load()))
		expired.push(l, float64(m.Expired.Load()))
		gone.push(l, float64(m.ClientGone.Load()))
		retried.push(l, float64(m.Retried.Load()))
		denied.push(l, float64(m.RetryDenied.Load()))
		brkOpen.push(l, float64(m.BreakerOpen.Load()))
		up5xx.push(l, float64(m.Upstream5xx.Load()))

		lim := g.Limiter()
		limit.push(l, float64(lim.Limit()))
		inflight.push(l, float64(lim.InFlight()))
		if grad, isGrad := lim.(*limiter.Gradient); isGrad {
			floor.push(l, grad.FloorRTT().Seconds())
		}

		qs := g.QueueStats()
		total := 0
		for tier := 0; tier < len(qs.Depth); tier++ {
			tl := map[string]string{"route": name,
				"priority": pqueue.Priority(tier).String()}
			depth.push(tl, float64(qs.Depth[tier]))
			shed.push(tl, float64(qs.Shed[tier]))
			total += qs.Depth[tier]
		}
		maxDepth.push(l, float64(qs.MaxDepth))
		brkState.push(l, float64(g.Breaker().State()))

		pushSummary(lat, l, m.Total.Snapshot())
		pushSummary(qwait, l, m.Queue.Snapshot())
		pushSummary(upLat, l, m.Up.Snapshot())
		pushSummary(shedLat, l, m.ShedNs.Snapshot())
	}

	var b strings.Builder
	for _, m := range ms {
		if len(m.samples) == 0 {
			continue
		}
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", m.name, m.help,
			m.name, m.kind)
		for _, s := range m.samples {
			fmt.Fprintf(&b, "%s%s %g\n", m.name, renderLabels(s.labels), s.value)
		}
	}
	return b.String()
}

func (m *metric) push(labels map[string]string, v float64) {
	cp := make(map[string]string, len(labels))
	for k, val := range labels {
		cp[k] = val
	}
	m.samples = append(m.samples, sample{labels: cp, value: v})
}

func pushSummary(m *metric, labels map[string]string, s interface {
	Quantile(float64) int64
}) {
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		l := map[string]string{}
		for k, v := range labels {
			l[k] = v
		}
		l["quantile"] = fmt.Sprintf("%g", q)
		m.push(l, float64(s.Quantile(q))/1e9)
	}
}

func renderLabels(l map[string]string) string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, l[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
