# ballast

An API gateway that decides, under overload, **which requests to fail**, and an
experiment measuring whether adaptive concurrency control is actually worth
having.

The honest headline first, because it is the interesting part:

> Adaptive concurrency did **not** beat a well-tuned fixed limit. It matched it.
> What it did was turn a *badly* tuned limit from a catastrophe into a
> non-event: with the limit set too high, 3 out of 735 critical requests
> survived 12× overload. With adaptive control, 735 out of 735 did.
>
> Adaptive concurrency is insurance, not an optimisation. The results section
> shows the runs, including the ones where it wins nothing.

## What it does

Four stages sit between a client and an upstream, each answering a different
question:

```
client                    B A L L A S T                          upstream
  |                                                                  |
  |  request + X-Priority + X-Deadline-Ms                            |
  +--> [1. admission] --> [2. queue] --> [3. limiter] --> [4. breaker] -->
  |     deadline           P0 P1 P2      gradient          + retry
  |     already gone?      LIFO tail     on RTT            budget
  |         |                  |              |                |
  |     drop, no work      shed lowest    Little's Law     trip / half-open
  |         |                  |              |                |
  +<--------+------------------+--------------+----------------+
     429 + Retry-After                 /metrics (Prometheus), /snapshot
```

- **Adaptive concurrency**, not a fixed pool. A gradient controller compares
  recent round-trip latency against the shortest it has seen and scales the
  in-flight limit by the ratio, the concurrency at the knee of the throughput
  curve, which is Little's Law read backwards. Upward movement requires the
  limit to actually be *binding*: inflating a number nothing is reaching only
  gives you a greater height to fall from.
- **Deadline propagation and dead-on-arrival drops.** Every request carries a
  deadline; one that has already expired is dropped without an upstream call.
  Work done for a client that has gone is not merely wasted, it is load with
  negative value.
- **Priority shedding with a LIFO tail.** Tiers decide who gives way. Past a
  depth threshold each tier serves *newest* first. Counter-intuitive, but the
  oldest waiter is closest to timing out, so FIFO under overload spends capacity
  on answers nobody is waiting for. Below the threshold it returns to FIFO.
- **Retry budgets**, not retry counts. Retries draw from a shared allowance
  (10% of originals over a sliding window). As failures rise, originals stay
  flat, so the allowance stays flat. Retry load is bounded exactly when it
  would otherwise compound.

No third-party dependencies. `go build ./...` and nothing else.

## Results

Everything below is from `make experiment`, on one machine, against a
programmable backend: 8 workers, bimodal service time (25 ms / 120 ms, 5% slow),
≈250 rps of real capacity. Traffic is 5% critical, 35% standard, 60% bulk, with
deadlines of 400 ms / 1 s / 3 s.

### 1. Latency and success against offered load

`critical p99` is measured from each request's **intended send time**, not from
when it was actually sent. See "coordinated omission" below.

| limiter | 1× | 2× | 4× | 8× | 12× | critical served at 12× |
|---|---:|---:|---:|---:|---:|---:|
| `fixed-16` | 158 ms | 165 ms | 166 ms | 165 ms | **180 ms** | 735 / 735 |
| `fixed-64` | 122 ms | 311 ms | 374 ms | 368 ms | **385 ms** | 731 / 735 |
| `fixed-256` | 120 ms | 402 ms | 402 ms | 406 ms | **412 ms** | **3 / 735** |
| `gradient` | 164 ms | 191 ms | 179 ms | 186 ms | **207 ms** | 735 / 735 |

`fixed-256` is the failure this project is about. The limit is high enough that
it never limits anything, so the backend's own queue absorbs the flood, so every
request blows past its deadline. 99.6% of the most important traffic is lost,
not shed, *lost*, after the work was already done for it.

`fixed-16` is the uncomfortable row. It happens to be near-optimal for this
backend, and it matches the adaptive controller everywhere. That is a real
result and it is left in.

### 2. When the upstream degrades

The backend becomes 5× slower for the middle third of a 24-second run, at 2×
offered load:

| limiter | critical served | limit range |
|---|---:|---:|
| `fixed-16` | 565 / 577 | 16 → 16 |
| `fixed-64` | **391 / 577** | 64 → 64 |
| `gradient` | 559 / 577 | 13 → 31 |

Again: the adaptive controller matches the good constant and rescues the bad
one. It tracked the degradation down to 13 and back up to 31 without being told
anything had happened.

### 3. When the *capacity* changes, the case no constant survives

The backend goes 8 → 64 → 8 workers mid-run, at 1600 rps offered. Now a limit
tuned for eight workers wastes seven eighths of the capacity when it arrives,
and one tuned for sixty-four floods the backend when it leaves.

| limiter | requests served | limit range |
|---|---:|---:|
| `fixed-16` | 8,075 | 16 → 16 |
| `fixed-64` | 18,130 | 64 → 64 |
| `fixed-256` | 18,495 | 256 → 256 |
| `gradient` | 15,459 | **17 → 106** |

The adaptive controller found the extra capacity. 17 up to 106, and served
91% more than the constant that was correct a moment earlier. It still trails
the high constants on raw throughput here, because this backend has a very deep
internal queue and throughput is all this row measures. **That gap is a real
limitation, not a rounding error**: a controller that reacts on a 500 ms window
cannot exploit a step change as fast as a limit that was already too big.

Fixing the probe rate was worth measuring: before adding multiplicative growth
when the limit is binding, the controller crawled 17 → 41 and served 10,861.
After, 17 → 106 and 15,459, a 43% improvement from four lines.

## Coordinated omission

The load generator is **open-loop**: requests are issued on a fixed schedule
whether or not earlier ones have returned, and latency is measured from the
moment each was *due*.

A closed-loop generator. N workers looping send-wait-send. Slows down when the
server does. It stops offering the load you configured and starts offering
whatever the server will accept, and the requests that would have been slowest
are the ones never sent. The result is a load test that reports a healthy p99
for a system visibly falling over. Gil Tene named it; most published latency
numbers still have it.

If the generator itself cannot keep up, that shows up as latency here rather
than disappearing from the histogram, and the count of requests it failed to
issue at all is reported separately. Both bias *against* the gateway.

## Running it

```console
$ make test                    # unit + integration, with -race
$ make experiment              # the sweep, the fault, the capacity swing
$ make demo                    # upstream + gateway + load, three terminals in one

$ go run ./cmd/ballast-upstream -workers 8 -fast 25ms -slow 120ms
$ go run ./cmd/ballast          -upstream http://127.0.0.1:9101 -limiter gradient
$ go run ./cmd/ballast-load     -rate 2000 -duration 20s
$ curl -s localhost:8081/snapshot | jq
$ curl -s localhost:8081/metrics
```

Make the backend misbehave without restarting anything:

```console
$ curl -XPOST localhost:9102/profile -d '{"workers":8,"fast":"25ms","slow":"120ms","latency_multiplier":5}'
```

## Layout

| path | |
|---|---|
| `internal/limiter` | gradient controller and the fixed baseline it is measured against |
| `internal/pqueue` | priority queue, LIFO tail, deadline expiry |
| `internal/breaker` | circuit breaker, one probe at a time in half-open |
| `internal/budget` | sliding-window retry budget |
| `internal/proxy` | the four-stage data plane |
| `internal/upstream` | programmable backend: real queueing, bimodal latency |
| `internal/loadgen` | open-loop generator |
| `internal/hist` | log-linear histogram, ~1% relative error, allocation-free |
| `internal/metrics` | Prometheus exposition, hand-written, no dependencies |
| `cmd/ballast-bench` | the experiment |
| `docs/RUNBOOK.md` | what to look at when it misbehaves |
| `docs/adr/` | the decisions and what they cost |

## Limitations

- **One route, one upstream.** Routing is not the subject; the control loop is.
- **Responses are buffered, not streamed**, so a failed attempt can be replaced
  by a retry without the client having already seen the error. A streaming
  gateway has to give up retries instead, the trade should be made
  deliberately, and here it is made in favour of retries.
- **The controller's window is 500 ms.** It cannot react faster than that, which
  section 3 shows costing real throughput on a step change in capacity.
- **The experiment runs the generator in the same process as the gateway.** On a
  two-core machine they compete, and the generator's own scheduling delay is
  recorded as latency. Both effects understate the gateway.
- **No hot config reload, no TLS, no k8s manifests yet.** Next, in that order.

## Licence

MIT.
