# 1. Adaptive concurrency instead of a fixed limit

Status: accepted, with a measured caveat

## Context

Under overload, latency does not degrade gently. Requests queue, clients time
out and retry, and the retries add load to a system already over capacity. The
usual mitigations each make one part worse: a fixed `MaxConns` is a guess that
is wrong in both directions, per-call retries multiply load exactly when
capacity has fallen, and CPU-based autoscaling reacts in tens of seconds to
something that arrives in one.

## Decision

A gradient controller sets the in-flight limit from observed round-trip latency:
compare the recent minimum RTT against a longer-lived floor, scale the limit by
the ratio, and probe upwards when nothing is queueing.

Upward movement is gated on the limit being *binding*. In-flight pressed to
within 90% of it. Downward movement is not gated, because latency can rise for
reasons this limiter is not causing and the limit should still come down.

## Consequences

Measured, in `README.md`:

- Against a *well-tuned* constant, this wins nothing. `fixed-16` matched it on
  every point of the load sweep and beat it slightly under a latency fault.
- Against a *badly tuned* constant it is the difference between 3 and 735
  critical requests surviving 12× overload.
- On a step change in capacity it recovers 91% more throughput than the
  previously-correct constant, but still trails an over-large constant, because
  a 500 ms control window cannot follow a step as fast as a limit that was
  already too big.

So the honest claim is not "adaptive is faster". It is "adaptive removes a
tuning parameter whose wrong values are catastrophic and whose right value
changes when the backend does".
