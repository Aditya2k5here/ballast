# Runbook

What to look at, in what order, when ballast is behaving badly. Every symptom
below maps to a metric on `:8081/metrics` or a field in `:8081/snapshot`.

## First three commands

```console
curl -s localhost:8081/snapshot | jq          # everything, human-readable
curl -s localhost:8081/metrics | grep ballast_concurrency
curl -s localhost:9102/stats | jq             # the upstream's own view, if simulated
```

## Symptom: clients see 429, throughput is fine

`ballast_shed_total` rising, `ballast_concurrency_limit` stable and small.

This is the gateway working. Offered load exceeds what the upstream can serve
and the lowest-priority tier is giving way. Check `ballast_shed_total` by
`priority` label: if `critical` is non-zero, critical traffic alone exceeds
capacity and no shedding policy can fix that — the upstream needs to be bigger.

## Symptom: p99 climbing, limit falling

`ballast_concurrency_limit` dropping, `ballast_limiter_rtt_floor_seconds` rising.

The upstream got slower and the controller is following it down. This is
correct. Look at the upstream, not the gateway. If the limit has fallen to
`Min` (default 4) the upstream is very degraded or entirely stalled.

## Symptom: limit pinned at Max, latency terrible

The controller believes there is no queueing. Almost always the RTT floor is
stale — it was measured during a quiet period and everything since looks
proportionally fine. `FloorDecay` (default 10 s) should be shorter than the
timescale on which the upstream's baseline latency changes.

## Symptom: limit oscillating

`ballast_concurrency_limit` sawtoothing every window. Lower `Smoothing` (default
0.2) or raise `RTTTolerance` (default 1.5) so ordinary jitter is not read as
congestion. Oscillation is usually a bimodal upstream: the window minimum RTT
alternates between the two modes.

## Symptom: 503 with `X-Ballast: upstream circuit open`

`ballast_breaker_state` is 1. The upstream returned `FailureThreshold` (20)
consecutive 5xx. It will admit one probe every `OpenFor` (5 s) and close after
`SuccessThreshold` (3) consecutive successes. Nothing to do at the gateway.

## Symptom: 504 with `deadline passed while queued`

`ballast_expired_in_queue_total` rising. Clients are sending deadlines shorter
than the queue takes to clear. Either they are too impatient or the queue is too
deep — check `ballast_queue_depth_max`. A queue deeper than
`limit × (deadline ÷ upstream_p50)` can never be cleared in time and should be
made smaller, so requests are refused immediately rather than after waiting.

## Symptom: retries not happening during an outage

`ballast_retries_denied_total` rising. Working as designed: the budget is spent.
Raising `RetryRatio` above ~0.2 is how a partial outage becomes a total one.

## Symptom: gateway slow with an idle upstream

Check `ballast_queue_wait_seconds` against `ballast_upstream_duration_seconds`.
If the wait dominates while the upstream is fast, the limit is too low — look at
whether the controller ever sees its limit as *binding*, since it will not grow
a limit nothing is reaching.

## Shutdown

`SIGTERM` stops accepting, drains the queue (every waiter gets an immediate 503
rather than a timeout), and finishes in-flight requests within `-grace`
(default 15 s). Zero client-visible errors from in-flight work is the
expectation; anything else is a bug.
