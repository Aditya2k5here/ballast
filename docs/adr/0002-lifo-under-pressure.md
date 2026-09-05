# 2. LIFO within a tier, but only under pressure

Status: accepted

## Context

A queue that serves oldest-first is obviously fair. Past the knee of the
throughput curve it is also obviously wrong: the oldest waiter has been waiting
longest, so it is the closest to its client giving up, so serving it first
spends capacity on a response nobody is waiting for any more. Under sustained
overload FIFO can converge on serving *only* requests that have already timed
out.

## Decision

Each tier serves newest-first once total queue depth exceeds `LIFOAbove`
(default 64), and oldest-first below it.

## Consequences

- Under overload the number of *useful* responses rises, because the requests
  served are the ones whose deadlines are furthest away.
- Under normal load nothing changes, so the unfairness is confined to the
  situation that justifies it.
- Some requests are starved: a waiter that arrives just as the queue deepens can
  sit behind every later arrival. Deadline expiry bounds how long that lasts,
a starved request is refused, not held forever, but it is a real cost and the
  threshold exists so it is paid only when it buys something.
- The alternative considered was to sort by remaining deadline, which is
  strictly better and needs a heap per tier rather than a list. It was not worth
  the complexity for a first version; the threshold makes LIFO a close
  approximation, since arrival order and deadline order usually agree.
