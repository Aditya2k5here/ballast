# Ballast: live interview demo (about 3 minutes)

Open `E:\PROJECTS\ballast\demo` and double-click the files in order.
No Go install needed: the .exe files are already built in `bin\`.
Arrange windows [1], [2], [3], [4] so all four are visible (Win + arrow keys).

| step | double-click | what to say |
|---|---|---|
| 1 | `1-start-backend.bat` | "This is a fake backend with 8 workers. It can handle about 250 requests a second." |
| 2 | `2-start-gateway.bat` | "This is Ballast, the gateway I built. Every request goes through it before hitting the backend." |
| 3 | `3-watch-live.bat` | "This shows, every second, how many requests the gateway allows in flight (the limit), how many it served and how many it shed." |
| 4 | `4-send-load.bat` | "Now I send 1,000 requests a second, 4x what the backend can take. Watch: the gateway starts shedding immediately and served holds at about 250 a second, exactly the backend's capacity. The limit moves on its own; nobody configured that number, it finds it from latency." |
| 5 | `5-make-backend-slow.bat` (about 15s into the load; the load runs for 60s) | "I just made the backend 5x slower, like a database having a bad day. Watch the limit drop and served per second fall, so the backend isn't flooded." |
| 6 | `6-fix-backend.bat` (about 10s later) | "Backend is healthy again. The limit climbs back up to use the capacity. No restart, no config change." |
| 7 | wait for window [4] to finish | Point at the summary table: "Bulk traffic was shed almost instantly, in about 2 ms, so clients aren't left hanging. Critical traffic got priority. Latency is measured from when each request *should* have been sent, so slow periods can't hide." |
| 8 | `7-stop-everything.bat` | Done. |

Optional: while it runs, open http://localhost:8081/metrics in a browser to show Prometheus metrics.

## The 30-second pitch

"Ballast is an API gateway in Go, no dependencies, that decides which requests to fail under overload.
It has four stages: drop requests whose deadline has already passed, a priority queue that sheds bulk first and serves newest-first under pressure,
an adaptive concurrency limiter that uses latency and Little's Law instead of a hard-coded limit, and a circuit breaker with a retry budget.
I benchmarked it: a well-tuned fixed limit did just as well, but a badly tuned one lost 99.6% of critical traffic at 12x overload, while adaptive lost none.
So adaptive control is insurance: it removes a setting that's catastrophic when you get it wrong."

## Likely questions

- **Why not just a fixed connection limit?** The right number changes when the backend changes. Too high: the backend queue floods and everything times out. Too low: you waste capacity.
- **Little's Law?** In-flight = throughput x latency. Past the backend's "knee", more concurrency only adds latency, so rising latency is the signal to cut the limit.
- **Why LIFO?** Under overload the oldest request is closest to timing out. Serving newest first means your work goes to clients still waiting. It switches to LIFO only when the queue is deep.
- **Coordinated omission?** A load tester that waits for replies slows down when the server does and under-reports latency. Mine is open-loop: it sends on a fixed schedule and measures from the intended send time.
- **Retry budget?** Retries are capped at 10% of normal traffic, so during an outage retries can't multiply the load.
- **Limitations?** The limiter reacts on a 500 ms window, so it's slower than an oversized limit on a sudden capacity jump. One route and one upstream only. No TLS yet.

## If something goes wrong

- **Don't click inside the black windows.** Clicking selects text and stops the view from scrolling. If it looks frozen, scroll down with the mouse wheel.
- **The breaker shows `open` right after the load finishes.** Known bug: the last requests of a run hit their deadlines mid-call, the gateway gets a 502 back and counts each one as a backend failure, and 20 in a row trip the breaker. It closes again after the next few successful requests. If asked, say: "That's a bug I'd fix. A request that ran out of its own deadline shouldn't count against the backend's health."

- **Windows Firewall popup:** click Allow (private networks is enough).
- **"gateway not reachable" in [3]:** start [1] and [2] first.
- **Port already in use:** run `7-stop-everything.bat`, then start again.
