// Command ballast-upstream is the programmable backend the experiments run
// against: a fixed number of workers, a bimodal service-time distribution, and
// an admin endpoint that lets a scenario make it misbehave at a known second.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Aditya2k5here/ballast/internal/upstream"
)

func main() {
	addr := flag.String("addr", ":9101", "serve address")
	admin := flag.String("admin", ":9102", "admin address")
	workers := flag.Int("workers", 32, "concurrency the backend can actually serve")
	fast := flag.Duration("fast", 5*time.Millisecond, "fast-mode service time")
	slow := flag.Duration("slow", 60*time.Millisecond, "slow-mode service time")
	frac := flag.Float64("slow-fraction", 0.05, "how often the slow mode is drawn")
	seed := flag.Int64("seed", 1, "rng seed")
	flag.Parse()

	p := upstream.DefaultProfile()
	p.Workers, p.Fast, p.Slow, p.SlowFraction = *workers, *fast, *slow, *frac
	srv := upstream.New(p, *seed)

	go func() {
		log.Printf("upstream admin on %s", *admin)
		log.Fatal(http.ListenAndServe(*admin, srv.AdminHandler()))
	}()
	log.Printf("upstream on %s: %d workers, %s/%s bimodal",
		*addr, p.Workers, p.Fast, p.Slow)
	log.Fatal(http.ListenAndServe(*addr, srv))
}
