// Command ballast is the gateway.
//
// One route, one upstream, one limiter -- deliberately, because the experiment
// is about the control loop rather than about routing. Everything the operator
// needs is on the admin port: Prometheus metrics, and a snapshot endpoint that
// prints the same numbers in a form a human can read during an incident.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Aditya2k5here/ballast/internal/limiter"
	"github.com/Aditya2k5here/ballast/internal/metrics"
	"github.com/Aditya2k5here/ballast/internal/proxy"
)

func main() {
	addr := flag.String("addr", ":8080", "serve address")
	adminAddr := flag.String("admin", ":8081", "admin and metrics address")
	up := flag.String("upstream", "http://127.0.0.1:9101", "upstream base URL")
	mode := flag.String("limiter", "gradient", "gradient or fixed")
	fixed := flag.Int("fixed-limit", 64, "limit when -limiter=fixed")
	queueCap := flag.Int("queue", 2048, "how many requests may wait for capacity")
	lifo := flag.Int("lifo-above", 64, "queue depth past which each tier serves newest-first")
	deadline := flag.Duration("deadline", 2*time.Second, "default deadline when a client sends none")
	retries := flag.Int("retries", 1, "retry attempts, subject to the budget")
	grace := flag.Duration("grace", 15*time.Second, "shutdown drain period")
	flag.Parse()

	opts := proxy.DefaultOptions(*up)
	opts.QueueCapacity, opts.LIFOAbove = *queueCap, *lifo
	opts.DefaultDeadline, opts.MaxRetries = *deadline, *retries
	if *mode == "fixed" {
		opts.Limiter = limiter.NewFixed(*fixed)
	} else {
		opts.Limiter = limiter.NewGradient(limiter.DefaultGradient())
	}

	gw, err := proxy.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	reg := metrics.New()
	reg.Add("default", gw)

	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", reg.Handler())
	adminMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	adminMux.HandleFunc("/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		m := gw.Metrics()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"limiter":    gw.Limiter().Name(),
			"limit":      gw.Limiter().Limit(),
			"in_flight":  gw.Limiter().InFlight(),
			"queue":      gw.QueueStats(),
			"budget":     gw.BudgetStats(),
			"breaker":    gw.Breaker().State().String(),
			"received":   m.Received.Load(),
			"ok":         m.OK.Load(),
			"shed":       m.Shed.Load(),
			"expired":    m.Expired.Load(),
			"dead":       m.DeadOnArrival.Load(),
			"retried":    m.Retried.Load(),
			"latency":    m.Total.Snapshot().Percentiles(),
			"queue_wait": m.Queue.Snapshot().Percentiles(),
		})
	})

	adminSrv := &http.Server{Addr: *adminAddr, Handler: adminMux}
	go func() {
		log.Printf("admin on %s (/metrics, /snapshot, /healthz)", *adminAddr)
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Print(err)
		}
	}()

	srv := &http.Server{Addr: *addr, Handler: gw}
	go func() {
		log.Printf("ballast on %s -> %s (%s limiter)", *addr, *up, gw.Limiter().Name())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown: stop accepting, let in-flight work finish, then go.
	// Draining the queue first matters -- anything still waiting for capacity
	// will never get it, and telling it so immediately beats letting it time out.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("draining for up to %s", *grace)
	ctx, cancel := context.WithTimeout(context.Background(), *grace)
	defer cancel()
	gw.Close()
	_ = srv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
	log.Print("stopped")
}
