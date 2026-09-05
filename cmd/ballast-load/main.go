// Command ballast-load offers a fixed arrival rate regardless of how the target
// responds, and reports latency measured from each request's intended send
// time. See package loadgen for why that distinction is the whole point.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Aditya2k5here/ballast/internal/loadgen"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:8080/", "URL to load")
	rate := flag.Float64("rate", 500, "offered requests per second")
	dur := flag.Duration("duration", 10*time.Second, "how long to offer it")
	timeout := flag.Duration("timeout", 5*time.Second, "client give-up")
	seed := flag.Int64("seed", 1, "rng seed")
	jsonOut := flag.String("json", "", "also write the summary here")
	flag.Parse()

	cfg := loadgen.Config{
		Target: *target, Rate: *rate, Duration: *dur, Timeout: *timeout,
		Seed: *seed,
		Classes: []loadgen.Class{
			{Name: "critical", Priority: "0", Share: 0.2, Deadline: 500 * time.Millisecond},
			{Name: "standard", Priority: "1", Share: 0.5, Deadline: 1 * time.Second},
			{Name: "bulk", Priority: "2", Share: 0.3, Deadline: 3 * time.Second},
		},
	}
	sum, err := loadgen.Run(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(sum.Report())
	if *jsonOut != "" {
		f, err := os.Create(*jsonOut)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", " ")
		_ = enc.Encode(summaryJSON(sum))
	}
}

func summaryJSON(s loadgen.Summary) map[string]any {
	classes := make([]map[string]any, 0, len(s.Results))
	for _, r := range s.Results {
		classes = append(classes, map[string]any{
			"class": r.Class, "sent": r.Sent, "ok": r.OK,
			"shed": r.Shed, "failed": r.Failed, "abandoned": r.Abandoned,
			"p50_ns": r.Latency.Quantile(0.5), "p90_ns": r.Latency.Quantile(0.9),
			"p99_ns": r.Latency.Quantile(0.99), "p999_ns": r.Latency.Quantile(0.999),
			"max_ns": r.Latency.Max,
		})
	}
	return map[string]any{
		"offered_rps": s.OfferedRate, "achieved_rps": s.Achieved,
		"wall_seconds": s.Wall.Seconds(), "classes": classes,
		"overall_p99_ns": s.Overall.Quantile(0.99),
	}
}
