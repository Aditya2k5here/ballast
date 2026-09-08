package upstream

import (
	"encoding/json"
	"testing"
	"time"
)

// The admin endpoint is used from a shell, so it has to take what a person
// would type. encoding/json alone does not: it only reads integer nanoseconds
// into a time.Duration, which nobody writes by hand.
func TestProfileAcceptsDurationStringsAndNanoseconds(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"strings", `{"workers":8,"fast":"25ms","slow":"120ms","latency_multiplier":5}`},
		{"nanoseconds", `{"workers":8,"fast":25000000,"slow":120000000,"latency_multiplier":5}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := DefaultProfile()
			if err := json.Unmarshal([]byte(tc.body), &p); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if p.Workers != 8 {
				t.Fatalf("workers %d, want 8", p.Workers)
			}
			if p.Fast != 25*time.Millisecond {
				t.Fatalf("fast %v, want 25ms", p.Fast)
			}
			if p.Slow != 120*time.Millisecond {
				t.Fatalf("slow %v, want 120ms", p.Slow)
			}
			if p.LatencyMultiplier != 5 {
				t.Fatalf("multiplier %v, want 5", p.LatencyMultiplier)
			}
		})
	}
}

// Fields the body leaves out keep the value they already had, so a partial
// update is a partial update rather than a reset to zero.
func TestProfileOmittedDurationsAreLeftAlone(t *testing.T) {
	p := DefaultProfile()
	want := p.Jitter
	if err := json.Unmarshal([]byte(`{"workers":4}`), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.Jitter != want {
		t.Fatalf("jitter %v, want it left at %v", p.Jitter, want)
	}
	if p.Fast != DefaultProfile().Fast {
		t.Fatalf("fast %v, want it left at %v", p.Fast, DefaultProfile().Fast)
	}
}

func TestProfileRejectsNonsenseDurations(t *testing.T) {
	p := DefaultProfile()
	if err := json.Unmarshal([]byte(`{"fast":"twenty-five milliseconds"}`), &p); err == nil {
		t.Fatal("accepted a duration it cannot parse")
	}
}
