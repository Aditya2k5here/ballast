// Package hist records latencies and answers percentile questions about them.
//
// A tail-latency SLO cannot be checked against a mean, and it cannot be checked
// against a sample either: the whole point of a p99 is that it is the part of
// the distribution sampling throws away. So every observation is recorded, in a
// log-linear bucketed histogram that costs one array increment and gives
// percentiles accurate to about 1% across nanoseconds to minutes.
//
// The layout is the one HdrHistogram uses. Values are bucketed by the position
// of their highest set bit, and each such bucket is divided linearly into
// subBuckets slots, so relative precision is constant over the whole range
// rather than concentrated where you happened to guess the interesting values
// would be. Recording is allocation-free and lock-free enough to sit on a
// request path; merging is a loop over the same fixed array.
package hist

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
)

const (
	// 128 sub-buckets gives roughly 1% relative error, which is finer than the
	// run-to-run variation of anything being measured here.
	subBucketBits = 7
	subBuckets    = 1 << subBucketBits
	buckets       = 64 - subBucketBits
	slots         = buckets * subBuckets
)

// Histogram records int64 observations, typically nanoseconds.
//
// The zero value is ready to use. It is safe for concurrent Record and Snapshot
// calls; Snapshot takes a consistent copy rather than reading a moving array,
// because a percentile computed from a half-updated histogram is not wrong in
// an obvious way, which is the worst kind of wrong.
type Histogram struct {
	mu     sync.Mutex
	counts [slots]uint64
	total  uint64
	sum    int64
	min    int64
	max    int64
}

func slotFor(v int64) int {
	if v < subBuckets {
		if v < 0 {
			return 0
		}
		return int(v)
	}
	// The magnitude is the index of the highest set bit; within a magnitude the
	// next subBucketBits bits select the slot.
	mag := 63 - bits.LeadingZeros64(uint64(v))
	shift := mag - subBucketBits
	idx := (mag-subBucketBits+1)*subBuckets + int((v>>uint(shift))&(subBuckets-1))
	if idx >= slots {
		return slots - 1
	}
	return idx
}

// valueAt returns the representative value for a slot: the midpoint of the
// range it covers, so percentiles do not systematically read low.
func valueAt(idx int) int64 {
	if idx < subBuckets {
		return int64(idx)
	}
	mag := idx/subBuckets - 1 + subBucketBits
	shift := uint(mag - subBucketBits)
	base := int64(idx%subBuckets) << shift
	high := int64(1) << uint(mag)
	return high + base + (int64(1)<<shift)/2
}

// Record adds one observation.
func (h *Histogram) Record(v int64) {
	if v < 0 {
		v = 0
	}
	h.mu.Lock()
	if h.total == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.counts[slotFor(v)]++
	h.total++
	h.sum += v
	h.mu.Unlock()
}

// Merge folds another histogram into this one. Load generators keep one
// histogram per worker to avoid contention and merge at the end; merging is
// exact, unlike averaging percentiles, which is meaningless.
func (h *Histogram) Merge(other *Histogram) {
	other.mu.Lock()
	snap := other.counts
	total, sum, mn, mx := other.total, other.sum, other.min, other.max
	other.mu.Unlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	if total == 0 {
		return
	}
	if h.total == 0 || mn < h.min {
		h.min = mn
	}
	if mx > h.max {
		h.max = mx
	}
	for i, c := range snap {
		h.counts[i] += c
	}
	h.total += total
	h.sum += sum
}

// Snapshot is an immutable view, safe to compute percentiles from.
type Snapshot struct {
	counts [slots]uint64
	Total  uint64
	Sum    int64
	Min    int64
	Max    int64
}

func (h *Histogram) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Snapshot{counts: h.counts, Total: h.total, Sum: h.sum,
		Min: h.min, Max: h.max}
}

func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Quantile returns the value at q in [0,1].
func (s Snapshot) Quantile(q float64) int64 {
	if s.Total == 0 {
		return 0
	}
	if q <= 0 {
		return s.Min
	}
	if q >= 1 {
		return s.Max
	}
	want := uint64(q*float64(s.Total) + 0.5)
	if want == 0 {
		want = 1
	}
	var seen uint64
	for i, c := range s.counts {
		seen += c
		if seen >= want {
			v := valueAt(i)
			// The recorded extremes are exact; clamping to them keeps a
			// bucket's midpoint from reporting a latency nothing reached.
			if v < s.Min {
				return s.Min
			}
			if v > s.Max {
				return s.Max
			}
			return v
		}
	}
	return s.Max
}

func (s Snapshot) Mean() float64 {
	if s.Total == 0 {
		return 0
	}
	return float64(s.Sum) / float64(s.Total)
}

// Percentiles renders the tail the way an SLO conversation actually goes.
func (s Snapshot) Percentiles() string {
	if s.Total == 0 {
		return "no samples"
	}
	qs := []float64{0.5, 0.9, 0.99, 0.999}
	names := []string{"p50", "p90", "p99", "p999"}
	var b strings.Builder
	fmt.Fprintf(&b, "n=%d min=%s mean=%s", s.Total,
		Dur(s.Min), Dur(int64(s.Mean())))
	for i, q := range qs {
		fmt.Fprintf(&b, " %s=%s", names[i], Dur(s.Quantile(q)))
	}
	fmt.Fprintf(&b, " max=%s", Dur(s.Max))
	return b.String()
}

// Dur formats nanoseconds at a fixed width so columns of them line up.
func Dur(ns int64) string {
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1000_000:
		return fmt.Sprintf("%.1fus", float64(ns)/1e3)
	case ns < 1000_000_000:
		return fmt.Sprintf("%.1fms", float64(ns)/1e6)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1e9)
	}
}

// Buckets returns the non-empty (value, count) pairs, for exporting a
// histogram rather than only its summary.
func (s Snapshot) Buckets() [][2]int64 {
	var out [][2]int64
	for i, c := range s.counts {
		if c > 0 {
			out = append(out, [2]int64{valueAt(i), int64(c)})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a][0] < out[b][0] })
	return out
}
