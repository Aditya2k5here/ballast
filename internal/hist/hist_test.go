package hist

import (
	"math"
	"sort"
	"testing"
)

// The histogram trades exactness for a fixed footprint, so the thing to test is
// that the error stays inside the bound it claims, across the whole range.
func TestQuantilesAreAccurateToOnePercent(t *testing.T) {
	var h Histogram
	var raw []int64
	for i := 1; i <= 100000; i++ {
		v := int64(i) * 37
		h.Record(v)
		raw = append(raw, v)
	}
	sort.Slice(raw, func(a, b int) bool { return raw[a] < raw[b] })
	s := h.Snapshot()
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := raw[int(q*float64(len(raw)))-1]
		got := s.Quantile(q)
		if rel := math.Abs(float64(got-want)) / float64(want); rel > 0.01 {
			t.Fatalf("q=%g got %d want %d (%.2f%% off)", q, got, want, rel*100)
		}
	}
}

func TestExtremesAreExact(t *testing.T) {
	var h Histogram
	for _, v := range []int64{5, 999999999, 42} {
		h.Record(v)
	}
	s := h.Snapshot()
	if s.Min != 5 || s.Max != 999999999 {
		t.Fatalf("min %d max %d", s.Min, s.Max)
	}
	if s.Quantile(0) != 5 || s.Quantile(1) != 999999999 {
		t.Fatal("q0 and q1 must be the recorded extremes")
	}
}

func TestMergeIsExact(t *testing.T) {
	var a, b, both Histogram
	for i := int64(1); i <= 1000; i++ {
		a.Record(i)
		both.Record(i)
	}
	for i := int64(5000); i <= 6000; i++ {
		b.Record(i)
		both.Record(i)
	}
	a.Merge(&b)
	sa, sb := a.Snapshot(), both.Snapshot()
	if sa.Total != sb.Total || sa.Sum != sb.Sum || sa.Min != sb.Min || sa.Max != sb.Max {
		t.Fatalf("merged %+v != combined %+v", sa, sb)
	}
	for _, q := range []float64{0.5, 0.99} {
		if sa.Quantile(q) != sb.Quantile(q) {
			t.Fatalf("q=%g merged %d != combined %d", q, sa.Quantile(q), sb.Quantile(q))
		}
	}
}

func TestAnEmptyHistogramAnswersRatherThanPanics(t *testing.T) {
	var h Histogram
	s := h.Snapshot()
	if s.Quantile(0.99) != 0 || s.Mean() != 0 {
		t.Fatal("an empty histogram should report zeroes")
	}
	if s.Percentiles() != "no samples" {
		t.Fatalf("got %q", s.Percentiles())
	}
}

func TestSlotsAreMonotonicAcrossTheWholeRange(t *testing.T) {
	prev := -1
	for shift := uint(0); shift < 62; shift++ {
		v := int64(1) << shift
		got := slotFor(v)
		if got <= prev {
			t.Fatalf("slot for 2^%d is %d, not above %d", shift, got, prev)
		}
		prev = got
	}
}
