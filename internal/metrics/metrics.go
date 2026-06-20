// Package metrics provides lightweight, lock-free runtime counters for the
// daemon. Everything is atomic so instrumentation adds no contention on the
// hot path. A single process-wide registry is exposed via Snapshot(), which
// the master's HTTP API renders as JSON/text so operators can see where time
// goes (active transfers, completion latency, race-stat cost, link saturation)
// instead of guessing from client-side symptoms.
package metrics

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Int64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n int64)  { c.v.Add(n) }
func (c *Counter) Value() int64 { return c.v.Load() }

// Gauge is a value that goes up and down (e.g. active transfers).
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Inc()         { g.v.Add(1) }
func (g *Gauge) Dec()         { g.v.Add(-1) }
func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Value() int64 { return g.v.Load() }

// Latency accumulates count, total and max in nanoseconds, all via atomics.
// avg = sum/count. It is intentionally cheap (no histogram) — enough to spot a
// rising completion time, which is what makes clients time out.
type Latency struct {
	count atomic.Int64
	sumNs atomic.Int64
	maxNs atomic.Int64
}

func (l *Latency) Observe(d time.Duration) {
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	l.count.Add(1)
	l.sumNs.Add(ns)
	for {
		cur := l.maxNs.Load()
		if ns <= cur || l.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// Snapshot returns count, average milliseconds and max milliseconds.
func (l *Latency) Snapshot() (count int64, avgMs, maxMs float64) {
	c := l.count.Load()
	if c > 0 {
		avgMs = float64(l.sumNs.Load()) / float64(c) / 1e6
	}
	maxMs = float64(l.maxNs.Load()) / 1e6
	return c, avgMs, maxMs
}

// Process-wide instruments. Grouped by concern; populated where the work
// happens and read by Snapshot().
var (
	// Transfers.
	UploadsActive   Gauge
	DownloadsActive Gauge
	UploadsTotal    Counter
	DownloadsTotal  Counter
	UploadBytes     Counter
	DownloadBytes   Counter
	UploadLatency   Latency // begin-of-transfer -> completion (proxy for time-to-226)
	DownloadLatency Latency

	// Master <-> slave link health (slave side). A non-zero, growing
	// StatusDrops means the shared write stream is saturated and completion
	// messages (which fire the client's 226) are competing with status spam.
	StatusDrops Counter

	// Race-stat computation cost (master side). High counts/latency here under
	// load indicate the per-listing race recompute is a bottleneck.
	RaceComputes    Counter
	RaceComputeTime Latency

	// ZeroSizeRepairs counts files that were on disk but read as size 0 in the
	// VFS and had to be repaired from the race DB during a listing. A non-zero,
	// climbing value during a race means uploaded files are momentarily invisible
	// (zero-size) to clients, which makes racers skip and retry them.
	ZeroSizeRepairs Counter
)

// TransferBegin marks a transfer as started. direction is "upload"/"download".
func TransferBegin(direction string) {
	switch direction {
	case "upload":
		UploadsActive.Inc()
	case "download":
		DownloadsActive.Inc()
	}
}

// TransferEnd records a finished transfer (success or failure).
func TransferEnd(direction string, dur time.Duration, bytes int64) {
	switch direction {
	case "upload":
		UploadsActive.Dec()
		UploadsTotal.Inc()
		if bytes > 0 {
			UploadBytes.Add(bytes)
		}
		UploadLatency.Observe(dur)
	case "download":
		DownloadsActive.Dec()
		DownloadsTotal.Inc()
		if bytes > 0 {
			DownloadBytes.Add(bytes)
		}
		DownloadLatency.Observe(dur)
	}
}

// Snapshot renders the whole registry plus Go runtime stats as a nested map
// suitable for JSON encoding.
func Snapshot() map[string]interface{} {
	upCount, upAvg, upMax := UploadLatency.Snapshot()
	dnCount, dnAvg, dnMax := DownloadLatency.Snapshot()
	rcCount, rcAvg, rcMax := RaceComputeTime.Snapshot()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return map[string]interface{}{
		"transfers": map[string]interface{}{
			"uploads_active":   UploadsActive.Value(),
			"downloads_active": DownloadsActive.Value(),
			"uploads_total":    UploadsTotal.Value(),
			"downloads_total":  DownloadsTotal.Value(),
			"upload_bytes":     UploadBytes.Value(),
			"download_bytes":   DownloadBytes.Value(),
			"upload_ms_avg":    round2(upAvg),
			"upload_ms_max":    round2(upMax),
			"upload_samples":   upCount,
			"download_ms_avg":  round2(dnAvg),
			"download_ms_max":  round2(dnMax),
			"download_samples": dnCount,
		},
		"link": map[string]interface{}{
			"status_drops": StatusDrops.Value(),
		},
		"race_stats": map[string]interface{}{
			"computes":        RaceComputes.Value(),
			"compute_ms_avg":  round2(rcAvg),
			"compute_ms_max":  round2(rcMax),
			"compute_samples": rcCount,
		},
		"vfs": map[string]interface{}{
			"zero_size_repairs": ZeroSizeRepairs.Value(),
		},
		"runtime": map[string]interface{}{
			"goroutines":      runtime.NumGoroutine(),
			"heap_mb":         round2(float64(mem.HeapAlloc) / 1024.0 / 1024.0),
			"sys_mb":          round2(float64(mem.Sys) / 1024.0 / 1024.0),
			"num_gc":          mem.NumGC,
			"gc_pause_ms_avg": gcPauseAvgMs(&mem),
		},
	}
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func gcPauseAvgMs(mem *runtime.MemStats) float64 {
	if mem.NumGC == 0 {
		return 0
	}
	return round2(float64(mem.PauseTotalNs) / float64(mem.NumGC) / 1e6)
}
