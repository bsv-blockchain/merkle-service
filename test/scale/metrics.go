//go:build scale

package scale

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

// MetricsReport holds timing and throughput data for a scale test run.
type MetricsReport struct {
	T0             time.Time     // block message injected
	T1             time.Time     // first MINED callback received
	T2             time.Time     // last MINED callback received
	T3             time.Time     // all BLOCK_PROCESSED received
	TotalWallClock time.Duration // total test duration
	ServerStats    []ServerStats

	// Phase timings.
	BlockProcessingTime    time.Duration // T0→T1: time until first callback (proxy for block processing)
	CallbackDeliverySpread time.Duration // T1→T2: spread of MINED callback delivery
	TotalPipelineTime      time.Duration // T0→T3: total end-to-end pipeline

	// Percentile latencies (per-instance, T0 to last callback).
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration
}

// collectMetrics gathers timing data from the callback fleet.
func collectMetrics(fleet *CallbackFleet, t0 time.Time) *MetricsReport {
	report := &MetricsReport{
		T0:          t0,
		ServerStats: make([]ServerStats, fleet.Count()),
	}

	var earliestFirst, latestLast time.Time
	var latestBP time.Time
	var instanceLatencies []time.Duration

	for i := 0; i < fleet.Count(); i++ {
		stats := fleet.GetServer(i).Stats()
		report.ServerStats[i] = stats

		if !stats.FirstCallback.IsZero() {
			if earliestFirst.IsZero() || stats.FirstCallback.Before(earliestFirst) {
				earliestFirst = stats.FirstCallback
			}
		}
		if !stats.LastCallback.IsZero() {
			if stats.LastCallback.After(latestLast) {
				latestLast = stats.LastCallback
			}
			instanceLatencies = append(instanceLatencies, stats.LastCallback.Sub(t0))
		}

		// Check BLOCK_PROCESSED timing.
		bpPayloads := fleet.GetServer(i).BlockProcessedPayloads()
		if len(bpPayloads) > 0 {
			if stats.LastCallback.After(latestBP) {
				latestBP = stats.LastCallback
			}
		}
	}

	report.T1 = earliestFirst
	report.T2 = latestLast
	report.T3 = latestBP
	report.TotalWallClock = time.Since(t0)

	// Compute phase timings.
	if !report.T1.IsZero() {
		report.BlockProcessingTime = report.T1.Sub(report.T0)
	}
	if !report.T1.IsZero() && !report.T2.IsZero() {
		report.CallbackDeliverySpread = report.T2.Sub(report.T1)
	}
	if !report.T3.IsZero() {
		report.TotalPipelineTime = report.T3.Sub(report.T0)
	}

	// Compute percentile latencies.
	if len(instanceLatencies) > 0 {
		sort.Slice(instanceLatencies, func(i, j int) bool { return instanceLatencies[i] < instanceLatencies[j] })
		report.P50Latency = percentile(instanceLatencies, 50)
		report.P95Latency = percentile(instanceLatencies, 95)
		report.P99Latency = percentile(instanceLatencies, 99)
	}

	return report
}

// percentile returns the p-th percentile from a sorted slice of durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// printReport outputs a formatted summary.
func printReport(t *testing.T, report *MetricsReport) {
	t.Helper()

	var totalMined, totalBP, totalTxids int
	var totalBytes int64
	for _, s := range report.ServerStats {
		totalMined += s.MinedCallbacks
		totalBP += s.BlockProcessed
		totalTxids += s.TotalTxids
		totalBytes += s.TotalBytes
	}

	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║               SCALE TEST METRICS REPORT                     ║\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║ Total wall clock:           %-32s ║\n", report.TotalWallClock.Round(time.Millisecond)))

	// Phase breakdown.
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ PHASE BREAKDOWN                                             ║\n")

	if !report.T1.IsZero() {
		blockTime := report.BlockProcessingTime.Round(time.Millisecond)
		sb.WriteString(fmt.Sprintf("║ Block processing (T0→T1):   %-32s ║\n", blockTime))
		if report.TotalPipelineTime > 0 {
			pct := float64(report.BlockProcessingTime) / float64(report.TotalPipelineTime) * 100
			sb.WriteString(fmt.Sprintf("║   %% of total:               %-32s ║\n", fmt.Sprintf("%.1f%%", pct)))
		}
		if totalTxids > 0 && report.BlockProcessingTime > 0 {
			throughput := float64(totalTxids) / report.BlockProcessingTime.Seconds()
			sb.WriteString(fmt.Sprintf("║   Block throughput:         %-32s ║\n", fmt.Sprintf("%.0f txids/sec", throughput)))
		}
	}

	if !report.T1.IsZero() && !report.T2.IsZero() {
		deliveryTime := report.CallbackDeliverySpread.Round(time.Millisecond)
		sb.WriteString(fmt.Sprintf("║ Callback delivery (T1→T2):  %-32s ║\n", deliveryTime))
		if report.TotalPipelineTime > 0 {
			pct := float64(report.CallbackDeliverySpread) / float64(report.TotalPipelineTime) * 100
			sb.WriteString(fmt.Sprintf("║   %% of total:               %-32s ║\n", fmt.Sprintf("%.1f%%", pct)))
		}
		if totalTxids > 0 && report.CallbackDeliverySpread > 0 {
			throughput := float64(totalTxids) / report.CallbackDeliverySpread.Seconds()
			sb.WriteString(fmt.Sprintf("║   Delivery throughput:      %-32s ║\n", fmt.Sprintf("%.0f txids/sec", throughput)))
		}
	}

	if !report.T3.IsZero() {
		sb.WriteString(fmt.Sprintf("║ Total pipeline (T0→T3):     %-32s ║\n", report.TotalPipelineTime.Round(time.Millisecond)))
		if totalTxids > 0 && report.TotalPipelineTime > 0 {
			throughput := float64(totalTxids) / report.TotalPipelineTime.Seconds()
			sb.WriteString(fmt.Sprintf("║   Overall throughput:       %-32s ║\n", fmt.Sprintf("%.0f txids/sec", throughput)))
		}
	}

	// Percentile latencies.
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ PER-INSTANCE LATENCY (T0 → last callback)                   ║\n")
	sb.WriteString(fmt.Sprintf("║ P50:                        %-32s ║\n", report.P50Latency.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("║ P95:                        %-32s ║\n", report.P95Latency.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("║ P99:                        %-32s ║\n", report.P99Latency.Round(time.Millisecond)))

	// Callback counts and data volume.
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ CALLBACK SUMMARY                                            ║\n")
	sb.WriteString(fmt.Sprintf("║ Total MINED callbacks:      %-32d ║\n", totalMined))
	sb.WriteString(fmt.Sprintf("║ Total BLOCK_PROCESSED:      %-32d ║\n", totalBP))
	sb.WriteString(fmt.Sprintf("║ Total txids delivered:      %-32d ║\n", totalTxids))
	sb.WriteString(fmt.Sprintf("║ Total bytes received:       %-32s ║\n", formatBytes(totalBytes)))
	sb.WriteString(fmt.Sprintf("║ Callback servers:           %-32d ║\n", len(report.ServerStats)))

	// Per-server table.
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ Per-server summary (first 10):                              ║\n")
	sb.WriteString("║  Port   | MINED | BP | TxIDs  | Bytes    | Latency         ║\n")

	showCount := 10
	if len(report.ServerStats) < showCount {
		showCount = len(report.ServerStats)
	}
	for i := 0; i < showCount; i++ {
		s := report.ServerStats[i]
		latency := ""
		if !s.LastCallback.IsZero() {
			latency = s.LastCallback.Sub(report.T0).Round(time.Millisecond).String()
		}
		sb.WriteString(fmt.Sprintf("║  %5d  | %5d | %2d | %6d | %-8s | %-15s ║\n",
			s.Port, s.MinedCallbacks, s.BlockProcessed, s.TotalTxids, formatBytes(s.TotalBytes), latency))
	}
	if len(report.ServerStats) > showCount {
		sb.WriteString(fmt.Sprintf("║  ... and %d more servers                                     ║\n", len(report.ServerStats)-showCount))
	}

	sb.WriteString("╚══════════════════════════════════════════════════════════════╝\n")

	t.Log(sb.String())
}

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/1024/1024)
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
