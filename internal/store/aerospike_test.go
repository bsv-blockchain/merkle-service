package store

import (
	"testing"
	"time"
)

// TestBatchPolicyConcurrentNodes pins the ConcurrentNodes fix: the policy that
// drives every BatchGet/BatchOperate must carry whatever the operator
// configured, with the zero value meaning "all nodes concurrent" (0) rather
// than the Aerospike library default of serial per-node fan-out (1).
//
// The returned *aerospike.BatchPolicy is referenced only by field access, so
// this test deliberately does not import the aerospike client package and is
// agnostic to its major version.
func TestBatchPolicyConcurrentNodes(t *testing.T) {
	cases := []struct {
		name string
		cfg  int
		want int
	}{
		{"default is all-nodes-concurrent", 0, 0},
		{"serial restores library default", 1, 1},
		{"bounded fan-out", 8, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &AerospikeClient{
				batchTimeoutMs:       3000,
				socketTimeoutMs:      1000,
				batchConcurrentNodes: tc.cfg,
			}
			bp := c.BatchPolicy(0, 100)
			if bp.ConcurrentNodes != tc.want {
				t.Fatalf("ConcurrentNodes = %d, want %d", bp.ConcurrentNodes, tc.want)
			}
			// Guard against a refactor silently dropping the bounded timeouts /
			// no-retry contract this policy depends on.
			if bp.MaxRetries != 0 {
				t.Errorf("MaxRetries = %d, want 0 (app retries via Kafka)", bp.MaxRetries)
			}
			if bp.TotalTimeout != 3000*time.Millisecond {
				t.Errorf("TotalTimeout = %v, want 3s", bp.TotalTimeout)
			}
		})
	}
}
