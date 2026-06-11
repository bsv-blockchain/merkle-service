package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// rec builds a single-partition record at the given offset on topic "test".
func rec(offset int64) *kgo.Record {
	return &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    offset,
		Value:     []byte("v"),
	}
}

// committedOffsets extracts the offsets of the records processBatch returned
// as committable, in order.
func committedOffsets(recs []*kgo.Record) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Offset
	}
	return out
}

func runBatch(handler MessageHandler, recs ...*kgo.Record) (committable []*kgo.Record, failed *kgo.Record) {
	return processBatch(context.Background(), recs, handler, nil, discardLogger(), "test-group")
}

// TestProcessBatch_AllSuccess verifies that when the handler returns nil for
// every record, every record is returned as committable and there is no
// rewind point.
func TestProcessBatch_AllSuccess(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error { return nil }

	committable, failed := runBatch(handler, rec(10), rec(11), rec(12))
	got := committedOffsets(committable)
	want := []int64{10, 11, 12}
	if len(got) != len(want) {
		t.Fatalf("committable offsets: got %v, want %v", got, want)
	}
	for i, o := range want {
		if got[i] != o {
			t.Errorf("committable[%d] = %d, want %d", i, got[i], o)
		}
	}
	if failed != nil {
		t.Errorf("failed = offset %d, want nil", failed.Offset)
	}
}

// TestProcessBatch_StopsOnHandlerError is the regression test for F-030. On
// the first handler error, processing stops so a later success cannot advance
// the committed offset past the failed one, and the failed record is reported
// as the partition's rewind point. The rewind is what makes the stall real:
// kgo's fetch position advances independently of commits, so without it the
// failed record would never be re-polled in this session.
func TestProcessBatch_StopsOnHandlerError(t *testing.T) {
	wantErr := errors.New("boom")
	var calls int
	handler := func(_ context.Context, _ *Message) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	}

	committable, failed := runBatch(handler, rec(10), rec(11), rec(12), rec(13))
	got := committedOffsets(committable)
	want := []int64{10}
	if len(got) != len(want) {
		t.Fatalf("committable offsets: got %v, want %v (failed offset 11 must NOT be committable, and 12/13 must NOT have been processed)", got, want)
	}
	if got[0] != 10 {
		t.Errorf("committable[0] = %d, want 10", got[0])
	}
	if failed == nil || failed.Offset != 11 {
		t.Fatalf("failed rewind point = %v, want offset 11", failed)
	}
	// Handler must NOT have been invoked for offsets 12 or 13: bailing out
	// preserves the guarantee that the failed offset and everything after it in
	// the partition are redelivered.
	if calls != 2 {
		t.Errorf("handler invoked %d times, want 2 (one success + one failure)", calls)
	}
}

// TestProcessBatch_FirstMessageError covers the corner case where the very
// first record fails: nothing is committable and the rewind point is the
// first record.
func TestProcessBatch_FirstMessageError(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error { return errors.New("first") }

	committable, failed := runBatch(handler, rec(100), rec(101))
	if len(committable) != 0 {
		t.Errorf("expected no committable records, got offsets %v", committedOffsets(committable))
	}
	if failed == nil || failed.Offset != 100 {
		t.Errorf("failed rewind point = %v, want offset 100", failed)
	}
}

// TestConsumerOpts_AcceptedByClient is a smoke test that the option set built by
// consumerOpts is mutually valid (e.g. SessionTimeout >= 3x HeartbeatInterval,
// a valid balancer, offset-reset and auto-commit settings) — franz validates
// these when the client is constructed. The behavioral invariants F-031
// (offset reset to oldest) and F-053 (recovery/error surfacing) are verified
// against in-memory kfake clusters in redelivery_test.go and
// partition_concurrency_test.go, because franz options are opaque and cannot
// be introspected the way sarama's *Config struct could.
func TestConsumerOpts_AcceptedByClient(t *testing.T) {
	client, err := kgo.NewClient(consumerOpts([]string{"localhost:9092"}, "test-group", []string{"test"})...)
	if err != nil {
		t.Fatalf("consumerOpts produced an invalid franz option set: %v", err)
	}
	client.Close()
}

// TestClampBatchMaxBytes guards teranode #660: values at or below the 1 MiB
// broker default are floored to the default (never used as a hard cap that
// would reject normal records), while an explicit larger value is honored.
func TestClampBatchMaxBytes(t *testing.T) {
	cases := []struct {
		name      string
		requested int
		want      int32
	}{
		{"zero floors to default", 0, defaultBatchMaxBytes},
		{"small flush-style value floors to default", 1024, defaultBatchMaxBytes},
		{"exactly default", int(defaultBatchMaxBytes), defaultBatchMaxBytes},
		{"10MiB honored (merkle cap-raise)", 10 * 1024 * 1024, 10 * 1024 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampBatchMaxBytes(tc.requested); got != tc.want {
				t.Errorf("clampBatchMaxBytes(%d) = %d, want %d", tc.requested, got, tc.want)
			}
		})
	}
}
