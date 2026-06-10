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

// fetchesOf wraps records (assumed same topic/partition) into a kgo.Fetches so
// processRecords can be exercised without a broker.
func fetchesOf(recs ...*kgo.Record) kgo.Fetches {
	if len(recs) == 0 {
		return kgo.Fetches{}
	}
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: recs[0].Topic,
			Partitions: []kgo.FetchPartition{{
				Partition: recs[0].Partition,
				Records:   recs,
			}},
		}},
	}}
}

// committedOffsets extracts the offsets of the records processRecords returned
// as committable, in order.
func committedOffsets(recs []*kgo.Record) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Offset
	}
	return out
}

func run(handler MessageHandler, fetches kgo.Fetches) []*kgo.Record {
	return processRecords(context.Background(), fetches, handler, nil, discardLogger(), "test-group")
}

// TestProcessRecords_AllSuccess verifies that when the handler returns nil for
// every record, every record is returned as committable.
func TestProcessRecords_AllSuccess(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error { return nil }

	got := committedOffsets(run(handler, fetchesOf(rec(10), rec(11), rec(12))))
	want := []int64{10, 11, 12}
	if len(got) != len(want) {
		t.Fatalf("committable offsets: got %v, want %v", got, want)
	}
	for i, o := range want {
		if got[i] != o {
			t.Errorf("committable[%d] = %d, want %d", i, got[i], o)
		}
	}
}

// TestProcessRecords_StopsOnHandlerError is the regression test for F-030. On
// the first handler error in a partition, processing of that partition must
// stop so a later success cannot advance the committed offset past the failed
// one. franz commits from the last committed offset, so leaving the failed
// record (and everything after it in the partition) uncommitted means they are
// redelivered on the next poll/rebalance — preserving at-least-once.
func TestProcessRecords_StopsOnHandlerError(t *testing.T) {
	wantErr := errors.New("boom")
	var calls int
	handler := func(_ context.Context, _ *Message) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	}

	got := committedOffsets(run(handler, fetchesOf(rec(10), rec(11), rec(12), rec(13))))
	want := []int64{10}
	if len(got) != len(want) {
		t.Fatalf("committable offsets: got %v, want %v (failed offset 11 must NOT be committable, and 12/13 must NOT have been processed)", got, want)
	}
	if got[0] != 10 {
		t.Errorf("committable[0] = %d, want 10", got[0])
	}
	// Handler must NOT have been invoked for offsets 12 or 13: bailing out
	// preserves the guarantee that the failed offset and everything after it in
	// the partition are redelivered.
	if calls != 2 {
		t.Errorf("handler invoked %d times, want 2 (one success + one failure)", calls)
	}
}

// TestProcessRecords_FirstMessageError covers the corner case where the very
// first record fails: nothing is committable.
func TestProcessRecords_FirstMessageError(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error { return errors.New("first") }

	if got := run(handler, fetchesOf(rec(100), rec(101))); len(got) != 0 {
		t.Errorf("expected no committable records, got offsets %v", committedOffsets(got))
	}
}

// TestProcessRecords_PartitionsAreIndependent verifies F-030 is enforced
// per-partition: a handler error on one partition must not stop committing
// successful records on a different partition.
func TestProcessRecords_PartitionsAreIndependent(t *testing.T) {
	// partition 0: offsets 1 (ok), 2 (fail), 3 (must be skipped)
	// partition 1: offsets 5 (ok), 6 (ok)
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "test",
			Partitions: []kgo.FetchPartition{
				{Partition: 0, Records: []*kgo.Record{
					{Topic: "test", Partition: 0, Offset: 1, Value: []byte("v")},
					{Topic: "test", Partition: 0, Offset: 2, Value: []byte("FAIL")},
					{Topic: "test", Partition: 0, Offset: 3, Value: []byte("v")},
				}},
				{Partition: 1, Records: []*kgo.Record{
					{Topic: "test", Partition: 1, Offset: 5, Value: []byte("v")},
					{Topic: "test", Partition: 1, Offset: 6, Value: []byte("v")},
				}},
			},
		}},
	}}

	handler := func(_ context.Context, m *Message) error {
		if string(m.Value) == "FAIL" {
			return errors.New("boom")
		}
		return nil
	}

	got := processRecords(context.Background(), fetches, handler, nil, discardLogger(), "test-group")

	// partition 0 stops at offset 2 (only offset 1 committable); partition 1 both commit.
	var p0, p1 []int64
	for _, r := range got {
		switch r.Partition {
		case 0:
			p0 = append(p0, r.Offset)
		case 1:
			p1 = append(p1, r.Offset)
		}
	}
	if len(p0) != 1 || p0[0] != 1 {
		t.Errorf("partition 0 committable = %v, want [1] (offset 2 failed, 3 must be skipped)", p0)
	}
	if len(p1) != 2 || p1[0] != 5 || p1[1] != 6 {
		t.Errorf("partition 1 committable = %v, want [5 6] (independent of partition 0 failure)", p1)
	}
}

// TestConsumerOpts_AcceptedByClient is a smoke test that the option set built by
// consumerOpts is mutually valid (e.g. SessionTimeout >= 3x HeartbeatInterval,
// a valid balancer, offset-reset and auto-commit settings) — franz validates
// these when the client is constructed. The behavioural invariants F-031
// (offset reset to oldest) and F-053 (recovery/error surfacing) are verified
// against a real broker in kafka_integration_test.go, because franz options are
// opaque and cannot be introspected the way sarama's *Config struct could.
func TestConsumerOpts_AcceptedByClient(t *testing.T) {
	client, err := kgo.NewClient(consumerOpts([]string{"localhost:9092"}, "test-group", []string{"test"})...)
	if err != nil {
		t.Fatalf("consumerOpts produced an invalid franz option set: %v", err)
	}
	client.Close()
}

// TestClampBatchMaxBytes guards teranode #660: values at or below the 1 MiB
// broker default are floored to the default (never used as a hard cap that
// would reject normal records), while an explicit larger value is honoured.
func TestClampBatchMaxBytes(t *testing.T) {
	cases := []struct {
		name      string
		requested int
		want      int32
	}{
		{"zero floors to default", 0, defaultBatchMaxBytes},
		{"small flush-style value floors to default", 1024, defaultBatchMaxBytes},
		{"exactly default", int(defaultBatchMaxBytes), defaultBatchMaxBytes},
		{"10MiB honoured (merkle cap-raise)", 10 * 1024 * 1024, 10 * 1024 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampBatchMaxBytes(tc.requested); got != tc.want {
				t.Errorf("clampBatchMaxBytes(%d) = %d, want %d", tc.requested, got, tc.want)
			}
		})
	}
}
