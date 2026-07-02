package block

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// fakeExpectedStump returns a fixed index set per URL — enough to prove the
// emit path reads the set and attaches it to BLOCK_PROCESSED.
type fakeExpectedStump struct{ byURL map[string][]int }

func (f *fakeExpectedStump) AddSubtreeIndex(string, int, []string) error { return nil }
func (f *fakeExpectedStump) GetSubtreeIndices(_, url string) ([]int, error) {
	return f.byURL[url], nil
}

// TestEmitBlockProcessed_AttachesExpectedIndices proves the read-side wiring:
// the BLOCK_PROCESSED message published for a URL carries that URL's recorded
// subtree-index set (so the receiver can detect a missing STUMP).
func TestEmitBlockProcessed_AttachesExpectedIndices(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &mockSyncProducer{}
	url := "http://cb.example.test/hook"

	s := &SubtreeWorkerService{
		urlRegistry:    &fakeURLRegistry{urls: []string{url}},
		expectedStumps: &fakeExpectedStump{byURL: map[string][]int{url: {3, 7}}},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(mock, "callback-test", logger)

	if err := s.emitBlockProcessed(context.Background(), "blk-expected", "", "", nil); err != nil {
		t.Fatalf("emitBlockProcessed: %v", err)
	}
	if len(mock.messages) != 1 {
		t.Fatalf("published %d messages, want 1", len(mock.messages))
	}

	var msg kafka.CallbackTopicMessage
	if err := json.Unmarshal(mock.messages[0].Value, &msg); err != nil {
		t.Fatalf("decode BLOCK_PROCESSED: %v", err)
	}
	if msg.Type != kafka.CallbackBlockProcessed {
		t.Fatalf("type = %q, want BLOCK_PROCESSED", msg.Type)
	}
	if !reflect.DeepEqual(msg.ExpectedSubtreeIndices, []int{3, 7}) {
		t.Fatalf("ExpectedSubtreeIndices = %v, want [3 7]", msg.ExpectedSubtreeIndices)
	}
}
