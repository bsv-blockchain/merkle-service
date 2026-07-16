package block

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// recordedStumpAdd captures one AddSubtreeIndex invocation so tests can assert
// exactly what the write path recorded.
type recordedStumpAdd struct {
	blockHash    string
	subtreeIndex int
	urls         []string
}

// recordingExpectedStump is an ExpectedStumpStore fake that counts writes and
// reads: AddSubtreeIndex calls are captured, GetSubtreeIndices serves a fixed
// per-URL set and counts invocations. Used to prove the
// block.emitExpectedStumpSet flag gates the WRITE path (recording) and the
// READ path (attach) together.
type recordingExpectedStump struct {
	mu    sync.Mutex
	adds  []recordedStumpAdd
	reads int
	byURL map[string][]int
}

func (r *recordingExpectedStump) AddSubtreeIndex(blockHash string, subtreeIndex int, callbackURLs []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adds = append(r.adds, recordedStumpAdd{
		blockHash:    blockHash,
		subtreeIndex: subtreeIndex,
		urls:         append([]string(nil), callbackURLs...),
	})
	return nil
}

func (r *recordingExpectedStump) GetSubtreeIndices(_, url string) ([]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads++
	return r.byURL[url], nil
}

func (r *recordingExpectedStump) addCalls() []recordedStumpAdd {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedStumpAdd(nil), r.adds...)
}

func (r *recordingExpectedStump) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// boolPtr returns a pointer to b, for populating *bool config fields.
func boolPtr(b bool) *bool { return &b }

// TestEmitBlockProcessed_FlagOff_OmitsExpectedIndices proves the attach path
// is suppressed when block.emitExpectedStumpSet is explicitly false: the
// BLOCK_PROCESSED message carries NO expectedSubtreeIndices field (not an
// empty set — an empty set would tell the receiver to expect zero STUMPs,
// reintroducing the silent-loss bug), and the store is never even read.
func TestEmitBlockProcessed_FlagOff_OmitsExpectedIndices(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &mockSyncProducer{}
	url := "http://cb.example.test/hook"

	store := &recordingExpectedStump{byURL: map[string][]int{url: {3, 7}}}
	s := &SubtreeWorkerService{
		blockCfg:       config.BlockConfig{EmitExpectedStumpSet: boolPtr(false)},
		urlRegistry:    &fakeURLRegistry{urls: []string{url}},
		expectedStumps: store,
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(mock, "callback-test", logger)

	if err := s.emitBlockProcessed(context.Background(), "blk-flag-off", "", "", nil); err != nil {
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
	if msg.ExpectedSubtreeIndices != nil {
		t.Errorf("ExpectedSubtreeIndices = %v, want nil when flag is off", msg.ExpectedSubtreeIndices)
	}
	// omitempty must drop the key entirely so older/newer receivers treat the
	// payload exactly like a pre-feature one.
	if bytes.Contains(mock.messages[0].Value, []byte("expectedSubtreeIndices")) {
		t.Errorf("raw BLOCK_PROCESSED payload contains expectedSubtreeIndices key with flag off: %s", mock.messages[0].Value)
	}
	if got := store.readCount(); got != 0 {
		t.Errorf("GetSubtreeIndices called %d times, want 0 when flag is off", got)
	}
}

// TestHandleMessage_FlagOff_SkipsAddSubtreeIndex proves the WRITE path is
// gated: with the flag explicitly false, a successfully processed subtree that
// matched a callback URL records nothing into the expected-STUMP store, while
// the STUMP callback still publishes and the per-block counter still
// decrements exactly once.
func TestHandleMessage_FlagOff_SkipsAddSubtreeIndex(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-flag-off"
	counter := newCountingSubtreeCounter()
	// Pre-seed above 1 so the counter does not drain to zero — this test is
	// about the write path, not the emit path.
	_ = counter.Init(blockHash, 2, nil)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)
	recording := &recordingExpectedStump{}
	svc.expectedStumps = recording
	svc.blockCfg.EmitExpectedStumpSet = boolPtr(false)

	value := makeWorkMessageBytes(t, blockHash, contentAddressOf(t, subtreePayload), server.URL, 0)
	if err := svc.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
		t.Fatalf("handleMessage with flag off: expected nil error, got: %v", err)
	}

	if got := len(recording.addCalls()); got != 0 {
		t.Errorf("AddSubtreeIndex called %d times, want 0 when flag is off", got)
	}
	if got := cbMock.sentCountOfType(kafka.CallbackStump); got != 1 {
		t.Errorf("expected 1 STUMP callback publish with flag off, got %d", got)
	}
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
}

// TestHandleMessage_FlagUnset_RecordsSubtreeIndex pins default-on semantics
// in-path: a zero-valued BlockConfig (flag nil, as every directly-constructed
// service has) records the subtree index for each matched callback URL —
// exactly once, with the right block hash, index, and URL set.
func TestHandleMessage_FlagUnset_RecordsSubtreeIndex(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-flag-unset"
	counter := newCountingSubtreeCounter()
	_ = counter.Init(blockHash, 2, nil)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)
	recording := &recordingExpectedStump{}
	svc.expectedStumps = recording
	// blockCfg.EmitExpectedStumpSet deliberately left nil: unset means enabled.

	value := makeWorkMessageBytes(t, blockHash, contentAddressOf(t, subtreePayload), server.URL, 0)
	if err := svc.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
		t.Fatalf("handleMessage with flag unset: expected nil error, got: %v", err)
	}

	adds := recording.addCalls()
	if len(adds) != 1 {
		t.Fatalf("AddSubtreeIndex called %d times, want exactly 1 with flag unset", len(adds))
	}
	if adds[0].blockHash != blockHash {
		t.Errorf("recorded blockHash = %q, want %q", adds[0].blockHash, blockHash)
	}
	if adds[0].subtreeIndex != 0 {
		t.Errorf("recorded subtreeIndex = %d, want 0", adds[0].subtreeIndex)
	}
	// The harness's staticRegStore matches every txid to this single URL.
	if want := []string{"http://cb.example.test/hook"}; !reflect.DeepEqual(adds[0].urls, want) {
		t.Errorf("recorded urls = %v, want %v", adds[0].urls, want)
	}
	if got := cbMock.sentCountOfType(kafka.CallbackStump); got != 1 {
		t.Errorf("expected 1 STUMP callback publish, got %d", got)
	}
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once, got %d", got)
	}
}
