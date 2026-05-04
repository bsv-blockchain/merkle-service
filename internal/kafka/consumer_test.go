package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// fakeClaim is a tiny stand-in for sarama.ConsumerGroupClaim. Only Messages()
// is exercised by ConsumeClaim; the other interface methods return zero values.
type fakeClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (f *fakeClaim) Topic() string                            { return "test" }
func (f *fakeClaim) Partition() int32                         { return 0 }
func (f *fakeClaim) InitialOffset() int64                     { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return f.messages }

// fakeSession is a tiny stand-in for sarama.ConsumerGroupSession. It records
// every MarkMessage call so tests can assert which offsets advanced.
type fakeSession struct {
	ctx context.Context

	mu     sync.Mutex
	marked []*sarama.ConsumerMessage
}

func newFakeSession(ctx context.Context) *fakeSession {
	return &fakeSession{ctx: ctx}
}

func (f *fakeSession) Claims() map[string][]int32 { return nil }
func (f *fakeSession) MemberID() string           { return "" }
func (f *fakeSession) GenerationID() int32        { return 0 }
func (f *fakeSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (f *fakeSession) Commit() {}
func (f *fakeSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

func (f *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, msg)
}
func (f *fakeSession) Context() context.Context { return f.ctx }

func (f *fakeSession) markedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.marked))
	for i, m := range f.marked {
		out[i] = m.Offset
	}
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runConsumeClaim wires a handler to ConsumeClaim, feeds it the supplied
// messages, and returns the session (for marked-offset inspection) plus the
// final return value of ConsumeClaim. The channel is closed after all messages
// are sent so a successful run terminates naturally.
func runConsumeClaim(t *testing.T, handler MessageHandler, msgs []*sarama.ConsumerMessage) (*fakeSession, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := newFakeSession(ctx)
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, len(msgs)+1)}
	for _, m := range msgs {
		claim.messages <- m
	}
	close(claim.messages)

	h := &consumerGroupHandler{
		handler: handler,
		logger:  discardLogger(),
		ready:   make(chan struct{}),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- h.ConsumeClaim(session, claim)
	}()

	select {
	case err := <-errCh:
		return session, err
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("ConsumeClaim did not return in time")
		return nil, nil
	}
}

func msg(offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: 0,
		Offset:    offset,
		Value:     []byte("v"),
	}
}

// TestConsumeClaim_AllSuccess verifies that when the handler returns nil for
// every message, every offset is marked and ConsumeClaim returns nil.
func TestConsumeClaim_AllSuccess(t *testing.T) {
	handler := func(_ context.Context, _ *sarama.ConsumerMessage) error {
		return nil
	}

	msgs := []*sarama.ConsumerMessage{msg(10), msg(11), msg(12)}
	session, err := runConsumeClaim(t, handler, msgs)
	if err != nil {
		t.Fatalf("ConsumeClaim returned unexpected error: %v", err)
	}

	got := session.markedOffsets()
	want := []int64{10, 11, 12}
	if len(got) != len(want) {
		t.Fatalf("marked offsets: got %v, want %v", got, want)
	}
	for i, o := range want {
		if got[i] != o {
			t.Errorf("marked[%d] = %d, want %d", i, got[i], o)
		}
	}
}

// TestConsumeClaim_StopsOnHandlerError is the regression test for F-030. The
// previous implementation logged and continued, allowing later successful
// messages to advance the committed offset past a failed one. The fix returns
// the handler error and stops processing further messages so sarama redelivers
// the failed offset in the next session.
func TestConsumeClaim_StopsOnHandlerError(t *testing.T) {
	wantErr := errors.New("boom")
	var calls int
	handler := func(_ context.Context, _ *sarama.ConsumerMessage) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	}

	msgs := []*sarama.ConsumerMessage{msg(10), msg(11), msg(12), msg(13)}
	session, err := runConsumeClaim(t, handler, msgs)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ConsumeClaim error: got %v, want %v", err, wantErr)
	}

	got := session.markedOffsets()
	want := []int64{10}
	if len(got) != len(want) {
		t.Fatalf("marked offsets: got %v, want %v (failed offset 11 must NOT be marked, and 12/13 must NOT have been processed)", got, want)
	}
	for i, o := range want {
		if got[i] != o {
			t.Errorf("marked[%d] = %d, want %d", i, got[i], o)
		}
	}

	// Handler must NOT have been invoked for offsets 12 or 13: bailing out
	// preserves the original ordering guarantee that sarama redelivers the
	// failed offset and everything after it on the next session.
	if calls != 2 {
		t.Errorf("handler invoked %d times, want 2 (one success + one failure)", calls)
	}
}

// TestConsumeClaim_FirstMessageError covers the corner case where the very
// first message fails: nothing should be marked, and ConsumeClaim should
// surface the error immediately.
func TestConsumeClaim_FirstMessageError(t *testing.T) {
	wantErr := errors.New("first")
	handler := func(_ context.Context, _ *sarama.ConsumerMessage) error {
		return wantErr
	}

	msgs := []*sarama.ConsumerMessage{msg(100), msg(101)}
	session, err := runConsumeClaim(t, handler, msgs)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ConsumeClaim error: got %v, want %v", err, wantErr)
	}
	if got := session.markedOffsets(); len(got) != 0 {
		t.Errorf("expected no marked offsets, got %v", got)
	}
}

// TestNewConsumerConfig_InitialOffsetOldest is the regression test for F-031.
// Consumer groups with no committed offsets must start at the OLDEST available
// offset so renaming a group, recovering lost offsets, or deploying into a
// fresh environment with a non-empty topic still processes the durable
// backlog instead of silently skipping it.
func TestNewConsumerConfig_InitialOffsetOldest(t *testing.T) {
	cfg := newConsumerConfig()
	if cfg == nil {
		t.Fatal("newConsumerConfig returned nil")
	}
	if got, want := cfg.Consumer.Offsets.Initial, sarama.OffsetOldest; got != want {
		t.Errorf("Consumer.Offsets.Initial = %d, want %d (sarama.OffsetOldest); a new consumer group must replay the backlog, not jump to the topic head", got, want)
	}
	if got := cfg.Consumer.Offsets.Initial; got == sarama.OffsetNewest {
		t.Errorf("Consumer.Offsets.Initial must not be sarama.OffsetNewest (F-031): new groups would silently skip queued work")
	}
}

// fakeGroup is a stand-in for sarama.ConsumerGroup used by the Start tests
// below. It lets each test script the behaviour of Consume(): fire Setup
// (by closing the handler's ready channel), return an error, block until
// the context is cancelled, etc.
type fakeGroup struct {
	consume func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	closed  chan struct{}
}

func newFakeGroup(fn func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error) *fakeGroup {
	return &fakeGroup{consume: fn, closed: make(chan struct{})}
}

func (f *fakeGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return f.consume(ctx, topics, handler)
}
func (f *fakeGroup) Errors() <-chan error                   { return nil }
func (f *fakeGroup) Pause(map[string][]int32)               {}
func (f *fakeGroup) Resume(map[string][]int32)              {}
func (f *fakeGroup) PauseAll()                              {}
func (f *fakeGroup) ResumeAll()                             {}
func (f *fakeGroup) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

// newTestConsumer wires a Consumer around a fakeGroup so we can drive the
// Start lifecycle without a real broker.
func newTestConsumer(group sarama.ConsumerGroup) *Consumer {
	return &Consumer{
		group:   group,
		topics:  []string{"test"},
		handler: func(_ context.Context, _ *sarama.ConsumerMessage) error { return nil },
		logger:  discardLogger(),
		ready:   make(chan struct{}),
	}
}

// TestStart_ReadyOnSetup is the happy path: Consume drives Setup, which
// closes c.ready, and Start returns nil.
func TestStart_ReadyOnSetup(t *testing.T) {
	group := newFakeGroup(func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
		// Mimic sarama: call Setup so the ready channel is closed, then
		// block until the parent context is cancelled.
		if err := handler.Setup(nil); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})

	c := newTestConsumer(group)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Setup fired")
	}

	if err := c.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

// TestStart_SetupErrorUnblocks is the F-029 regression test. If Consume
// returns an error before Setup fires, Start must NOT hang on <-c.ready.
// Instead it must surface the error via the errCh branch of the select.
func TestStart_SetupErrorUnblocks(t *testing.T) {
	wantErr := errors.New("broker unreachable")
	group := newFakeGroup(func(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		// Setup never runs; surface a setup-time failure on every call.
		return wantErr
	})

	c := newTestConsumer(group)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Start error: got %v, want wrapping %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start hung waiting on c.ready despite Consume error (F-029 regression)")
	}

	if err := c.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

// TestStart_ContextCancelUnblocks verifies that Start exits via ctx.Done()
// even when neither Setup fires nor Consume errors out (e.g. a hung broker
// connection that swallows the context internally for a while). Without the
// ctx.Done() branch added in F-029 this would block until the goroutine
// noticed cancellation and exited, which can be unbounded.
func TestStart_ContextCancelUnblocks(t *testing.T) {
	// Consume blocks until the context is cancelled and never calls Setup.
	group := newFakeGroup(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		<-ctx.Done()
		return ctx.Err()
	})

	c := newTestConsumer(group)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	// Let Start install its derived context, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// Either ctx.Err() directly or the wrapped Consume error are
		// acceptable: both mean Start unblocked rather than hanging on
		// <-c.ready.
		if err == nil {
			t.Fatalf("Start returned nil; expected cancellation error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start hung after context cancel (F-029 regression)")
	}

	if err := c.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

// TestConsumeClaim_ContextCancelled verifies the loop exits cleanly when the
// session context is canceled mid-flight (Stop / rebalance path).
func TestConsumeClaim_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := newFakeSession(ctx)
	// Use an unbuffered channel that we never close so the only exit path is
	// context cancellation.
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage)}

	h := &consumerGroupHandler{
		handler: func(_ context.Context, _ *sarama.ConsumerMessage) error { return nil },
		logger:  discardLogger(),
		ready:   make(chan struct{}),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- h.ConsumeClaim(session, claim)
	}()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("expected nil on context cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeClaim did not return after context cancel")
	}
}
