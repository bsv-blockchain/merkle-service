package main

import (
	"testing"
)

// TestExitHook verifies that the exit hook is overridable. Issue #7 relies on
// the entrypoint exiting non-zero when client.Run returns a terminal error
// (e.g. ErrPublishExhausted) so the orchestrator can restart the pod. We can't
// exercise the full main() in-process because it needs Kafka brokers and a
// libp2p host, but verifying the indirection point keeps that contract honest.
func TestExitHook_IsOverridable(t *testing.T) {
	original := exit
	t.Cleanup(func() { exit = original })

	captured := -1
	exit = func(code int) { captured = code }

	exit(1)
	if captured != 1 {
		t.Fatalf("expected captured exit code 1, got %d", captured)
	}

	exit(0)
	if captured != 0 {
		t.Fatalf("expected captured exit code 0, got %d", captured)
	}
}
