package sql

import (
	"reflect"
	"testing"
)

func TestExpectedStump_AddAndGet(t *testing.T) {
	db, d := newTestDB(t)
	s := newExpectedStump(db, d, 600)

	const block = "block-aaa"
	urlA, urlB := "https://a.example/cb", "https://b.example/cb"

	// Subtree 2 matched both URLs; subtree 5 matched only A; subtree 0 only B.
	mustAdd(t, s, block, 2, []string{urlA, urlB})
	mustAdd(t, s, block, 5, []string{urlA})
	mustAdd(t, s, block, 0, []string{urlB})

	if got := mustGet(t, s, block, urlA); !reflect.DeepEqual(got, []int{2, 5}) {
		t.Errorf("urlA indices = %v, want [2 5]", got)
	}
	if got := mustGet(t, s, block, urlB); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Errorf("urlB indices = %v, want [0 2] (ascending)", got)
	}
}

func TestExpectedStump_Idempotent(t *testing.T) {
	db, d := newTestDB(t)
	s := newExpectedStump(db, d, 600)

	const block = "block-bbb"
	url := "https://a.example/cb"

	// A re-driven subtree work item re-adds the same index repeatedly — the set
	// must not grow (this is what keeps arcade's expected count correct).
	for i := 0; i < 4; i++ {
		mustAdd(t, s, block, 7, []string{url})
	}
	if got := mustGet(t, s, block, url); !reflect.DeepEqual(got, []int{7}) {
		t.Fatalf("idempotency broken: indices = %v, want [7]", got)
	}
}

func TestExpectedStump_IsolationAndEmpty(t *testing.T) {
	db, d := newTestDB(t)
	s := newExpectedStump(db, d, 600)

	mustAdd(t, s, "block-1", 3, []string{"https://a.example/cb"})

	// A different block, or a URL with no matches, has an empty set (arcade then
	// expects zero STUMPs — the correct answer for a URL absent from this block).
	if got := mustGet(t, s, "block-2", "https://a.example/cb"); len(got) != 0 {
		t.Errorf("different block leaked indices: %v", got)
	}
	if got := mustGet(t, s, "block-1", "https://other.example/cb"); len(got) != 0 {
		t.Errorf("unmatched URL returned indices: %v", got)
	}
}

func mustAdd(t *testing.T, s *expectedStump, block string, idx int, urls []string) {
	t.Helper()
	if err := s.AddSubtreeIndex(block, idx, urls); err != nil {
		t.Fatalf("AddSubtreeIndex(%s, %d, %v): %v", block, idx, urls, err)
	}
}

func mustGet(t *testing.T, s *expectedStump, block, url string) []int {
	t.Helper()
	got, err := s.GetSubtreeIndices(block, url)
	if err != nil {
		t.Fatalf("GetSubtreeIndices(%s, %s): %v", block, url, err)
	}
	return got
}
