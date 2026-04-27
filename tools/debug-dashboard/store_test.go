package main

import (
	"sync"
	"testing"
	"time"
)

func TestCallbackStore_Add_GetAll(t *testing.T) {
	s := NewCallbackStore(10)

	s.Add(CallbackEntry{Timestamp: time.Now(), RawJSON: `{"status":"SEEN_ON_NETWORK"}`})
	s.Add(CallbackEntry{Timestamp: time.Now(), RawJSON: `{"status":"MINED"}`})

	all := s.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}

	// GetAll returns reverse chronological (newest first).
	if all[0].RawJSON != `{"status":"MINED"}` {
		t.Errorf("expected newest first, got %s", all[0].RawJSON)
	}
	if all[1].RawJSON != `{"status":"SEEN_ON_NETWORK"}` {
		t.Errorf("expected oldest second, got %s", all[1].RawJSON)
	}
}

func TestCallbackStore_CapacityEviction(t *testing.T) {
	s := NewCallbackStore(3)

	s.Add(CallbackEntry{RawJSON: "1"})
	s.Add(CallbackEntry{RawJSON: "2"})
	s.Add(CallbackEntry{RawJSON: "3"})
	s.Add(CallbackEntry{RawJSON: "4"})

	if s.Count() != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", s.Count())
	}

	all := s.GetAll()
	// Oldest ("1") should have been evicted.
	if all[2].RawJSON != "2" {
		t.Errorf("expected oldest remaining entry '2', got %s", all[2].RawJSON)
	}
	if all[0].RawJSON != "4" {
		t.Errorf("expected newest entry '4', got %s", all[0].RawJSON)
	}
}

func TestCallbackStore_Clear(t *testing.T) {
	s := NewCallbackStore(10)
	s.Add(CallbackEntry{RawJSON: "test"})
	s.Clear()

	if s.Count() != 0 {
		t.Errorf("expected 0 entries after clear, got %d", s.Count())
	}
}

func TestCallbackStore_ThreadSafety(t *testing.T) {
	s := NewCallbackStore(100)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Add(CallbackEntry{RawJSON: "concurrent"})
		}()
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.GetAll()
		}()
	}

	wg.Wait()

	if s.Count() != 50 {
		t.Errorf("expected 50 entries, got %d", s.Count())
	}
}

func TestTxidTracker_Add_GetAll(t *testing.T) {
	tracker := NewTxidTracker()
	tracker.Add("txid1", []string{"http://cb1"})
	tracker.Add("txid2", []string{"http://cb2"})

	all := tracker.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 tracked txids, got %d", len(all))
	}
	if all[0].Txid != "txid1" {
		t.Errorf("expected txid1, got %s", all[0].Txid)
	}
}

func TestTxidTracker_Add_Deduplicate(t *testing.T) {
	tracker := NewTxidTracker()
	tracker.Add("txid1", []string{"http://cb1"})
	tracker.Add("txid1", []string{"http://cb1", "http://cb2"})

	if tracker.Count() != 1 {
		t.Fatalf("expected 1 tracked txid (deduplicated), got %d", tracker.Count())
	}

	all := tracker.GetAll()
	if len(all[0].CallbackURLs) != 2 {
		t.Errorf("expected 2 callback URLs after update, got %d", len(all[0].CallbackURLs))
	}
}
