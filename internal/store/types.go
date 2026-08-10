package store

// IncrementResult is returned by SeenCounterStore.AddPeer / BatchAddPeer.
// NewCount is the weighted seen score after the call (sum of peer weights).
type IncrementResult struct {
	NewCount         int
	ThresholdReached bool // true only when score first reaches threshold (not above)
}

// AccumulatedCallbackEntry holds data for one subtree's contribution to a
// callback URL. All txids in an entry share the same STUMP and subtree index.
type AccumulatedCallbackEntry struct {
	TxIDs        []string
	SubtreeIndex int
	StumpData    []byte
}

// AccumulatedCallback holds aggregated entries for a single callback URL
// across multiple subtrees within a block.
type AccumulatedCallback struct {
	Entries []AccumulatedCallbackEntry
}
