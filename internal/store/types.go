package store

// IncrementResult is returned by SeenCounterStore.Increment.
type IncrementResult struct {
	NewCount         int
	ThresholdReached bool // true only when count first equals threshold (not above)
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
