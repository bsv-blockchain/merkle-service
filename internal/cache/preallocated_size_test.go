package cache

import (
	"math"
	"strings"
	"testing"
)

// A Preallocated cache whose per-bucket size is not a whole number of chunks
// is floored to whole chunks instead of panicking while slicing the mmap
// (#36). This matches bucketTrimmed.
func TestNew_PreallocatedNonChunkAlignedSize(t *testing.T) {
	cases := []struct {
		name           string
		bucketBytes    int
		expectedChunks int
	}{
		{"one chunk plus a remainder", ChunkSize + 500, 1},
		{"two chunks plus a remainder", 2*ChunkSize + 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(BucketsCount*tc.bucketBytes, Preallocated)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			b, ok := c.buckets[0].(*bucketPreallocated)
			if !ok {
				t.Fatalf("expected a preallocated bucket, got %T", c.buckets[0])
			}
			if got := len(b.chunks); got != tc.expectedChunks {
				t.Fatalf("expected %d chunks, got %d", tc.expectedChunks, got)
			}
		})
	}
}

// New raises a sub-chunk bucket size to ChunkSize*8 before Init, so Init's own
// sub-chunk case is only reachable directly; it allocates one whole chunk.
func TestBucketPreallocated_InitSubChunkSize(t *testing.T) {
	var b bucketPreallocated
	if err := b.Init(ChunkSize/2, defaultTrimRatio); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := len(b.chunks); got != 1 {
		t.Fatalf("expected 1 chunk, got %d", got)
	}
}

// On 32-bit builds a bucket size that fits under maxBucketSize can still
// exceed math.MaxInt; Init refuses it instead of passing a wrapped size to
// mmap.
func TestBucketPreallocated_InitSizeOverflowsInt(t *testing.T) {
	if uint64(math.MaxInt) >= maxBucketSize {
		t.Skip("every bucket size below maxBucketSize fits in an int on this platform")
	}
	var b bucketPreallocated
	err := b.Init(uint64(math.MaxInt)+ChunkSize, defaultTrimRatio)
	if err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("expected an overflow error before mmap, got %v", err)
	}
}
