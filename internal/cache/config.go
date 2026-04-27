package cache

import "log/slog"

// maxValueSizeKB defines the maximum size of a cached value in kilobytes.
const maxValueSizeKB = 2 // 2KB

// maxValueSizeLog is the log2 representation of maxValueSizeKB (with offset).
// Used for faster size calculations with bitwise operations.
const maxValueSizeLog = 11 // 10 + log2(maxValueSizeKB)

// BucketsCount defines the number of cache buckets.
const BucketsCount = 32

// ChunkSize defines the memory chunk size.
const ChunkSize = maxValueSizeKB * 512

// defaultTrimRatio is the default ratio for trimming the cache when full.
const defaultTrimRatio = 2

// logCacheSize logs which cache configuration is active for diagnostics.
func logCacheSize() {
	slog.Debug("Using small cache configuration", "buckets", BucketsCount, "chunkSize", ChunkSize)
}
