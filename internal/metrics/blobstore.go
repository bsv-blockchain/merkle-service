package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// BlobStoreSweptFilesTotal counts blob files removed by the age sweeper —
// orphaned subtree blobs (no delete-at-height ever fired because their
// subtree-work item never completed) plus zero-byte ENOSPC litter. This is
// the visibility the 2026-07-15 dev-ovh-1 incident lacked: the store filled
// a 1TiB volume in ~3h and the growth was only diagnosable by walking the
// volume by hand.
var BlobStoreSweptFilesTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "merkle_blobstore_swept_files_total",
		Help: "Blob files removed by the blob-store age sweeper (orphaned subtree blobs and zero-byte litter).",
	},
)

// BlobStoreSweptBytesTotal counts the bytes reclaimed by the age sweeper.
var BlobStoreSweptBytesTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "merkle_blobstore_swept_bytes_total",
		Help: "Bytes reclaimed by the blob-store age sweeper.",
	},
)

func init() {
	Registry.MustRegister(
		BlobStoreSweptFilesTotal,
		BlobStoreSweptBytesTotal,
	)
}

// AddBlobSweep records one age-sweep pass. Zero deltas are recorded too so
// the series exists (and stays flat) on healthy stores.
func AddBlobSweep(files int, bytes int64) {
	BlobStoreSweptFilesTotal.Add(float64(files))
	BlobStoreSweptBytesTotal.Add(float64(bytes))
}
