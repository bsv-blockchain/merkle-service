// Package logfields defines the canonical snake_case slog field keys used
// across merkle-service, shared with the arcade repo's own log-field canon.
// The same logical identifier (a txid, a block hash, a callback URL, ...)
// must always be logged under the same key name — otherwise a Coralogix
// search for e.g. a block hash misses every call site that spelled its log
// key differently. Every log call that emits one of these identifiers should
// use the typed constructor below rather than a bare string literal, so a
// future rename only touches this file.
package logfields

import "log/slog"

// Canonical snake_case field key names. Kept as exported consts so callers
// that can't use the typed constructor (e.g. building a map) still spell the
// key consistently, and so the accompanying test can enumerate them.
const (
	KeyTxID = "txid"
	// KeyTxIDs is a LIST of txids. A txid COUNT must use KeyTxIDCount instead:
	// mixing an int and an array under the same key path breaks
	// Coralogix/Elasticsearch field mapping.
	KeyTxIDs     = "txids"
	KeyTxIDCount = "txid_count"
	// KeyTxIDsTruncated marks a KeyTxIDs list that was capped (see
	// subtree.seenTxidLogMax) rather than complete.
	KeyTxIDsTruncated = "txids_truncated"
	KeyBlockHash      = "block_hash"
	KeyBlockHeight    = "block_height"
	KeySubtreeHash    = "subtree_hash"
	KeySubtreeIndex   = "subtree_index"
	KeyCallbackURL    = "callback_url"
	KeyDataHubURL     = "datahub_url"
	KeyPeerID         = "peer_id"
	KeyRequestID      = "request_id"

	// KeyTraceID and KeySpanID are reserved for the tracing work that lands
	// alongside this canon (see the otel-coralogix branch). No constructor
	// yet — added when the tracing integration needs to stamp them on log
	// records.
	KeyTraceID = "trace_id"
	KeySpanID  = "span_id"
)

// TxID returns the canonical attribute for a single transaction id.
func TxID(v string) slog.Attr { return slog.String(KeyTxID, v) }

// TxIDs returns the canonical attribute for a list of transaction ids.
func TxIDs(v []string) slog.Attr { return slog.Any(KeyTxIDs, v) }

// TxIDCount returns the canonical attribute for a count of transaction ids.
func TxIDCount(v int) slog.Attr { return slog.Int(KeyTxIDCount, v) }

// TxIDsTruncated returns the canonical attribute marking whether a TxIDs
// list was capped (see subtree.seenTxidLogMax) rather than complete.
func TxIDsTruncated(v bool) slog.Attr { return slog.Bool(KeyTxIDsTruncated, v) }

// BlockHash returns the canonical attribute for a block hash.
func BlockHash(v string) slog.Attr { return slog.String(KeyBlockHash, v) }

// BlockHeight returns the canonical attribute for a block height. The
// parameter type matches kafka.BlockMessage.Height / datahub.BlockMetadata.Height.
func BlockHeight(v uint32) slog.Attr { return slog.Uint64(KeyBlockHeight, uint64(v)) }

// SubtreeHash returns the canonical attribute for a subtree hash.
func SubtreeHash(v string) slog.Attr { return slog.String(KeySubtreeHash, v) }

// SubtreeIndex returns the canonical attribute for a subtree's index within a block.
func SubtreeIndex(v int) slog.Attr { return slog.Int(KeySubtreeIndex, v) }

// CallbackURL returns the canonical attribute for an arcade callback URL.
func CallbackURL(v string) slog.Attr { return slog.String(KeyCallbackURL, v) }

// DataHubURL returns the canonical attribute for a Teranode DataHub URL —
// either a peer's base URL or a full request URL derived from it.
func DataHubURL(v string) slog.Attr { return slog.String(KeyDataHubURL, v) }

// PeerID returns the canonical attribute for a P2P peer identifier.
func PeerID(v string) slog.Attr { return slog.String(KeyPeerID, v) }

// RequestID returns the canonical attribute for an HTTP request id.
func RequestID(v string) slog.Attr { return slog.String(KeyRequestID, v) }
