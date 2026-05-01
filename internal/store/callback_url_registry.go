package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const (
	// callbackURLBin holds the original URL string on each per-URL record.
	// Each registered callback URL lives in its own Aerospike record keyed by
	// sha256(url); the bin lets GetAll reconstruct the URL list during scan.
	callbackURLBin = "u"

	// defaultCallbackURLRegistryTTLSec is the eviction window applied to a
	// registered callback URL when no explicit TTL is configured. URLs that
	// haven't seen a fresh `Add` within this window are evicted by Aerospike's
	// nsup. 7 days is long enough to cover quiet-but-still-watching tenants
	// while bounding the registry's growth.
	defaultCallbackURLRegistryTTLSec = 7 * 24 * 60 * 60
)

// aerospikeCallbackURLRegistry is the Aerospike-backed CallbackURLRegistry
// implementation. Every registered URL is stored in its own record keyed by
// sha256(url); GetAll reconstructs the active list via ScanAll. Per-record
// TTL prevents the registry from growing forever (F-037 / issue #23). Each
// `Add` rewrites the record, which refreshes the TTL — i.e. a URL stays in
// the registry as long as it keeps registering watches.
type aerospikeCallbackURLRegistry struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
	// ttlSec is the per-record TTL applied on Add. Treated as 0 = no TTL only
	// when the configured value is non-positive (callers must opt out
	// explicitly); the constructor coerces to defaultCallbackURLRegistryTTLSec.
	ttlSec int
}

var _ CallbackURLRegistry = (*aerospikeCallbackURLRegistry)(nil)

// NewCallbackURLRegistry creates a new Aerospike-backed callback URL registry.
// ttlSec sets the per-URL eviction window — pass 0 (or negative) to use the
// default of 7 days.
func NewCallbackURLRegistry(client *AerospikeClient, setName string, ttlSec int, maxRetries int, retryBaseMs int, logger *slog.Logger) CallbackURLRegistry {
	if ttlSec <= 0 {
		ttlSec = defaultCallbackURLRegistryTTLSec
	}
	return &aerospikeCallbackURLRegistry{
		client:      client,
		setName:     setName,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
		ttlSec:      ttlSec,
	}
}

// callbackURLKey returns the Aerospike record key digest for a callback URL.
// Hashing keeps the key size bounded regardless of URL length and guarantees
// natural deduplication: the same URL hashes to the same record, so repeated
// Add calls upsert a single record (and refresh its TTL) instead of growing.
func callbackURLKey(url string) string {
	h := sha256.Sum256([]byte(url))
	return hex.EncodeToString(h[:])
}

// Add registers a callback URL in the registry. Repeat calls upsert the same
// record and refresh its TTL, so an actively-watching URL never expires.
func (r *aerospikeCallbackURLRegistry) Add(callbackURL string) error {
	key, err := as.NewKey(r.client.Namespace(), r.setName, callbackURLKey(callbackURL))
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := r.client.WritePolicy(r.maxRetries, r.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	if r.ttlSec > 0 {
		wp.Expiration = uint32(r.ttlSec)
	}

	bins := as.BinMap{callbackURLBin: callbackURL}
	if err := r.client.Client().Put(wp, key, bins); err != nil {
		// If TTL is rejected (namespace lacks nsup-period), retry without TTL.
		// We log loudly because losing TTL re-introduces F-037's unbounded
		// growth — the operator should fix the namespace config.
		if asErr, ok := err.(as.Error); ok && asErr.Matches(astypes.FAIL_FORBIDDEN) && r.ttlSec > 0 {
			if r.logger != nil {
				r.logger.Warn("callback URL registry TTL rejected, writing without TTL "+
					"(configure Aerospike nsup-period to enable bounded growth)",
					"url", callbackURL)
			}
			wp2 := r.client.WritePolicy(r.maxRetries, r.retryBaseMs)
			wp2.RecordExistsAction = as.UPDATE
			if err2 := r.client.Client().Put(wp2, key, bins); err2 != nil {
				return fmt.Errorf("failed to add callback URL to registry (without TTL): %w", err2)
			}
			return nil
		}
		return fmt.Errorf("failed to add callback URL to registry: %w", err)
	}
	return nil
}

// GetAll returns every registered callback URL. Implemented as a ScanAll over
// the registry set — the URL count is bounded by per-record TTL eviction
// (typically <= a few thousand) and BLOCK_PROCESSED fan-out runs at most once
// per block, so a scan-per-block is cheap relative to the actual callback
// publish work.
func (r *aerospikeCallbackURLRegistry) GetAll() ([]string, error) {
	sp := as.NewScanPolicy()
	sp.IncludeBinData = true
	// Bound the scan so a stalled node can't hang BLOCK_PROCESSED forever.
	sp.TotalTimeout = 30 * time.Second
	sp.SocketTimeout = 5 * time.Second
	// Match the rest of the codebase's stance: don't pile retries onto a
	// flaky cluster — let the caller re-scan on the next block.
	sp.MaxRetries = 0

	rs, err := r.client.Client().ScanAll(sp, r.client.Namespace(), r.setName, callbackURLBin)
	if err != nil {
		return nil, fmt.Errorf("failed to scan callback URLs: %w", err)
	}
	defer func() { _ = rs.Close() }()

	var urls []string
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("scan error reading callback URLs: %w", res.Err)
		}
		if res.Record == nil {
			continue
		}
		v, ok := res.Record.Bins[callbackURLBin].(string)
		if !ok || v == "" {
			continue
		}
		urls = append(urls, v)
	}
	return urls, nil
}
