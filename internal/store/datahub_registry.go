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
	// dataHubURLBin holds the original URL string on each per-URL record.
	// Mirror of callbackURLBin — keyed by sha256(url) so repeated Adds
	// upsert a single record and refresh its TTL.
	dataHubURLBin = "u"

	// defaultDataHubRegistryTTLSec is the eviction window applied to a
	// remembered DataHub URL when no explicit TTL is configured. 7 days
	// keeps URLs around long enough for reprocess of recent blocks while
	// dropping permanently-dead DataHubs.
	defaultDataHubRegistryTTLSec = 7 * 24 * 60 * 60
)

// aerospikeDataHubRegistry is the Aerospike-backed DataHubRegistry. Modeled
// directly on aerospikeCallbackURLRegistry: each URL lives in its own record
// keyed by sha256(url), GetAll reconstructs the active list via ScanAll, and
// per-record TTL (refreshed on every Add) bounds growth.
type aerospikeDataHubRegistry struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
	ttlSec      int
}

var _ DataHubRegistry = (*aerospikeDataHubRegistry)(nil)

// NewDataHubRegistry creates a new Aerospike-backed DataHub URL registry.
// ttlSec sets the per-URL eviction window — pass 0 (or negative) to use the
// default of 7 days.
func NewDataHubRegistry(client *AerospikeClient, setName string, ttlSec, maxRetries, retryBaseMs int, logger *slog.Logger) DataHubRegistry {
	if ttlSec <= 0 {
		ttlSec = defaultDataHubRegistryTTLSec
	}
	return &aerospikeDataHubRegistry{
		client:      client,
		setName:     setName,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
		ttlSec:      ttlSec,
	}
}

func dataHubURLKey(url string) string {
	h := sha256.Sum256([]byte(url))
	return hex.EncodeToString(h[:])
}

// Add registers a DataHub URL. Repeat calls upsert and refresh TTL so an
// actively-serving DataHub stays in the registry as long as it keeps
// successfully serving blocks.
func (r *aerospikeDataHubRegistry) Add(dataHubURL string) error {
	key, err := as.NewKey(r.client.Namespace(), r.setName, dataHubURLKey(dataHubURL))
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := r.client.WritePolicy(r.maxRetries, r.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	if r.ttlSec > 0 {
		wp.Expiration = uint32(r.ttlSec) //nolint:gosec // ttlSec is config-validated and fits uint32
	}

	bins := as.BinMap{dataHubURLBin: dataHubURL}
	if err := r.client.Client().Put(wp, key, bins); err != nil {
		if err.Matches(astypes.FAIL_FORBIDDEN) && r.ttlSec > 0 {
			if r.logger != nil {
				r.logger.Warn("DataHub registry TTL rejected, writing without TTL "+
					"(configure Aerospike nsup-period to enable bounded growth)",
					"url", dataHubURL)
			}
			wp2 := r.client.WritePolicy(r.maxRetries, r.retryBaseMs)
			wp2.RecordExistsAction = as.UPDATE
			if err2 := r.client.Client().Put(wp2, key, bins); err2 != nil {
				return fmt.Errorf("failed to add DataHub URL to registry (without TTL): %w", err2)
			}
			return nil
		}
		return fmt.Errorf("failed to add DataHub URL to registry: %w", err)
	}
	return nil
}

// GetAll returns every registered DataHub URL via a bounded ScanAll. The set
// is small (one entry per known DataHub on this network) so a per-call scan
// is cheap relative to the actual reprocess work.
func (r *aerospikeDataHubRegistry) GetAll() ([]string, error) {
	sp := as.NewScanPolicy()
	sp.IncludeBinData = true
	sp.TotalTimeout = 30 * time.Second
	sp.SocketTimeout = 5 * time.Second
	sp.MaxRetries = 0

	rs, err := r.client.Client().ScanAll(sp, r.client.Namespace(), r.setName, dataHubURLBin)
	if err != nil {
		return nil, fmt.Errorf("failed to scan DataHub URLs: %w", err)
	}
	defer func() { _ = rs.Close() }()

	var urls []string
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("scan error reading DataHub URLs: %w", res.Err)
		}
		if res.Record == nil {
			continue
		}
		url, ok := res.Record.Bins[dataHubURLBin].(string)
		if !ok || url == "" {
			continue
		}
		urls = append(urls, url)
	}
	return urls, nil
}
