package p2p

import (
	"strings"

	teranode "github.com/bsv-blockchain/teranode/services/p2p"
)

// pickDataHubURL returns the URL peers should be contacted at for fetching
// block and subtree data, derived from a NodeStatusMessage broadcast over
// the node_status topic. PropagationURL wins when present (it is the URL
// the peer explicitly advertises for cross-peer interaction); BaseURL is
// the fallback per the teranode protocol. Empty string means the peer has
// not advertised a usable endpoint yet and should be ignored. Whitespace
// is trimmed before evaluation so a peer advertising "   " is treated as
// empty.
//
// This mirrors arcade's behavior so peers register identically in both
// services' discovery layers.
func pickDataHubURL(m teranode.NodeStatusMessage) string {
	if u := strings.TrimSpace(m.PropagationURL); u != "" {
		return u
	}
	return strings.TrimSpace(m.BaseURL)
}

// normalizeDataHubURL strips a single trailing slash from a discovered URL
// so URLs differing only by trailing slash deduplicate in the registry.
// Scheme/host validation lives in ssrfguard.ValidateURL; this function is
// intentionally a no-op beyond the trim.
func normalizeDataHubURL(raw string) string {
	return strings.TrimSuffix(strings.TrimSpace(raw), "/")
}
