// Package metrics provides Prometheus instrumentation for merkle-service.
//
// Cardinality policy (load-bearing — reviewers must reject violations):
//   - URL-derived labels MUST be hostname-only via HostLabel. Never full URL.
//   - HTTP route labels MUST use chi RoutePattern, never the raw path.
//   - txid, blockHash, peer ID, and other unbounded identifiers are NEVER labels.
//   - outcome / op / store / topic labels MUST use the bounded enum constants
//     declared in this package.
//
// All metrics are registered against the private Registry, not
// prometheus.DefaultRegisterer, so transitively-registered metrics from other
// packages do not collide with ours and our /metrics endpoint exposes only
// merkle-service series plus the Go and process collectors.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the private Prometheus registry used by merkle-service. All
// metric Vars in this package are registered here, and the metrics HTTP
// handler serves only this registry.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}
