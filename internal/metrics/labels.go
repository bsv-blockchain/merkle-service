package metrics

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
)

// Outcome label constants. Use these everywhere the `outcome` label is set
// so the values stay a bounded enum and don't drift across packages.
const (
	OutcomeSuccess          = "success"
	OutcomeError            = "error"
	OutcomeTimeout          = "timeout"
	OutcomeNotFound         = "not_found"
	OutcomeDLQ              = "dlq"
	OutcomeRetry            = "retry"
	OutcomeRetried          = "retried"
	OutcomeProcessed        = "processed"
	OutcomeSkippedUnhealthy = "skipped_unhealthy"
	OutcomeDecodeError      = "decode_error"
	OutcomePermanentFailure = "permanent_failure"
	OutcomeDelivered        = "delivered"
	OutcomeDedupHit         = "dedup_hit"
	OutcomeRetryScheduled   = "retry_scheduled"
	OutcomeStumpNotFound    = "stump_not_found"
	OutcomePermanent4xx     = "permanent_4xx"
	OutcomeHit              = "hit"
	OutcomeMiss             = "miss"
	OutcomeEmpty            = "empty"
	OutcomeHandlerError     = "handler_error"
	OutcomeRejectedURL      = "rejected_url"
	OutcomePublished        = "published"
)

// Unknown is the fallback label value when an input cannot be classified
// without risking unbounded cardinality.
const Unknown = "unknown"

// Label name constants. Used as the second argument to NewCounterVec /
// NewHistogramVec / NewGaugeVec so the spelling stays consistent and the
// goconst linter doesn't flag repeated string literals.
const (
	labelOutcome      = "outcome"
	labelBackend      = "backend"
	labelStore        = "store"
	labelOp           = "op"
	labelTopic        = "topic"
	labelGroup        = "group"
	labelCallbackHost = "callback_host"
	labelPeerHost     = "peer_host"
	labelKind         = "kind"
	labelDirection    = "direction"
	labelStatusClass  = "status_class"
	labelRoute        = "route"
	labelMethod       = "method"
	labelStatus       = "status"
)

// maxHostLen caps the host label length defensively. A label value longer
// than the DNS limit cannot be a real hostname; refusing it keeps a single
// degenerate input from spawning its own metric series.
const maxHostLen = 253

// HostLabel returns a bounded hostname label for a URL. Lowercased,
// port-stripped. Returns Unknown ("unknown") for empty input, parse
// failures, missing host, or absurdly long values.
//
// This is the single sanctioned way to derive a label from a URL — never
// use the full URL, the path, or query string as a label.
func HostLabel(rawURL string) string {
	s := strings.TrimSpace(rawURL)
	if s == "" {
		return Unknown
	}
	if len(s) > 4096 {
		return Unknown
	}

	// If the input lacks a scheme, prepend one so url.Parse pulls out
	// the host instead of treating the whole string as a path.
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}

	u, err := url.Parse(s)
	if err != nil {
		return Unknown
	}
	host := u.Hostname()
	if host == "" {
		return Unknown
	}
	host = strings.ToLower(host)
	if len(host) > maxHostLen {
		return Unknown
	}
	return host
}

// StatusClass maps an HTTP status code (or an error) to a bounded class
// label: "2xx", "3xx", "4xx", "5xx", "timeout", "error".
//
// When err is non-nil it takes precedence over the code so transport
// failures don't masquerade as status 0.
func StatusClass(code int, err error) string {
	if err != nil {
		if isTimeoutErr(err) {
			return OutcomeTimeout
		}
		return OutcomeError
	}
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return OutcomeError
	}
}

func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}
