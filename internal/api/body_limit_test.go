package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
)

const bodyLimitTxID = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

// An oversized /watch body is refused with 413 before anything is stored (#56).
func TestHandleWatch_OversizedBodyRejected(t *testing.T) {
	rs := &fakeRegStore{}
	router := newTestRouterWithRegStore(rs)

	body := fmt.Sprintf(`{"txid":%q,"callbackUrl":"https://1.1.1.1/cb","pad":%q}`, bodyLimitTxID, strings.Repeat("a", 1<<20))
	w := postWatch(router, body)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(rs.added) != 0 {
		t.Fatalf("oversized request must not register, got %d registrations", len(rs.added))
	}
}

// The largest legitimate /watch request, a maximum-length callbackToken and a
// long callback URL, still fits under the body limit.
func TestHandleWatch_LargeLegitimateBodyAccepted(t *testing.T) {
	rs := &fakeRegStore{}
	router := newTestRouterWithRegStore(rs)

	callbackURL := "https://1.1.1.1/cb?q=" + strings.Repeat("b", 8<<10)
	token := strings.Repeat("t", maxCallbackTokenLen)
	body := fmt.Sprintf(`{"txid":%q,"callbackUrl":%q,"callbackToken":%q}`, bodyLimitTxID, callbackURL, token)
	w := postWatch(router, body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(rs.added) != 1 {
		t.Fatalf("expected 1 registration, got %d", len(rs.added))
	}
}

// An oversized /reprocess body is refused with 413 before anything is
// published (#56).
func TestHandleReprocess_OversizedBodyRejected(t *testing.T) {
	prod := &recordingProducer{}
	s := newReprocessServer(t, &ReprocessDeps{DataHubClient: datahub.NewClient(5, 0, discardLogger())})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb","pad":%q}`, fixtureBlockHash, strings.Repeat("a", 1<<20))
	w := postReprocess(router, body)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(prod.keys) != 0 {
		t.Fatalf("oversized request must not enqueue, got %d publishes", len(prod.keys))
	}
}
