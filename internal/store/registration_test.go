package store

import (
	"testing"
)

// TestParseCallbackEntries_DualRead verifies the legacy / new shape parser
// the Aerospike RegistrationStore uses on every Get / BatchGet. The reader
// must handle both:
//
//   - bare-string entries written by older deployments (token = "")
//   - {u: url, t: token} map entries written post-callback-token rollout
//
// This is the unit-level coverage for the rolling-deploy guarantee called
// out in the PR: existing in-flight registrations stay valid; nothing 401s
// during the rollout window. A real Aerospike round-trip is exercised in
// the integration suite.
func TestParseCallbackEntries_DualRead(t *testing.T) {
	t.Run("legacy bare-string entries decode with empty token", func(t *testing.T) {
		// Aerospike returns CDT lists as []interface{}; emulate that here so
		// the parse logic gets exactly what the live reader sees.
		legacy := []interface{}{
			"https://arcade.example/cb1",
			"https://arcade.example/cb2",
		}
		got := parseCallbackEntries(legacy)
		if len(got) != 2 {
			t.Fatalf("want 2 entries, got %d", len(got))
		}
		for i, want := range []string{"https://arcade.example/cb1", "https://arcade.example/cb2"} {
			if got[i].URL != want {
				t.Errorf("entry %d URL = %q, want %q", i, got[i].URL, want)
			}
			if got[i].Token != "" {
				t.Errorf("entry %d Token = %q, want empty (legacy entry)", i, got[i].Token)
			}
		}
	})

	t.Run("new map entries decode with token populated", func(t *testing.T) {
		newShape := []interface{}{
			map[interface{}]interface{}{
				callbackEntryURLKey:   "https://arcade.example/cb1",
				callbackEntryTokenKey: "tok-v1",
			},
			map[interface{}]interface{}{
				callbackEntryURLKey:   "https://arcade.example/cb2",
				callbackEntryTokenKey: "tok-v2",
			},
		}
		got := parseCallbackEntries(newShape)
		if len(got) != 2 {
			t.Fatalf("want 2 entries, got %d", len(got))
		}
		if got[0].URL != "https://arcade.example/cb1" || got[0].Token != "tok-v1" {
			t.Errorf("entry 0 = %+v", got[0])
		}
		if got[1].URL != "https://arcade.example/cb2" || got[1].Token != "tok-v2" {
			t.Errorf("entry 1 = %+v", got[1])
		}
	})

	t.Run("mixed legacy and new shapes coexist (mid-migration)", func(t *testing.T) {
		// This is the rolling-deploy case: a record that had a bare-string
		// entry pre-deploy, then got a new {u,t} entry appended after the
		// new code rolled out. (In practice writers always rewrite the
		// whole list, but the reader must still cope with whatever it
		// finds in case of partial deploys or data older than this PR.)
		mixed := []interface{}{
			"https://legacy.example/cb",
			map[interface{}]interface{}{
				callbackEntryURLKey:   "https://new.example/cb",
				callbackEntryTokenKey: "tok-v1",
			},
		}
		got := parseCallbackEntries(mixed)
		if len(got) != 2 {
			t.Fatalf("want 2 entries, got %d", len(got))
		}
		if got[0].URL != "https://legacy.example/cb" || got[0].Token != "" {
			t.Errorf("legacy entry = %+v, want url=https://legacy.example/cb token=\"\"", got[0])
		}
		if got[1].URL != "https://new.example/cb" || got[1].Token != "tok-v1" {
			t.Errorf("new entry = %+v", got[1])
		}
	})

	t.Run("map entry with missing token field decodes to empty token", func(t *testing.T) {
		// Defensive: forward-compat with hand-edited records or future
		// schema changes that drop the token field.
		partial := []interface{}{
			map[interface{}]interface{}{
				callbackEntryURLKey: "https://arcade.example/cb",
			},
		}
		got := parseCallbackEntries(partial)
		if len(got) != 1 || got[0].URL != "https://arcade.example/cb" || got[0].Token != "" {
			t.Fatalf("want one entry with empty token, got %+v", got)
		}
	})

	t.Run("map entry with empty url is skipped", func(t *testing.T) {
		bad := []interface{}{
			map[interface{}]interface{}{
				callbackEntryURLKey:   "",
				callbackEntryTokenKey: "tok",
			},
			map[interface{}]interface{}{
				callbackEntryURLKey:   "https://arcade.example/cb",
				callbackEntryTokenKey: "tok",
			},
		}
		got := parseCallbackEntries(bad)
		if len(got) != 1 || got[0].URL != "https://arcade.example/cb" {
			t.Fatalf("want only the non-empty entry, got %+v", got)
		}
	})

	t.Run("non-string non-map entries are silently skipped", func(t *testing.T) {
		// Forward-compat: don't propagate a parse error for an entry shape
		// we don't understand; just ignore it. A noisy log would do more
		// harm than good for a registry that fans out per-block.
		junk := []interface{}{
			42,
			[]byte("oops"),
			"https://arcade.example/cb",
		}
		got := parseCallbackEntries(junk)
		if len(got) != 1 || got[0].URL != "https://arcade.example/cb" {
			t.Fatalf("want only the string entry, got %+v", got)
		}
	})

	t.Run("nil and empty inputs yield nil", func(t *testing.T) {
		if got := parseCallbackEntries(nil); got != nil {
			t.Errorf("nil input → want nil, got %+v", got)
		}
		if got := parseCallbackEntries([]interface{}{}); got != nil {
			t.Errorf("empty input → want nil, got %+v", got)
		}
	})
}

// TestEncodeCallbackEntry_RoundTripThroughParser confirms the writer and
// reader agree on the on-wire shape: every entry encoded by the writer is
// recovered exactly by the reader.
func TestEncodeCallbackEntry_RoundTripThroughParser(t *testing.T) {
	cases := []CallbackEntry{
		{URL: "https://arcade.example/cb", Token: "tok-v1"},
		{URL: "https://arcade.example/cb-no-token", Token: ""},
	}
	encoded := make([]interface{}, 0, len(cases))
	for _, c := range cases {
		encoded = append(encoded, encodeCallbackEntry(c.URL, c.Token))
	}
	got := parseCallbackEntries(encoded)
	if len(got) != len(cases) {
		t.Fatalf("round-trip lost entries: want %d, got %d", len(cases), len(got))
	}
	for i, want := range cases {
		if got[i] != want {
			t.Errorf("round-trip mismatch at %d: got %+v, want %+v", i, got[i], want)
		}
	}
}
