package store

import (
	"testing"
)

// TestCallbackURLRegistryKey verifies the constant values are set correctly.
func TestCallbackURLRegistryKey(t *testing.T) {
	if callbackURLRegistryKey != "__all_urls__" {
		t.Errorf("expected __all_urls__, got %s", callbackURLRegistryKey)
	}
	if callbackURLsBin != "urls" {
		t.Errorf("expected urls, got %s", callbackURLsBin)
	}
}

// TestNewCallbackURLRegistry verifies constructor sets fields.
func TestNewCallbackURLRegistry(t *testing.T) {
	r := NewCallbackURLRegistry(nil, "test-set", 3, 100, nil).(*aerospikeCallbackURLRegistry)
	if r.setName != "test-set" {
		t.Errorf("expected set name test-set, got %s", r.setName)
	}
	if r.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", r.maxRetries)
	}
	if r.retryBaseMs != 100 {
		t.Errorf("expected retryBaseMs 100, got %d", r.retryBaseMs)
	}
}
