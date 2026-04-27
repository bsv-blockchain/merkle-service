## Context

The debug dashboard's registration form currently hardcodes the dashboard's own callback URL (`http://localhost:9900/callbacks/receive`). The `handleRegister` handler builds a JSON payload with only `txid` and this fixed callback URL, then POSTs to the merkle-service `/watch` endpoint.

The merkle-service `/watch` API accepts `{"txid": "...", "callbackUrl": "..."}` and the underlying `RegistrationStore` supports multiple callback URLs per txid (stored as a list in Aerospike). The dashboard's `TxidTracker` already supports multiple callback URLs per txid via its `Add` method.

## Goals / Non-Goals

**Goals:**
- Make the callback URL field editable in the registration form
- Default the callback URL to the dashboard's own receiver for convenience
- Track all registered callback URLs per txid in the dashboard's local state
- Display registered callback URLs accurately on the registrations page

**Non-Goals:**
- Batch registration of multiple txids at once
- Registering multiple callback URLs in a single form submission (users can submit the form multiple times)
- Modifying the merkle-service `/watch` API itself

## Decisions

**1. Editable callback URL field with pre-filled default**

The callback URL input changes from `readonly` to editable, pre-filled with the dashboard's receiver URL. This preserves the current one-click experience while allowing custom URLs.

Alternative: Separate form for custom callback URLs. Rejected — adds unnecessary UI complexity for a simple field change.

**2. Single callback URL per registration submission**

Each form submission registers one txid + one callback URL, matching the merkle-service `/watch` API's existing contract. To register multiple callbacks for the same txid, submit the form multiple times.

Alternative: Multi-URL input with comma separation. Rejected — adds parsing complexity and diverges from the `/watch` API's single-URL contract.

## Risks / Trade-offs

- [Custom callback URL may be unreachable] → Not our concern — the dashboard just forwards the registration to merkle-service. The user is responsible for the callback endpoint being reachable.
- [User may accidentally clear the default URL] → The form pre-fills the dashboard URL on each page load, so refreshing restores it.
