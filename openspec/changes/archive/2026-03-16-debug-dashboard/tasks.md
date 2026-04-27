## 1. Project Structure

- [x] 1.1 Create `tools/debug-dashboard/` directory
- [x] 1.2 Create `tools/debug-dashboard/main.go` with flag parsing (`-port`, `-merkle-api`), config loading, Aerospike connection, and HTTP server startup
- [x] 1.3 Create `tools/debug-dashboard/templates/` directory for HTML templates

## 2. In-Memory Stores

- [x] 2.1 Create `tools/debug-dashboard/store.go` with `CallbackEntry` struct (Timestamp, Body map, RawJSON) and `CallbackStore` (thread-safe ring buffer, max 1000 entries, Add/GetAll/Clear methods)
- [x] 2.2 Add `TrackedTxid` struct (Txid, RegisteredAt, CallbackURLs) and `TxidTracker` (thread-safe list, Add/GetAll/Lookup methods)

## 3. Callback Receiver

- [x] 3.1 Create `tools/debug-dashboard/handlers.go` with `handleCallbackReceive` — accepts POST `/callbacks/receive`, parses JSON body, stores in CallbackStore, returns 200
- [x] 3.2 Verify the callback receiver accepts the merkle-service callback format (txid, txids, status, stumpData, blockHash fields)

## 4. Registration Handler

- [x] 4.1 Add `handleRegister` — accepts POST from the registration form, validates txid (64 hex chars), POSTs to merkle-service `/watch` API with the dashboard's callback URL, adds to TxidTracker
- [x] 4.2 Add `handleLookup` — accepts GET/POST with a txid, queries Aerospike via `RegistrationStore.Get()`, displays results

## 5. HTML Templates

- [x] 5.1 Create `tools/debug-dashboard/templates/layout.html` — base layout with nav bar (Home, Registrations, Callbacks), simple CSS styling
- [x] 5.2 Create `tools/debug-dashboard/templates/home.html` — summary stats (tracked txids count, received callbacks count), registration form with auto-filled callback URL, txid lookup form
- [x] 5.3 Create `tools/debug-dashboard/templates/registrations.html` — table of tracked txids with callback URLs, registration time
- [x] 5.4 Create `tools/debug-dashboard/templates/callbacks.html` — reverse-chronological list of received callbacks showing timestamp, txid, status, full payload
- [x] 5.5 Embed templates using Go's `embed` package in main.go

## 6. Route Wiring

- [x] 6.1 Wire all routes in main.go: `GET /` (home), `POST /register` (register txid), `POST /lookup` (lookup txid), `GET /registrations` (list tracked), `GET /callbacks` (callback log), `POST /callbacks/receive` (callback receiver), `POST /callbacks/clear` (clear log)
- [x] 6.2 Add request logging middleware

## 7. Testing

- [x] 7.1 Test `CallbackStore` — Add, GetAll, capacity eviction, Clear, thread safety
- [x] 7.2 Test `handleCallbackReceive` — valid payload stored, returns 200
- [x] 7.3 Test txid validation (64 hex chars)

## 8. Build Verification

- [x] 8.1 Run `go build ./tools/debug-dashboard` to verify it compiles
- [x] 8.2 Run `go vet ./tools/debug-dashboard/...` to check for issues
- [x] 8.3 Run `go test ./tools/debug-dashboard/...` to verify tests pass
