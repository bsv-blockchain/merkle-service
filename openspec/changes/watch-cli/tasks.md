## 1. Package Scaffold

- [x] 1.1 Create `cmd/watch/main.go` with `package main` and an empty `main()` function
- [x] 1.2 Define flag variables: `flagURL`, `flagCallback`, `flagTxid`, `flagFile`, `flagTimeout` (default 10s), `flagConcurrency` (default 10), `flagQuiet`, `flagVerbose`
- [x] 1.3 Register all flags with `flag.StringVar` / `flag.DurationVar` / `flag.IntVar` / `flag.BoolVar` and descriptive usage strings
- [x] 1.4 Add `usage()` helper that prints a usage summary and calls `flag.PrintDefaults()`; set `flag.Usage = usage`

## 2. Input Validation

- [x] 2.1 Add `validateTxid(s string) error` that checks `len == 64` and all chars are hex; return descriptive error otherwise
- [x] 2.2 Add `loadTxids(path string) ([]string, error)` that opens `path` (or stdin if `path == "-"`), reads line by line, skips blank lines and `#`-prefixed comments, and returns the txid slice
- [x] 2.3 In `loadTxids`, call `validateTxid` for each line and collect all validation errors; return them all at once so the user sees every bad line before any HTTP request is sent
- [x] 2.4 In `main()`, validate that `--url` and `--callback` are both non-empty after `flag.Parse()`; print usage and `os.Exit(2)` if either is missing
- [x] 2.5 Validate that exactly one of `--txid` or `--file` is provided (not both, not neither); print usage and `os.Exit(2)` otherwise

## 3. HTTP Client and Register Function

- [x] 3.1 Construct an `*http.Client` with `Timeout` set to `flagTimeout`
- [x] 3.2 Define `type watchRequest struct { TxID string \`json:"txid"\`; CallbackURL string \`json:"callbackUrl"\` }`
- [x] 3.3 Define `registerOne(ctx context.Context, client *http.Client, apiURL, txid, callbackURL string) error` that marshals a `watchRequest`, POSTs to `apiURL + "/watch"`, reads the response, and returns an error if the status is not 200 (including the response body in the error message)
- [x] 3.4 In `registerOne`, if `flagVerbose` is set, log the request URL and response status to stderr

## 4. Single Txid Mode

- [x] 4.1 When `--txid` is set: validate with `validateTxid`, call `registerOne`, print `OK <txid>` on success or `FAIL <txid>: <err>` on failure
- [x] 4.2 Exit with code 0 on success, code 1 on failure

## 5. Bulk File Mode

- [x] 5.1 When `--file` is set: call `loadTxids` (which validates all txids); on any validation error print all errors and `os.Exit(2)`
- [x] 5.2 Create a semaphore channel of size `flagConcurrency` to bound concurrent requests
- [x] 5.3 For each txid, acquire the semaphore, launch a goroutine that calls `registerOne`, records OK/FAIL result, and releases the semaphore
- [x] 5.4 Use a `sync.WaitGroup` to wait for all goroutines to finish
- [x] 5.5 Collect results into a slice (using a mutex or buffered channel) preserving each txid's outcome
- [x] 5.6 After all goroutines complete, print per-txid output unless `--quiet` is set: `OK <txid>` or `FAIL <txid>: <err>`
- [x] 5.7 Print summary line `registered N/M` (always, even without `--quiet`)
- [x] 5.8 Exit with code 0 if all succeeded, code 1 if any failed

## 6. Tests

- [x] 6.1 Create `cmd/watch/main_test.go` with `package main`
- [x] 6.2 Test `validateTxid`: valid 64-char hex returns nil; all-zeros, mixed case pass; 63 chars, 65 chars, non-hex char return error
- [x] 6.3 Test `loadTxids` with a temp file: valid lines returned, blank lines and `#` comments skipped, invalid txid returns error listing the bad line
- [x] 6.4 Test `loadTxids` with stdin (`path == "-"`) using a `strings.NewReader` pipe
- [x] 6.5 Test `registerOne` with an `httptest.NewServer` that returns 200: no error returned
- [x] 6.6 Test `registerOne` with a server that returns 400 with JSON body: error contains the response body message
- [x] 6.7 Test `registerOne` with a server that returns 500: error returned

## 7. Build Integration

- [x] 7.1 Add `RUN CGO_ENABLED=0 go build -o /bin/watch ./cmd/watch` to `Dockerfile`
- [x] 7.2 Run `go build ./cmd/watch/...` and confirm it compiles
- [x] 7.3 Run `go test ./cmd/watch/...` and confirm all tests pass
- [x] 7.4 Run `go test ./...` to confirm no regressions in other packages
