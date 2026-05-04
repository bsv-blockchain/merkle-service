package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	flagURL         string
	flagCallback    string
	flagTxid        string
	flagFile        string
	flagTimeout     time.Duration
	flagConcurrency int
	flagQuiet       bool
	flagVerbose     bool
)

func init() {
	flag.StringVar(&flagURL, "url", "", "merkle-service API base URL (required, e.g. http://localhost:8080)")
	flag.StringVar(&flagCallback, "callback", "", "callback URL to register for all txids (required)")
	flag.StringVar(&flagTxid, "txid", "", "single 64-char hex txid to register")
	flag.StringVar(&flagFile, "file", "", "path to newline-delimited txid file (use - for stdin)")
	flag.DurationVar(&flagTimeout, "timeout", 10*time.Second, "HTTP request timeout per registration")
	flag.IntVar(&flagConcurrency, "concurrency", 10, "maximum concurrent HTTP requests (bulk mode)")
	flag.BoolVar(&flagQuiet, "quiet", false, "suppress per-txid output; print only final summary")
	flag.BoolVar(&flagVerbose, "verbose", false, "log HTTP request and response details to stderr")

	flag.Usage = usage
}

func usage() {
	fmt.Fprintln(os.Stderr, "watch — register transactions with the merkle-service API")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  watch --url <url> --callback <url> --txid <hash>")
	fmt.Fprintln(os.Stderr, "  watch --url <url> --callback <url> --file <path>")
	fmt.Fprintln(os.Stderr, "  watch --url <url> --callback <url> --file -   (read from stdin)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Flags:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Exit codes: 0=success, 1=registration failure, 2=bad flags/input")
}

// watchRequest mirrors the API's POST /watch body.
type watchRequest struct {
	TxID        string `json:"txid"`
	CallbackURL string `json:"callbackUrl"`
}

// result holds the outcome of a single registration attempt.
type result struct {
	txid string
	err  error
}

func main() {
	flag.Parse()

	// Validate required flags.
	if flagURL == "" || flagCallback == "" {
		fmt.Fprintln(os.Stderr, "error: --url and --callback are required")
		usage()
		os.Exit(2)
	}

	// Validate that exactly one of --txid or --file is set.
	if flagTxid == "" && flagFile == "" {
		fmt.Fprintln(os.Stderr, "error: one of --txid or --file is required")
		usage()
		os.Exit(2)
	}
	if flagTxid != "" && flagFile != "" {
		fmt.Fprintln(os.Stderr, "error: --txid and --file are mutually exclusive")
		usage()
		os.Exit(2)
	}

	// Validate --concurrency: must be >= 1. A zero value would create an
	// unbuffered semaphore that never lets goroutines proceed (wg.Wait hangs);
	// a negative value would panic inside make().
	if err := validateConcurrency(flagConcurrency); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		usage()
		os.Exit(2)
	}

	client := &http.Client{Timeout: flagTimeout}
	ctx := context.Background()

	if flagTxid != "" {
		runSingle(ctx, client)
	} else {
		runBulk(ctx, client)
	}
}

func runSingle(ctx context.Context, client *http.Client) {
	if err := validateTxid(flagTxid); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid --txid: %v\n", err)
		os.Exit(2)
	}

	if err := registerOne(ctx, client, flagURL, flagTxid, flagCallback); err != nil {
		fmt.Printf("FAIL %s: %v\n", flagTxid, err)
		os.Exit(1)
	}
	fmt.Printf("OK %s\n", flagTxid)
}

func runBulk(ctx context.Context, client *http.Client) {
	txids, err := loadTxids(flagFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	if len(txids) == 0 {
		fmt.Println("registered 0/0")
		return
	}

	sem := make(chan struct{}, flagConcurrency)
	results := make([]result, len(txids))
	var wg sync.WaitGroup

	for i, txid := range txids {
		wg.Add(1)
		go func(i int, txid string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			err := registerOne(ctx, client, flagURL, txid, flagCallback)
			results[i] = result{txid: txid, err: err}
		}(i, txid)
	}

	wg.Wait()

	// Print per-txid output and tally.
	succeeded := 0
	for _, r := range results {
		if r.err == nil {
			succeeded++
			if !flagQuiet {
				fmt.Printf("OK %s\n", r.txid)
			}
		} else {
			if !flagQuiet {
				fmt.Printf("FAIL %s: %v\n", r.txid, r.err)
			}
		}
	}

	fmt.Printf("registered %d/%d\n", succeeded, len(txids))

	if succeeded < len(txids) {
		os.Exit(1)
	}
}

// validateConcurrency returns an error if n is not a usable concurrency
// value. The semaphore in runBulk is sized from this value; a zero value
// produces an unbuffered channel that deadlocks wg.Wait, and a negative
// value panics inside make().
func validateConcurrency(n int) error {
	if n < 1 {
		return fmt.Errorf("--concurrency must be >= 1")
	}
	return nil
}

// validateTxid returns an error if s is not a 64-character hex string.
func validateTxid(s string) error {
	if len(s) != 64 {
		return fmt.Errorf("must be exactly 64 hex characters, got %d", len(s))
	}
	for i, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return fmt.Errorf("invalid character %q at position %d", c, i)
		}
	}
	return nil
}

// loadTxids reads txids from a file path (or stdin if path is "-").
// Blank lines and lines starting with "#" are skipped.
// All txids are validated before returning; all errors are collected.
func loadTxids(path string) ([]string, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path) //nolint:gosec // path comes from CLI flag, not user HTTP input
		if err != nil {
			return nil, fmt.Errorf("cannot open file: %w", err)
		}
		defer func() { _ = f.Close() }()
		r = f
	}

	var txids []string
	var errs []string
	lineNum := 0

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if err := validateTxid(line); err != nil {
			errs = append(errs, fmt.Sprintf("  line %d %q: %v", lineNum, line, err))
			continue
		}
		txids = append(txids, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading input: %w", err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid txids found:\n%s", strings.Join(errs, "\n"))
	}

	return txids, nil
}

// registerOne posts a single txid registration to the API.
func registerOne(ctx context.Context, client *http.Client, apiURL, txid, callbackURL string) error {
	reqBody, err := json.Marshal(watchRequest{TxID: txid, CallbackURL: callbackURL})
	if err != nil {
		return fmt.Errorf("encoding request: %w", err)
	}

	target := strings.TrimRight(apiURL, "/") + "/watch"

	if flagVerbose {
		fmt.Fprintf(os.Stderr, "POST %s txid=%s\n", target, txid)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if flagVerbose {
		fmt.Fprintf(os.Stderr, "  → %d\n", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}
