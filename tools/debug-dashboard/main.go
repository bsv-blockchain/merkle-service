package main

import (
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

func main() {
	port := flag.Int("port", 9900, "Dashboard listen port")
	merkleAPI := flag.String("merkle-api", "http://localhost:8080", "Merkle-service API base URL")
	maxCallbacks := flag.Int("max-callbacks", 1000, "Maximum stored callbacks")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Load merkle-service config for Aerospike connection.
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	// Connect to Aerospike.
	asClient, err := store.NewAerospikeClient(
		cfg.Aerospike.Host,
		cfg.Aerospike.Port,
		cfg.Aerospike.Namespace,
		cfg.Aerospike.MaxRetries,
		cfg.Aerospike.RetryBaseMs,
		logger,
	)
	if err != nil {
		log.Fatal("failed to connect to Aerospike: ", err)
	}
	defer asClient.Close()

	regStore := store.NewRegistrationStore(
		asClient,
		cfg.Aerospike.SetName,
		cfg.Aerospike.MaxRetries,
		cfg.Aerospike.RetryBaseMs,
		cfg.Registry.MaxCallbacksPerTxID,
		logger,
	)

	// Parse templates: each page gets its own template set with the layout.
	templates, err := parseTemplates()
	if err != nil {
		log.Fatal("failed to parse templates: ", err)
	}

	callbackURL := fmt.Sprintf("http://localhost:%d/callbacks/receive", *port)

	h := &Handlers{
		callbackStore: NewCallbackStore(*maxCallbacks),
		txidTracker:   NewTxidTracker(),
		regStore:      regStore,
		templates:     templates,
		merkleAPIURL:  *merkleAPI,
		callbackURL:   callbackURL,
		logger:        logger,
	}

	mux := http.NewServeMux()

	// Dashboard pages.
	mux.HandleFunc("GET /", h.handleHome)
	mux.HandleFunc("POST /register", h.handleRegister)
	mux.HandleFunc("POST /lookup", h.handleLookup)
	mux.HandleFunc("GET /registrations", h.handleRegistrations)
	mux.HandleFunc("GET /callbacks", h.handleCallbacks)
	mux.HandleFunc("GET /stump", h.handleStump)

	// Callback receiver.
	mux.HandleFunc("POST /callbacks/receive", h.handleCallbackReceive)
	mux.HandleFunc("POST /callbacks/clear", h.handleCallbacksClear)

	// Wrap with logging middleware.
	handler := loggingMiddleware(logger, mux)

	addr := fmt.Sprintf(":%d", *port)
	logger.Info("starting debug dashboard",
		"addr", addr,
		"merkleAPI", *merkleAPI,
		"callbackURL", callbackURL,
		"aerospike", fmt.Sprintf("%s:%d/%s", cfg.Aerospike.Host, cfg.Aerospike.Port, cfg.Aerospike.Namespace),
	)

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal("server error: ", err)
	}
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		logger.Debug("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start).String(),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter

	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// parseTemplates creates a separate template set per page, each including the
// layout. This avoids the issue where all pages define {{define "content"}} and
// only the last-parsed one would win if all were in a single template set.
func parseTemplates() (map[string]*template.Template, error) {
	pages := []string{"home.html", "registrations.html", "callbacks.html", "stump.html"}
	templates := make(map[string]*template.Template, len(pages))

	layoutData, err := fs.ReadFile(templateFS, "templates/layout.html")
	if err != nil {
		return nil, fmt.Errorf("reading layout: %w", err)
	}

	for _, page := range pages {
		pageData, err := fs.ReadFile(templateFS, "templates/"+page)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", page, err)
		}

		tmpl, err := template.New(page).Parse(string(layoutData))
		if err != nil {
			return nil, fmt.Errorf("parsing layout for %s: %w", page, err)
		}
		if _, err := tmpl.Parse(string(pageData)); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", page, err)
		}

		templates[page] = tmpl
	}

	return templates, nil
}
