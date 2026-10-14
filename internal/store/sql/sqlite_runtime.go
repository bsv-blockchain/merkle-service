package sql

import (
	"fmt"
	"net/url"
	"strings"
)

// sqliteRuntimeDSN supplies connection-local locking defaults before sql.Open,
// so newly opened pool connections receive them too. Explicit settings win.
// A timeout lets concurrent writers wait; immediate transactions also avoid
// the sweeper's deferred read-to-write upgrade, which can fail without waiting.
func sqliteRuntimeDSN(dsn string) (string, error) {
	base, rawQuery, _ := strings.Cut(dsn, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		// A DSN can contain sensitive parameters. Do not include it in errors.
		return "", fmt.Errorf("invalid SQLite DSN query parameters")
	}
	haveTimeout := false
	for _, pragma := range query["_pragma"] {
		name := strings.TrimSpace(pragma)
		if i := strings.IndexAny(name, "(= \t\r\n"); i >= 0 {
			name = name[:i]
		}
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		if strings.EqualFold(name, "busy_timeout") {
			haveTimeout = true
		}
	}
	if !haveTimeout {
		query.Add("_pragma", "busy_timeout(5000)")
	}
	if query.Get("_txlock") == "" {
		query.Set("_txlock", "immediate")
	}
	return base + "?" + query.Encode(), nil
}
