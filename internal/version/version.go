// Package version exposes the merkle-service build version.
package version

// Version is the merkle-service build version. It defaults to "dev" for
// local builds and `go run`, and is overridden at container build time via
//
//	-ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=<tag>"
//
// (see the Dockerfile's ARG VERSION and go build lines).
var Version = "dev"
