# Multi-stage Dockerfile for merkle-service binaries.
# Build all service binaries from a single image, then copy into minimal runtime images.

# Pinned by multi-arch manifest-list (OCI image index) digest so the digest
# resolves the correct per-platform manifest on every build OS/arch.
FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

ARG VERSION=dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build all service binaries.
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/merkle-service ./cmd/merkle-service
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/block-processor ./cmd/block-processor
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/subtree-worker ./cmd/subtree-worker
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/callback-delivery ./cmd/callback-delivery
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/subtree-fetcher ./cmd/subtree-fetcher
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/api-server ./cmd/api-server
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/p2p-client ./cmd/p2p-client
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/bsv-blockchain/merkle-service/internal/version.Version=${VERSION}" -o /bin/watch ./cmd/watch

# Runtime image with all binaries.
FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ /usr/local/bin/

ENTRYPOINT ["merkle-service"]
