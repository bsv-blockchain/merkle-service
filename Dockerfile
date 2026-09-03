# Multi-stage Dockerfile for merkle-service binaries.
# Build all service binaries from a single image, then copy into minimal runtime images.

# Pinned by multi-arch manifest-list (OCI image index) digest so the digest
# resolves the correct per-platform manifest on every build OS/arch.
FROM golang:1.27-alpine@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS builder

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
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ /usr/local/bin/

ENTRYPOINT ["merkle-service"]
