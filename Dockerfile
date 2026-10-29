# Multi-stage Dockerfile for merkle-service binaries.
# Build all service binaries from a single image, then copy into minimal runtime images.

# Pinned by multi-arch manifest-list (OCI image index) digest so the digest
# resolves the correct per-platform manifest on every build OS/arch.
FROM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder

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
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ /usr/local/bin/

ENTRYPOINT ["merkle-service"]
