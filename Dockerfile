# Multi-stage Dockerfile for merkle-service binaries.
# Build all service binaries from a single image, then copy into minimal runtime images.

FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build all service binaries.
RUN CGO_ENABLED=0 go build -o /bin/merkle-service ./cmd/merkle-service
RUN CGO_ENABLED=0 go build -o /bin/block-processor ./cmd/block-processor
RUN CGO_ENABLED=0 go build -o /bin/subtree-worker ./cmd/subtree-worker
RUN CGO_ENABLED=0 go build -o /bin/callback-delivery ./cmd/callback-delivery
RUN CGO_ENABLED=0 go build -o /bin/subtree-fetcher ./cmd/subtree-fetcher
RUN CGO_ENABLED=0 go build -o /bin/api-server ./cmd/api-server
RUN CGO_ENABLED=0 go build -o /bin/p2p-client ./cmd/p2p-client
RUN CGO_ENABLED=0 go build -o /bin/watch ./cmd/watch

# Runtime image with all binaries.
FROM alpine:3.19

RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ /usr/local/bin/

ENTRYPOINT ["merkle-service"]
