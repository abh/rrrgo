# Stage 1: Build
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build both binaries
# Use -ldflags to strip debug info and set version
ARG VERSION=dev-snapshot
RUN go build \
    -ldflags="-w -s -X go.ntppool.org/common/version.VERSION=${VERSION}" \
    -o rrr-server ./cmd/rrr-server

RUN go build \
    -ldflags="-w -s -X go.ntppool.org/common/version.VERSION=${VERSION}" \
    -o rrr-fsck ./cmd/rrr-fsck

# Stage 2: Runtime
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 rrr && \
    adduser -D -u 1000 -G rrr rrr

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/rrr-server /app/
COPY --from=builder /build/rrr-fsck /app/

# Create data directory with proper permissions
RUN mkdir -p /data && chown rrr:rrr /data

USER rrr

# Expose metrics port (default 9090 for rrr-server)
EXPOSE 9090

# Default to running rrr-server
# Override with docker run ... /app/rrr-fsck for fsck operations
ENTRYPOINT ["/app/rrr-server"]
CMD ["/data"]
