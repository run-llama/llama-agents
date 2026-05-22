# Build the statedb binary
FROM golang:1.24-alpine AS builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY operator/go.mod operator/go.sum ./
RUN go mod download

# Copy the go source
COPY operator/cmd/ cmd/
COPY operator/api/ api/
COPY operator/internal/ internal/

# Build
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o statedb ./cmd/statedb

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /workspace/statedb /usr/local/bin/statedb
USER 1001:1001

ENTRYPOINT ["statedb"]
