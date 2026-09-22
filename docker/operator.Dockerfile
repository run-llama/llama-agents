# Build the operator binary
FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy the Go Modules manifests
COPY operator/go.mod operator/go.sum ./
RUN go mod download

# Copy the go source
COPY operator/cmd/ cmd/
COPY operator/api/ api/
COPY operator/internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
