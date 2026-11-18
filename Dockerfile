# Build stage
FROM golang:1.27-alpine AS builder

# Build arguments for version injection
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG GIT_TREE_STATE=unknown
ARG BUILD_DATE=unknown
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Cache dependencies
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

# Build the binary with version information
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags="-X 'go.miloapis.net/rudderstack-provider/internal/version.Version=${VERSION}' \
              -X 'go.miloapis.net/rudderstack-provider/internal/version.GitCommit=${GIT_COMMIT}' \
              -X 'go.miloapis.net/rudderstack-provider/internal/version.GitTreeState=${GIT_TREE_STATE}' \
              -X 'go.miloapis.net/rudderstack-provider/internal/version.BuildDate=${BUILD_DATE}'" \
    -a -o manager ./cmd

RUN echo 'nonroot:x:65532:65532:nonroot:/home/nonroot:/sbin/nologin' > /workspace/passwd && \
    echo 'nonroot:x:65532:' > /workspace/group

# Runtime stage
FROM scratch

WORKDIR /
COPY --from=builder /workspace/passwd /etc/passwd
COPY --from=builder /workspace/group /etc/group
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
