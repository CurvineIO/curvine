# Use pre-cached compilation image as builder
FROM ghcr.io/curvineio/curvine-compile:build-cached AS builder

ARG GOPROXY
ARG TARGETARCH
ARG TARGETOS

# Install Go toolchain (build-cached image doesn't include Go)
# Support multi-architecture: amd64 and arm64
RUN GO_VERSION=1.23.4 && \
    case "${TARGETARCH:-amd64}" in \
        amd64) GO_ARCH="amd64" ;; \
        arm64) GO_ARCH="arm64" ;; \
        *) GO_ARCH="amd64" ;; \
    esac && \
    echo "Installing Go ${GO_VERSION} for ${GO_ARCH}..." && \
    wget -O go.tar.gz "https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" && \
    tar -C /usr/local -xzf go.tar.gz && \
    rm go.tar.gz && \
    echo "✓ Go ${GO_VERSION} installed successfully"

# Set environment variables for build tools
ENV PATH="/root/.cargo/bin:/usr/local/go/bin:/app/protoc/bin:/app/maven/bin:${PATH}"
ENV GOPATH=/go
ENV GOROOT=/usr/local/go
ENV GOPROXY=${GOPROXY:-https://goproxy.cn,direct}

WORKDIR /workspace

# Copy entire Curvine source code
COPY . /workspace/

# Build only required components: cli and fuse
# Skip WebUI and other unnecessary components
# Docker buildx uses QEMU for native compilation on each platform
RUN echo "Building curvine-cli and curvine-fuse..." && \
    echo "Target platform: ${TARGETOS:-linux}/${TARGETARCH:-amd64}" && \
    export CURVINE_HOME=/workspace && \
    mkdir -p /workspace/tmp && \
    export TMPDIR=/workspace/tmp && \
    echo "Using TMPDIR=$TMPDIR for compilation temporary files" && \
    echo "Running 'make build ARGS=\"-p cli -p fuse\"'..." && \
    make build ARGS="-p cli -p fuse" && \
    if [ ! -d "/workspace/build/dist" ]; then \
        echo "✗ ERROR: /workspace/build/dist not found after build" && \
        exit 1; \
    fi && \
    echo "✓ Build succeeded for cli and fuse components" && \
    rm -rf /workspace/tmp

# Build CSI binary
# Docker buildx sets TARGETARCH automatically (amd64, arm64, etc.)
# CGO_ENABLED=0 for static binary without C dependencies
RUN cd /workspace/curvine-csi && \
    echo "Building CSI for ${TARGETOS:-linux}/${TARGETARCH:-amd64}..." && \
    CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-amd64} \
    go build -o bin/csi main.go && \
    echo "✓ CSI binary built successfully"

# Create output directory and copy build artifacts
RUN mkdir -p /build-output && \
    echo "Copying build/dist contents to /build-output..." && \
    cp -r /workspace/build/dist/* /build-output/ && \
    cp /workspace/curvine-csi/bin/csi /build-output/bin/ && \
    echo "Build output summary:" && \
    echo "  bin/: $(ls -1 /build-output/bin 2>/dev/null | wc -l) files" && \
    echo "  lib/: $(ls -1 /build-output/lib 2>/dev/null | wc -l) files" && \
    echo "  conf/: $(ls -1 /build-output/conf 2>/dev/null | wc -l) files"

FROM rockylinux:9

# Install runtime dependencies
RUN dnf install -y \
    fuse3 \
    fuse3-libs \
    ca-certificates \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Create curvine directories
RUN mkdir -p /opt/curvine

# Copy only required binaries from builder stage
COPY --from=builder /build-output/lib/curvine-cli /opt/curvine/
COPY --from=builder /build-output/lib/curvine-fuse /opt/curvine/
COPY --from=builder /build-output/bin/csi /opt/curvine/

# Make binaries executable
RUN chmod +x /opt/curvine/curvine-cli && \
    chmod +x /opt/curvine/curvine-fuse && \
    chmod +x /opt/curvine/csi

# Set environment variables
ENV CURVINE_HOME=/opt/curvine
ENV PATH=$PATH:$CURVINE_HOME

WORKDIR /opt/curvine
