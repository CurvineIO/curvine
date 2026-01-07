# Build stage - Use pre-built compile image with all toolchains
FROM ghcr.io/curvineio/curvine-compile:ubuntu24.04 AS builder

ARG GOPROXY

# Override ENTRYPOINT to ensure RUN commands execute correctly
# The compile image has ENTRYPOINT ["/bin/bash", "-c"] which can interfere with RUN
ENTRYPOINT []

# Set GOPROXY environment variable (Go is already installed in compile image)
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org}

# Ensure environment variables are set (from compile image)
# Go toolchain is already included: PATH="/usr/local/go/bin:...", GOPATH=/go, GOROOT=/usr/local/go
ENV PATH="/root/.cargo/bin:/app/protoc/bin:/app/maven/bin:/usr/local/go/bin:${PATH}"

# Set JAVA_HOME with multi-arch support (required for HDFS JNI compilation)
RUN ARCH=$(uname -m) && \
    case "${ARCH}" in \
        x86_64) JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-amd64" ;; \
        aarch64) \
            if [ -d "/usr/lib/jvm/java-8-openjdk-arm64" ]; then \
                JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-arm64"; \
            elif [ -d "/usr/lib/jvm/java-8-openjdk-aarch64" ]; then \
                JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-aarch64"; \
            else \
                JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-amd64"; \
            fi ;; \
        *) JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-amd64" ;; \
    esac && \
    echo "Detected architecture: ${ARCH}, setting JAVA_HOME: ${JAVA_HOME_VALUE}" && \
    if [ ! -d "$JAVA_HOME_VALUE" ]; then \
        echo "Warning: JAVA_HOME directory not found, using fallback..." && \
        JAVA_HOME_VALUE=$(update-alternatives --list java 2>/dev/null | head -1 | sed 's|/bin/java||') || JAVA_HOME_VALUE="/usr/lib/jvm/java-8-openjdk-amd64"; \
    fi && \
    # Create symlink for hdfs-sys JNI compatibility (it looks for jre/include)
    if [ -d "$JAVA_HOME_VALUE/include" ] && [ ! -d "$JAVA_HOME_VALUE/jre/include" ]; then \
        mkdir -p "$JAVA_HOME_VALUE/jre" && \
        ln -s ../include "$JAVA_HOME_VALUE/jre/include"; \
    fi && \
    # Verify Java is available
    export JAVA_HOME=$JAVA_HOME_VALUE && \
    java -version

WORKDIR /workspace

# Copy entire Curvine source code
COPY . /workspace/

# Pre-install WebUI dependencies and fix vue-cli-service module resolution
RUN if [ -d "/workspace/curvine-web/webui" ]; then \
        echo "Pre-installing WebUI dependencies..."; \
        cd /workspace/curvine-web/webui && \
        npm install --legacy-peer-deps && \
        echo "npm install completed" && \
        ln -s @vue/cli-service/package.json node_modules/package.json && \
        echo "Created symlink: node_modules/package.json -> @vue/cli-service/package.json"; \
    fi

# Patch build.sh to use direct vue-cli-service.js call
RUN if [ -f "/workspace/build/build.sh" ]; then \
        echo "Patching build.sh to fix vue-cli-service wrapper script issue..."; \
        sed -i 's/npm run build/node node_modules\/@vue\/cli-service\/bin\/vue-cli-service.js build/g' /workspace/build/build.sh; \
    fi

# Build Curvine from source
RUN echo "Building Curvine from source..." && \
    # Detect and set JAVA_HOME for HDFS JNI compilation
    ARCH=$(uname -m) && \
    case "${ARCH}" in \
        x86_64) export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" ;; \
        aarch64) \
            if [ -d "/usr/lib/jvm/java-8-openjdk-arm64" ]; then \
                export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-arm64"; \
            elif [ -d "/usr/lib/jvm/java-8-openjdk-aarch64" ]; then \
                export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-aarch64"; \
            else \
                export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"; \
            fi ;; \
        *) export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" ;; \
    esac && \
    echo "Using JAVA_HOME: ${JAVA_HOME} for HDFS JNI compilation" && \
    export CURVINE_HOME=/workspace && \
    mkdir -p /workspace/tmp && \
    export TMPDIR=/workspace/tmp && \
    echo "Using TMPDIR=$TMPDIR for compilation temporary files" && \
    echo "Running 'make all' with --skip-java-sdk (CSI doesn't need Java SDK)..." && \
    for attempt in 1 2 3; do \
        echo "Build attempt $attempt..." && \
        if make all ARGS='--skip-java-sdk --ufs opendal-s3 --ufs opendal-oss --ufs opendal-azblob --ufs opendal-gcs --ufs opendal-hdfs --ufs opendal-webhdfs --ufs oss-hdfs'; then \
            echo "✓ Build succeeded" && \
            break; \
        else \
            echo "✗ Build attempt $attempt failed" && \
            if [ $attempt -eq 3 ]; then \
                echo "✗ All build attempts failed" && \
                exit 1; \
            fi && \
            echo "Retrying... (attempt $((attempt + 1))/3)" && \
            sleep 10; \
        fi; \
    done && \
    if [ ! -d "/workspace/build/dist" ]; then \
        echo "✗ ERROR: /workspace/build/dist not found after build" && \
        exit 1; \
    fi && \
    rm -rf /workspace/tmp

# Build CSI binary
RUN cd /workspace/curvine-csi && \
    go build -o bin/csi main.go

# Create output directory and copy build artifacts
RUN mkdir -p /build-output && \
    echo "Copying build/dist contents to /build-output..." && \
    cp -r /workspace/build/dist/* /build-output/ && \
    cp /workspace/curvine-csi/bin/csi /build-output/bin/ && \
    echo "Build output summary:" && \
    echo "  bin/: $(ls -1 /build-output/bin 2>/dev/null | wc -l) files" && \
    echo "  lib/: $(ls -1 /build-output/lib 2>/dev/null | wc -l) files" && \
    echo "  conf/: $(ls -1 /build-output/conf 2>/dev/null | wc -l) files"

FROM ubuntu:24.04

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    libfuse3-3 \
    ca-certificates \
    openjdk-8-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy JindoSDK from builder stage for OSS-HDFS support
COPY --from=builder /opt/jindosdk /opt/jindosdk

ENV JINDOSDK_HOME=/opt/jindosdk
ENV LD_LIBRARY_PATH="${JINDOSDK_HOME}/lib/native:${JINDOSDK_HOME}/lib"

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
