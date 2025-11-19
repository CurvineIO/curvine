# Build stage
FROM ubuntu:24.04 AS builder

# Add metadata labels
ARG REPO_URL=https://github.com/curvineio/curvine
LABEL org.opencontainers.image.source="${REPO_URL}"
LABEL org.opencontainers.image.description="Curvine Native Kubernetes Runtime Image"

# This Dockerfile is specifically for source code compilation

# Set non-interactive mode for apt to avoid prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# Use Alibaba Cloud mirror for faster package downloads in China
RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup 2>/dev/null || true && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse' > /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse' >> /etc/apt/sources.list && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 1. Install the basic toolchain and dependencies
RUN apt-get update && apt-get install -y --fix-missing \
    fuse3 \
    libfuse3-dev \
    clang \
    llvm \
    llvm-dev \
    git \
    wget \
    curl \
    zip \
    unzip \
    nodejs \
    npm \
    openjdk-8-jdk \
    libssl-dev \
    pkg-config \
    ca-certificates \
    rsync \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install the Rust toolchain
RUN export PATH="/root/.cargo/bin:${PATH}" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
    . $HOME/.cargo/env && \
    rustup default stable && \
    rustup component add rustfmt clippy

# 3. Install protoc
RUN mkdir -p /app/protoc && cd /app/protoc && \
    curl -k -LO https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protoc-27.2-linux-x86_64.zip && \
    unzip protoc-27.2-linux-x86_64.zip && \
    chmod +x bin/protoc && \
    rm -f *.zip

# 4. Install Maven
RUN cd /app && \
    curl -k -LO https://downloads.apache.org/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz && \
    tar -xzf apache-maven-3.9.11-bin.tar.gz && \
    mv apache-maven-3.9.11 maven && \
    rm -f *.tar.gz

# 5. Copy and apply build configurations
COPY compile/ /build-config/
RUN mkdir -p /app/maven/conf && \
    cp /build-config/settings.xml /app/maven/conf/settings.xml && \
    mkdir -p /root/.cargo && \
    cp /build-config/config /root/.cargo/config.toml

# Set environment variables for build
ENV PATH="/root/.cargo/bin:/app/protoc/bin:/app/maven/bin:${PATH}"

# Copy and build source code if building from source
COPY workspace/ /workspace/
WORKDIR /workspace

# 6. Pre-install WebUI dependencies and fix vue-cli-service module resolution
RUN if [ -d "/workspace/curvine-web/webui" ]; then \
        echo "Pre-installing WebUI dependencies..."; \
        cd /workspace/curvine-web/webui && \
        npm install --legacy-peer-deps && \
        echo "npm install completed" && \
        # Fix: vue-cli-service wrapper script expects node_modules/package.json
        # Create symlink to @vue/cli-service/package.json
        ln -s @vue/cli-service/package.json node_modules/package.json && \
        echo "Created symlink: node_modules/package.json -> @vue/cli-service/package.json" && \
        ls -la node_modules/package.json; \
    fi

# 7. Patch build.sh to use direct vue-cli-service.js call
RUN if [ -f "/workspace/build/build.sh" ]; then \
        echo "Patching build.sh to fix vue-cli-service wrapper script issue..."; \
        sed -i 's/npm run build/node node_modules\/@vue\/cli-service\/bin\/vue-cli-service.js build/g' /workspace/build/build.sh && \
        echo "Patch applied" && \
        grep -n "vue-cli-service" /workspace/build/build.sh || true; \
    fi

# 8. Build the project from source
RUN echo "Building Curvine from source..." && \
    export CURVINE_HOME=/workspace && \
    # Use workspace tmp directory instead of /tmp to avoid "no space left on device"
    mkdir -p /workspace/tmp && \
    export TMPDIR=/workspace/tmp && \
    echo "Using TMPDIR=$TMPDIR for compilation temporary files" && \
    echo "Running 'make all' with retry mechanism..." && \
    # Build with retry mechanism for Maven failures
    for attempt in 1 2 3; do \
        echo "Build attempt $attempt..." && \
        if make all; then \
            echo "✓ Build succeeded with 'make all'" && \
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
    # Clean up temporary files to reduce image size
    rm -rf /workspace/tmp

# 9. Create output directory and copy build artifacts from build/dist
RUN mkdir -p /build-output && \
    echo "Copying build/dist contents to /build-output..." && \
    cp -r /workspace/build/dist/* /build-output/ && \
    # Verify critical files exist
    if [ ! -f "/build-output/bin/launch-process.sh" ]; then \
        echo "✗ ERROR: launch-process.sh not found in build/dist/bin" && \
        exit 1; \
    fi && \
    if [ ! -f "/build-output/lib/curvine-server" ]; then \
        echo "✗ ERROR: curvine-server not found in build/dist/lib" && \
        exit 1; \
    fi && \
    echo "Build output summary:" && \
    echo "  bin/: $(ls -1 /build-output/bin 2>/dev/null | wc -l) files" && \
    echo "  lib/: $(ls -1 /build-output/lib 2>/dev/null | wc -l) files" && \
    echo "  conf/: $(ls -1 /build-output/conf 2>/dev/null | wc -l) files"

# Runtime stage
FROM ubuntu:24.04

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Use Alibaba Cloud mirror for faster package downloads in China
RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup 2>/dev/null || true && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse' > /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse' >> /etc/apt/sources.list && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install runtime dependencies
RUN apt-get update && apt-get install -y --fix-missing \
    python3 \
    fuse3 \
    libfuse3-3 \
    libfuse3-dev \
    curl \
    bash \
    openjdk-8-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/*

# Create curvine directories
RUN mkdir -p /app/curvine \
    && mkdir -p /opt/curvine/data/{meta,journal} \
    && mkdir -p /opt/curvine/logs

# Always create the base directory structure
RUN mkdir -p /app/curvine/bin /app/curvine/lib /app/curvine/conf

# Copy from builder stage when building from source
COPY --from=builder /build-output/ /app/curvine/

# Copy entrypoint script (use compile-specific entrypoint)
COPY entrypoint.sh /entrypoint.sh

# Make all binaries and scripts executable
RUN find /app/curvine -type f -name "*.sh" -exec chmod +x {} \; \
    && find /app/curvine -type f -path "*/bin/*" -exec chmod +x {} \; \
    && find /app/curvine -type f -path "*/lib/*" -exec chmod +x {} \; \
    && chmod +x /entrypoint.sh

# Set environment variables
ENV APP_HOME=/app
ENV CURVINE_HOME=/app/curvine
ENV PATH=$PATH:$CURVINE_HOME/bin

# Override environment settings for container
ENV ORPC_BIND_HOSTNAME=0.0.0.0
ENV CURVINE_CONF_FILE=$CURVINE_HOME/conf/curvine-cluster.toml

WORKDIR $APP_HOME

# Expose ports
# 8995: Master RPC
# 8996: Master Journal/Raft
# 8997: Worker RPC
# 9000: Master Web UI
# 9001: Worker Web UI / Master Web1
EXPOSE 8995 8996 8997 9000 9001

# Set entrypoint and default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["all", "start"]

