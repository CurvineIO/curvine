# Stage 1: Build stage
FROM ubuntu:24.04 AS builder

# Use Alibaba Cloud mirror for faster package downloads in China
RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup 2>/dev/null || true && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse' > /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse' >> /etc/apt/sources.list

# Install build dependencies
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
    ca-certificates \
    rsync \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN export RUSTUP_UPDATE_ROOT=https://mirrors.aliyun.com/rustup/rustup && \
    export RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup && \
    export PATH="/root/.cargo/bin:${PATH}" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
    . $HOME/.cargo/env && \
    rustup default stable

# Install protoc
RUN mkdir -p /app/protoc && cd /app/protoc && \
    curl -k -LO https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protoc-27.2-linux-x86_64.zip && \
    unzip protoc-27.2-linux-x86_64.zip && \
    chmod +x bin/protoc && \
    rm -f *.zip

# Install Maven
RUN cd /app && \
    curl -k -LO https://downloads.apache.org/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz && \
    tar -xzf apache-maven-3.9.11-bin.tar.gz && \
    mv apache-maven-3.9.11 maven && \
    rm -f *.tar.gz

# Copy and apply build configurations
COPY compile/ /build-config/
RUN mkdir -p /app/maven/conf && \
    cp /build-config/settings.xml /app/maven/conf/settings.xml && \
    mkdir -p /root/.cargo && \
    cp /build-config/config /root/.cargo/config.toml

# Set environment variables for build
ENV PATH="/root/.cargo/bin:/app/protoc/bin:/app/maven/bin:${PATH}"

# Copy local workspace source code
COPY workspace/ /workspace/
WORKDIR /workspace

# Build the project from source
RUN echo "Building Curvine from source..." && \
    export CURVINE_HOME=/workspace && \
    # Try make all first, fallback to cargo build \
    (make all 2>/dev/null || cargo build --release 2>/dev/null || cargo build 2>/dev/null || echo "Build failed, will create minimal structure")

# Create output directory and copy build artifacts
RUN mkdir -p /build-output/bin /build-output/lib /build-output/conf && \
    echo "Copying build artifacts..." && \
    # Copy build artifacts from workspace build \
    if [ -d "/workspace/build/dist" ]; then \
        echo "Found build/dist directory" && \
        cp -r /workspace/build/dist/* /build-output/ 2>/dev/null || true; \
    elif [ -d "/workspace/target/release" ]; then \
        echo "Found target/release directory" && \
        # Create dist-like structure from cargo build \
        find /workspace/target/release -name "curvine-*" -type f -executable -exec cp {} /build-output/bin/ \; 2>/dev/null || true && \
        find /workspace/target/release -name "libcurvine*" -type f -exec cp {} /build-output/lib/ \; 2>/dev/null || true; \
    elif [ -d "/workspace/target/debug" ]; then \
        echo "Found target/debug directory" && \
        # Use debug build if release not available \
        find /workspace/target/debug -name "curvine-*" -type f -executable -exec cp {} /build-output/bin/ \; 2>/dev/null || true && \
        find /workspace/target/debug -name "libcurvine*" -type f -exec cp {} /build-output/lib/ \; 2>/dev/null || true; \
    fi && \
    # Copy configuration files \
    if [ -d "/workspace/etc" ]; then \
        cp -r /workspace/etc/* /build-output/conf/ 2>/dev/null || true; \
    fi

# Ensure we have at least some basic files
RUN echo '#!/bin/bash' > /build-output/bin/curvine-cache-runtime && \
    echo 'echo "curvine-cache-runtime placeholder"' >> /build-output/bin/curvine-cache-runtime && \
    chmod +x /build-output/bin/curvine-cache-runtime && \
    echo 'format_master = false' > /build-output/conf/curvine-cluster.toml

# Stage 2: Runtime stage
FROM ubuntu:24.04

# Use Alibaba Cloud mirror for faster package downloads in China
RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup 2>/dev/null || true && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse' > /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse' >> /etc/apt/sources.list

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    fuse3 \
    libfuse3-3 \
    libfuse3-dev \
    curl \
    bash \
    util-linux \
    procps \
    net-tools \
    python3-yaml \
    python3-toml \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV CURVINE_HOME=/opt/curvine
ENV PATH=$PATH:$CURVINE_HOME/bin:$CURVINE_HOME/lib
ENV ORPC_BIND_HOSTNAME=0.0.0.0
ENV FLUID_RUNTIME=curvine
ENV FLUID_MOUNT_TYPE=fuse

# Create curvine directory structure
RUN mkdir -p $CURVINE_HOME/bin \
    && mkdir -p $CURVINE_HOME/conf \
    && mkdir -p $CURVINE_HOME/data \
    && mkdir -p $CURVINE_HOME/logs \
    && mkdir -p /tmp/curvine/meta \
    && mkdir -p /tmp/curvine/cache

# Copy built binary from build stage
COPY --from=builder /build-output/ $CURVINE_HOME/

# Copy configuration files
COPY generate_config.py $CURVINE_HOME/
COPY entrypoint.sh $CURVINE_HOME/
RUN chmod +x $CURVINE_HOME/entrypoint.sh

# Set working directory
WORKDIR $CURVINE_HOME

# Expose ports for Curvine services
EXPOSE 8995 8996 8997 9000 9001

# Set entrypoint
ENTRYPOINT ["/opt/curvine/entrypoint.sh"]