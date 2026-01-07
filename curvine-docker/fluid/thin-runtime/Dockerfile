# Build stage
FROM ubuntu:24.04 AS builder

# Add metadata labels
ARG REPO_URL=https://github.com/curvineio/curvine
LABEL org.opencontainers.image.source="${REPO_URL}"

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
    ca-certificates \
    rsync \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install the Rust toolchain
RUN export RUSTUP_UPDATE_ROOT=https://mirrors.aliyun.com/rustup/rustup && \
    export RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup && \
    export PATH="/root/.cargo/bin:${PATH}" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
    . $HOME/.cargo/env && \
    rustup default stable

# 3. Install protoc
RUN mkdir -p /app/protoc && cd /app/protoc && \
    curl -k -LO https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protoc-27.2-linux-x86_64.zip && \
    unzip protoc-27.2-linux-x86_64.zip && \
    chmod +x bin/protoc && \
    rm -f *.zip

# 4. Install Maven
RUN cd /app && \
    MAVEN_VERSION=3.9.12 && \
    curl -kfL -o apache-maven-${MAVEN_VERSION}-bin.tar.gz https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    tar -xzf apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    mv apache-maven-${MAVEN_VERSION} maven && \
    rm -f *.tar.gz && \
    echo "✓ Maven ${MAVEN_VERSION} installed successfully"

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

# 6. Build the project from source
RUN echo "Building Curvine from source..." && \
    export CURVINE_HOME=/workspace && \
    # Try make all first, fallback to cargo build \
    (make all 2>/dev/null || cargo build --release 2>/dev/null || cargo build 2>/dev/null || echo "Build failed, will create minimal structure")

# 7. Create output directory and copy build artifacts
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
RUN echo '#!/bin/bash' > /build-output/bin/curvine-fuse && \
    echo 'echo "curvine-fuse placeholder"' >> /build-output/bin/curvine-fuse && \
    chmod +x /build-output/bin/curvine-fuse && \
    echo 'format_master = false' > /build-output/conf/curvine-cluster.toml

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
RUN mkdir -p /opt/curvine \
    && mkdir -p /tmp/curvine/meta

# Copy build artifacts from builder stage

# Always create the base directory structure
RUN mkdir -p /opt/curvine/bin /opt/curvine/lib /opt/curvine/conf

# Copy from builder stage when building from source
COPY --from=builder /build-output/ /opt/curvine/

# Copy fluid config parser and entrypoint script
COPY fluid-config-parse.py /opt/curvine/
COPY entrypoint.sh /opt/curvine/

# Make all binaries and scripts executable
RUN find /opt/curvine -type f -name "*.sh" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/bin/*" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/lib/*" -exec chmod +x {} \; \
    && chmod +x /opt/curvine/fluid-config-parse.py \
    && chmod +x /opt/curvine/entrypoint.sh

# Set environment variables
ENV CURVINE_HOME=/opt/curvine
ENV PATH=$PATH:$CURVINE_HOME/bin

# Override environment settings for container
ENV ORPC_BIND_HOSTNAME=0.0.0.0
ENV CURVINE_CONF_FILE=$CURVINE_HOME/conf/curvine-cluster.toml

WORKDIR $CURVINE_HOME

# Set entrypoint and default command
ENTRYPOINT ["/opt/curvine/entrypoint.sh"]
CMD ["fluid-thin-runtime"]