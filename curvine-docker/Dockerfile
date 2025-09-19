# Stage 1: Build curvine package
FROM rockylinux:9 AS builder

# 1. Install EPEL repository and enable CRB for additional packages
RUN dnf install -y epel-release && \
    /usr/bin/crb enable && \
    dnf clean all

# 2. Install basic toolchain and dependencies
RUN dnf install -y \
    fuse3 \
    fuse3-devel \
    clang \
    llvm \
    llvm-devel \
    git \
    wget \
    zip \
    unzip \
    nodejs \
    npm \
    java-1.8.0-openjdk-devel \
    openssl-devel \
    zlib-devel \
    gcc \
    gcc-c++ \
    && dnf clean all

# 3. Install Rust toolchain
ENV PATH="/root/.cargo/bin:${PATH}"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88.0

# 4. Add musl target for static compilation
RUN rustup target add x86_64-unknown-linux-musl

# 5. Install musl tools manually for static compilation
RUN mkdir -p /opt/musl && cd /opt/musl && \
    wget https://musl.cc/x86_64-linux-musl-cross.tgz && \
    tar -xzf x86_64-linux-musl-cross.tgz && \
    rm -f x86_64-linux-musl-cross.tgz
ENV PATH="/opt/musl/x86_64-linux-musl-cross/bin:${PATH}"

# 6. Install protoc
RUN mkdir -p /app/protoc && cd /app/protoc && \
    curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protoc-27.2-linux-x86_64.zip && \
    unzip protoc-27.2-linux-x86_64.zip && \
    chmod +x bin/protoc && \
    rm -f *.zip
ENV PATH="/app/protoc/bin:${PATH}"

# 7. Install Maven
RUN cd /app && \
    curl -LO https://downloads.apache.org/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz && \
    tar -xzf apache-maven-3.9.11-bin.tar.gz && \
    mv apache-maven-3.9.11 maven && \
    rm -f *.tar.gz
ENV PATH="/app/maven/bin:${PATH}"

WORKDIR /workspace

# Create directories for build artifacts and cache
RUN mkdir -p /build-output /build-cache /build-cache/.m2/repository /build-cache/.npm

# Set environment variables for build cache
ENV CARGO_TARGET_DIR=/build-cache/target
ENV CARGO_HOME=/build-cache/.cargo

# Set Maven cache directory
ENV MAVEN_OPTS="-Dmaven.repo.local=/build-cache/.m2/repository"

# Set npm cache directory
ENV npm_config_cache=/build-cache/.npm

# Default command for builder stage - can be overridden when running
CMD ["bash", "-c", "echo 'Builder image ready. Mount your source code to /workspace and run build commands.'"]

# Stage 2: Runtime environment
FROM rockylinux:9 AS runtime

# Install minimal runtime dependencies (static binaries require less dependencies)
RUN yum install -y \
    java-1.8.0-openjdk-devel \
    procps \
    net-tools \
    && yum clean all

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Copy the build artifacts from docker-build-output directory
COPY docker-build-output/ $APP_HOME/curvine

# Copy the entrypoint script
COPY curvine-docker/deploy/entrypoint.sh /entrypoint.sh

# Set execute permissions
RUN chmod +x /entrypoint.sh && chmod +x $APP_HOME/curvine/bin/*

# Expose necessary ports
EXPOSE 8995
EXPOSE 8996
EXPOSE 8997
EXPOSE 9000
EXPOSE 9001

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
