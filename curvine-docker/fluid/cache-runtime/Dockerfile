FROM ubuntu:24.04

# Install dependencies for Fluid CacheRuntime integration
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
    && rm -rf /var/cache/apt/*

# Install Python packages for Fluid configuration parsing
RUN pip3 install --no-cache-dir --break-system-packages pyyaml toml

# Create curvine directories
RUN mkdir -p /opt/curvine \
    && mkdir -p /opt/curvine/conf \
    && mkdir -p /opt/curvine/data \
    && mkdir -p /opt/curvine/logs \
    && mkdir -p /tmp/curvine/meta \
    && mkdir -p /tmp/curvine/cache

# Copy entire build directory to /opt/curvine
COPY build/ /opt/curvine/
COPY entrypoint.sh /opt/curvine/entrypoint.sh
COPY generate_config.py /opt/curvine/generate_config.py

# Make all binaries and scripts executable
RUN find /opt/curvine -type f -name "*.sh" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/bin/*" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/lib/*" -exec chmod +x {} \; \
    && chmod +x /opt/curvine/entrypoint.sh \
    && chmod +x /opt/curvine/generate_config.py

# Set environment variables
ENV CURVINE_HOME=/opt/curvine
ENV PATH=$PATH:/opt/curvine/bin:/opt/curvine/lib

# Override environment settings for container
ENV ORPC_BIND_HOSTNAME=0.0.0.0

# Fluid-specific environment variables
ENV FLUID_RUNTIME=curvine
ENV FLUID_MOUNT_TYPE=fuse

WORKDIR /opt/curvine

# Expose ports for Curvine services
EXPOSE 8995 8996 8997 9000 9001

# Use unified entrypoint
ENTRYPOINT ["/opt/curvine/entrypoint.sh"]