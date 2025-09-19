FROM ubuntu:24.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    fuse3 \
    libfuse3-3 \
    libfuse3-dev \
    curl \
    bash \
    && rm -rf /var/cache/apt/*

# Create curvine directories
RUN mkdir -p /opt/curvine \
    && mkdir -p /tmp/curvine/meta

# Copy entire build directory to /opt/curvine
COPY build/ /opt/curvine/

# Copy fluid config parser
COPY fluid-config-parse.py /opt/curvine/

# Make all binaries and scripts executable
RUN find /opt/curvine -type f -name "*.sh" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/bin/*" -exec chmod +x {} \; \
    && find /opt/curvine -type f -path "*/lib/*" -exec chmod +x {} \; \
    && chmod +x /opt/curvine/fluid-config-parse.py

# Set environment variables
ENV CURVINE_HOME=/opt/curvine
ENV PATH=$PATH:/opt/curvine/bin

# Override environment settings for container
ENV ORPC_BIND_HOSTNAME=0.0.0.0
ENV CURVINE_CONF_FILE=/opt/curvine/conf/curvine-cluster.toml

WORKDIR /opt/curvine

CMD ["bash", "-c", "python3 /opt/curvine/fluid-config-parse.py && /opt/curvine/mount-curvine.sh"] 