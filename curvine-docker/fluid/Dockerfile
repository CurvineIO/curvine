ARG BASE_IMAGE_TAG=latest
FROM ghcr.io/curvineio/curvine:${BASE_IMAGE_TAG}

# Add metadata labels
ARG REPO_URL=https://github.com/curvineio/curvine
LABEL org.opencontainers.image.source="${REPO_URL}"
LABEL org.opencontainers.image.description="Curvine Fluid Runtime Image (supports cache-runtime and thin-runtime)"
LABEL org.opencontainers.image.base.name="ghcr.io/curvineio/curvine"
LABEL org.opencontainers.image.base.tag="${BASE_IMAGE_TAG}"

RUN if [ -f /etc/redhat-release ]; then \
        dnf install -y python3 python3-pip python3-yaml python3-toml && \
        dnf clean all && \
        rm -rf /var/cache/dnf/*; \
    else \
        apt-get update && \
        apt-get install -y --no-install-recommends \
            python3 \
            python3-yaml \
            python3-toml && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/* && \
        rm -rf /var/cache/apt/*; \
    fi

COPY generate_config.py /app/curvine/
COPY config-parse.py /app/curvine/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /app/curvine/generate_config.py \
    && chmod +x /app/curvine/config-parse.py \
    && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

