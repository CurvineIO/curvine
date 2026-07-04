ARG BASE_IMAGE_TAG=latest
FROM ghcr.io/curvineio/curvine:${BASE_IMAGE_TAG}
ARG BASE_IMAGE_TAG

# Add metadata labels
ARG REPO_URL=https://github.com/curvineio/curvine
LABEL org.opencontainers.image.source="${REPO_URL}"
LABEL org.opencontainers.image.description="Curvine Fluid Runtime Image (supports cache-runtime and thin-runtime)"
LABEL org.opencontainers.image.base.name="ghcr.io/curvineio/curvine"
LABEL org.opencontainers.image.base.tag="${BASE_IMAGE_TAG}"

COPY generate_config.py /app/curvine/
COPY config-parse.py /app/curvine/
COPY mountUfs.sh /app/curvine/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /app/curvine/generate_config.py \
    && chmod +x /app/curvine/config-parse.py \
    && chmod +x /app/curvine/mountUfs.sh \
    && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
