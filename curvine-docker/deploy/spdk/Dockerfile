# SPDK Target Image
#
# Builds SPDK with TCP&RDMA and produces a runtime image for NVMe-oF target nodes.
#
# ============================================================
# Base image with CDN mirror fix (avoid geolocated mirrorlist issues)
# ============================================================
FROM rockylinux:9 AS rocky-base

RUN for repo in /etc/yum.repos.d/*.repo; do \
        [ -f "$repo" ] || continue; \
        sed -i 's/^mirrorlist=/#mirrorlist=/g' "$repo"; \
        sed -i 's/^metalink=/#metalink=/g' "$repo"; \
        sed -i 's|^#baseurl=http://dl.rockylinux.org/$contentdir|baseurl=https://dl.rockylinux.org/pub/rocky|g' "$repo"; \
        sed -i 's|^#baseurl=https://download.example/pub/epel|baseurl=https://dl.fedoraproject.org/pub/epel|g' "$repo"; \
    done

# ============================================================
# Stage 0: Install dependencies & clone SPDK
# ============================================================
FROM rocky-base AS deps

ARG SPDK_VERSION=v25.09
ARG NASM_VERSION=2.16.03
ARG ISAL_VERSION=v2.31.1

RUN --mount=type=cache,sharing=locked,target=/var/cache/dnf \
    dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y git && \
    dnf clean all

# Clone SPDK
RUN git clone --branch "$SPDK_VERSION" https://github.com/spdk/spdk.git /spdk
WORKDIR /spdk
RUN git submodule update --init --depth 1

# Install SPDK build deps
RUN --mount=type=cache,sharing=locked,target=/var/cache/dnf \
    dnf config-manager --set-disabled devel 2>/dev/null || true && \
    scripts/pkgdep.sh --rdma && \
    dnf clean all

# Build nasm from source (needed by ISA-L, not available in EPEL 9 for aarch64)
RUN curl -sL https://www.nasm.us/pub/nasm/releasebuilds/$NASM_VERSION/nasm-$NASM_VERSION.tar.gz | \
    tar -xz -C /tmp && \
    cd /tmp/nasm-$NASM_VERSION && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    rm -rf /tmp/nasm-$NASM_VERSION

# Build isa-l from source (needed by SPDK, not available in all Rocky 9 arches)
RUN cd /tmp && \
    git clone --depth 1 --branch $ISAL_VERSION https://github.com/intel/isa-l.git isa-l-src && \
    cd isa-l-src && \
    ./autogen.sh && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd / && rm -rf /tmp/isa-l-src

# ============================================================
# Stage 1: Build SPDK
# ============================================================
FROM deps AS build

ARG SPDK_VERSION
WORKDIR /spdk

RUN ./configure \
    --disable-tests \
    --with-rdma \
    --without-vfio-user \
    --target-arch=native \
    --with-shared && \
    make -j$(nproc) && \
    mkdir -p /opt/spdk && \
    cp -r ./* /opt/spdk/ && \
    ldconfig

# ============================================================
# Stage 2: Full runtime for NVMe-oF Target nodes
# ============================================================
FROM rocky-base AS spdk-full

RUN --mount=type=cache,sharing=locked,target=/var/cache/dnf \
    dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y \
    libaio numactl-libs libuuid ncurses-libs openssl-libs \
    python3 \
    kmod pciutils procps-ng \
    libffi \
    rdma-core libibverbs librdmacm \
    fuse3-libs \
    && dnf clean all

COPY --from=build /opt/spdk /opt/spdk

RUN ldconfig && \
    mkdir -p /var/log/spdk /var/run/spdk

ENV SPDK_DIR=/opt/spdk
ENV LD_LIBRARY_PATH="/opt/spdk/build/lib:/opt/spdk/dpdk/build/lib:/opt/spdk/isa-l/.libs:/opt/spdk/isa-l-crypto/.libs"
EXPOSE 4420

# ============================================================
# Stage 3: Target runtime (SPDK NVMe-oF target)
# ============================================================
FROM spdk-full AS target-runtime

LABEL stage="target-runtime"

COPY curvine-docker/deploy/spdk/entrypoint-target.sh /entrypoint-target.sh
RUN chmod +x /entrypoint-target.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD /opt/spdk/scripts/rpc.py -s /var/tmp/spdk.sock rpc_get_methods >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/entrypoint-target.sh"]
