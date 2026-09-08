# Curvine regression test runner image.
# Build from repo root: docker build -f curvine-tests/Dockerfile -t curvine-tests .
# Run with project mounted:  docker run -v $(pwd):/workspace -p 5002:5002 curvine-tests
# One-shot run:              docker run -v $(pwd):/workspace curvine-tests --run-once
FROM ghcr.io/curvineio/curvine-compile:rocky9

# Python + Flask + Werkzeug (build-server) and regression helpers; no runtime pip installs in CI.
# bison/flex are required to build LTP from the release tarball.
RUN dnf install -y dnf-plugins-core epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y \
        python3 \
        python3-pip \
        python3-flask \
        python3-werkzeug \
        libstdc++-devel \
        libstdc++-static \
        zlib-devel \
        zlib-static \
        jq \
        fio \
        bison \
        flex \
        bzip2 \
        fuse \
        fuse-libs \
    && dnf clean all

# Preinstall pinned LTP into /opt/ltp (runltp era; do not bump to >=20260529
# without kirk migration). Version is intentionally inlined here so the image
# does not depend on cvtest at build time. Align with cvtest's LTP_VERSION
# when promoting a new pin. Curated runtest files (fs-cv-smoke, …) are NOT
# copied in — Portal uses stock suites; CI uses `cvtest run` to install them.
#
# make -k: same as cvtest tools/ltp/install-ltp.sh — a few unrelated
# testcases may fail to compile on this base image; -k continues so the
# install tree still completes. We still require runltp + runtest to exist.
# listmount04 is dropped up front on uapi >= 6.17 (compile break; irrelevant
# to FUSE POSIX validation), matching the cvtest installer.
ARG LTP_VERSION=20250930
RUN curl -fL --retry 8 --retry-all-errors --retry-delay 5 \
      -o /tmp/ltp-full-${LTP_VERSION}.tar.bz2 \
      "https://github.com/linux-test-project/ltp/releases/download/${LTP_VERSION}/ltp-full-${LTP_VERSION}.tar.bz2" \
 && tar -xjf /tmp/ltp-full-${LTP_VERSION}.tar.bz2 -C /tmp \
 && cd /tmp/ltp-full-${LTP_VERSION} \
 && if echo '#include <linux/mount.h>
int main(void){struct mnt_id_req r;return r.spare;}' \
       | gcc -x c - -o /dev/null 2>/dev/null; then \
     :; \
   else \
     echo "kernel uapi incompatible with listmount04; dropping it"; \
     rm -f testcases/kernel/syscalls/listmount/listmount04.c; \
   fi \
 && ./configure --prefix=/opt/ltp \
 && make -j"$(nproc)" -k \
 && make install -k \
 && test -x /opt/ltp/runltp \
 && test -d /opt/ltp/runtest \
 && rm -rf /tmp/ltp-full-${LTP_VERSION}* \
 && echo "LTP ${LTP_VERSION} installed at /opt/ltp"

# Rust proxy (rsproxy.cn) for rustup updates and cargo crate downloads
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUN mkdir -p /root/.cargo && printf '[source.crates-io]\nreplace-with = "rsproxy"\n[source.rsproxy]\nregistry = "sparse+https://rsproxy.cn/index/"\n' > /root/.cargo/config.toml

# Install cargo-llvm-cov and its required llvm-tools-preview component.
# Pre-installing the component avoids an interactive [Y/n] prompt at runtime
# that would corrupt the JSON coverage output and cause parse failures.
RUN source $HOME/.cargo/env && \
    cargo install cargo-llvm-cov && \
    rustup component add llvm-tools-preview

# Default master hostname for tests (override with -e CURVINE_MASTER_HOSTNAME=<host>)
ENV CURVINE_MASTER_HOSTNAME=localhost

# Project code is mounted at /workspace at runtime; no need to copy scripts into the image.
WORKDIR /workspace

# Reset entrypoint inherited from base image (base uses ["/bin/bash", "-c"] which breaks JSON CMD)
ENTRYPOINT []

# Default: run build-server directly from the mounted project code.
# PYTHONPATH lets Python find utils/ relative to regression/.
CMD ["python3", "/workspace/curvine-tests/regression/build-server.py", \
     "--project-path", "/workspace", \
     "--results-dir", "/workspace/curvine-tests/regression_result", \
     "--host", "0.0.0.0", "--port", "5002"]
