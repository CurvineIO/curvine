# Curvine regression test runner image.
# Build from repo root: docker build -f curvine-tests/Dockerfile -t curvine-tests .
# Run with project mounted:  docker run -v $(pwd):/workspace -p 5002:5002 curvine-tests
# One-shot run:              docker run -v $(pwd):/workspace curvine-tests --run-once
FROM ghcr.io/curvineio/curvine-compile:rocky9

# Python + Flask + Werkzeug (build-server) and regression helpers; no runtime pip installs in CI.
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
    && dnf clean all

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
