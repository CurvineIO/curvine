# Curvine regression test runner image.
# Build from repo root: docker build -f curvine-tests/Dockerfile -t curvine-tests .
# Run with project mounted:  docker run -v $(pwd):/workspace -p 5002:5002 curvine-tests
# One-shot run:              docker run -v $(pwd):/workspace curvine-tests --run-once
FROM ghcr.io/curvineio/curvine-compile:rocky9

# Install Python Flask and test utilities via EPEL (avoids PyPI network issues entirely)
RUN dnf install -y epel-release && \
    dnf install -y python3 python3-flask jq && \
    dnf clean all

# Rust proxy (rsproxy.cn) for rustup updates and cargo crate downloads
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUN mkdir -p /root/.cargo && printf '[source.crates-io]\nreplace-with = "rsproxy"\n[source.rsproxy]\nregistry = "sparse+https://rsproxy.cn/index/"\n' > /root/.cargo/config.toml

# Install cargo-llvm-cov for coverage reports (uses rsproxy mirror above)
RUN source $HOME/.cargo/env && cargo install cargo-llvm-cov

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
