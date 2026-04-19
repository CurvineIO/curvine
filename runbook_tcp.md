# SPDK over TCP - Test Runbook

## Prerequisites

- Standard Ethernet NIC (loopback or network)
- SPDK built and installed at `$SPDK_DIR`

## 1. SPDK Target Setup (TCP)

```bash
cd /home/thuongle/spdk_project/spdk

# Kill existing target
sudo pkill -f nvmf_tgt
sleep 2

# Setup SPDK environment (hugepages, VFIO, etc.)
sudo HUGEMEM=1024 scripts/setup.sh

# Start nvmf_tgt
sudo build/bin/nvmf_tgt -m 0x1 -s 256 &
sleep 5

# Verify it started
sudo scripts/rpc.py framework_wait_init

# Create a 64MB malloc bdev
sudo scripts/rpc.py bdev_malloc_create -b Malloc0 64 512

# Create subsystem
sudo scripts/rpc.py nvmf_create_subsystem nqn.2024-01.io.curvine:test -a -s SPDK00000000000001

# Attach bdev to subsystem
sudo scripts/rpc.py nvmf_subsystem_add_ns nqn.2024-01.io.curvine:test Malloc0

# Create TCP transport (THIS IS THE KEY)
sudo scripts/rpc.py nvmf_create_transport -t TCP

# Add TCP listener (use 127.0.0.1 for loopback, or NIC IP)
sudo scripts/rpc.py nvmf_subsystem_add_listener nqn.2024-01.io.curvine:test -t tcp -a 127.0.0.1 -s 4420

# Verify
sudo scripts/rpc.py nvmf_get_subsystems

# Test with kernel nvme cli (optional)
sudo nvme discover -t tcp -a 127.0.0.1 -s 4420
```

## 2. Run orpc Tests (TCP)

### Full test suite
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib
```

### SPDK bdev integration test only
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib -- io::spdk_bdev_test --nocapture
```

### All SPDK tests (env + bdev)
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib -- io::spdk
```

### Block I/O test
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma -p orpc --lib -- io::block_io --nocapture
```

## 3. Run curvine-server Tests (TCP)

### SPDK stress test
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
STRESS_NUM_THREADS=8 \
STRESS_BLOCKS_PER_THREAD=20 \
STRESS_ROUNDS=3 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test -p curvine-server --features spdk-rdma --test spdk_stress_test -- --test-threads=1 --nocapture
```

### SPDK worker test
```bash
SPDK_TARGET_ADDR=127.0.0.1 \
SPDK_TRANSPORT_TYPE=tcp \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test -p curvine-server --features spdk-rdma --test spdk_worker_test -- --test-threads=1 --nocapture
```

## 4. Common Tests (No SPDK)

These tests don't require SPDK hardware:

```bash
# curvine-common tests
cargo test -p curvine-common

# curvine-server library tests (unit tests only)
cargo test -p curvine-server --lib

# curvine-server SPDK meta store tests
cargo test -p curvine-server --lib spdk_meta
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SPDK_TARGET_ADDR` | Target IP address | `127.0.0.1` (loopback) |
| `SPDK_TARGET_PORT` | NVMe-oF port (default: 4420) | `4420` |
| `SPDK_TARGET_NQN` | Target NQN | `nqn.2024-01.io.curvine:test` |
| `SPDK_TRANSPORT_TYPE` | Transport type | `tcp` |
| `SPDK_HUGEPAGE_MB` | Hugepages in MB | `64` |
| `SPDK_DIR` | SPDK installation dir | `/home/thuongle/spdk_project/spdk` |
| `SPDK_REACTOR_MASK` | CPU cores to use | `0x2` |
| `STRESS_NUM_THREADS` | Stress test threads | `8` |
| `STRESS_BLOCKS_PER_THREAD` | Blocks per thread | `20` |
| `STRESS_ROUNDS` | Test rounds | `3` |
| `RUST_BACKTRACE` | Enable backtrace | `full` |
| `RUST_LOG` | Log level | `info` |

## Quick Reference

```bash
# Single command - orpc tests
SPDK_TARGET_ADDR=127.0.0.1 SPDK_TRANSPORT_TYPE=tcp SPDK_HUGEPAGE_MB=64 SPDK_DIR=/home/thuongle/spdk_project/spdk SPDK_REACTOR_MASK=0x2 RUST_BACKTRACE=full RUST_LOG=info cargo test --features spdk-rdma --package orpc --lib

# Single command - curvine-server stress test
SPDK_TARGET_ADDR=127.0.0.1 SPDK_TRANSPORT_TYPE=tcp SPDK_HUGEPAGE_MB=64 SPDK_DIR=/home/thuongle/spdk_project/spdk SPDK_REACTOR_MASK=0x2 STRESS_NUM_THREADS=8 STRESS_BLOCKS_PER_THREAD=20 STRESS_ROUNDS=3 RUST_BACKTRACE=full RUST_LOG=info cargo test -p curvine-server --features spdk-rdma --test spdk_stress_test -- --test-threads=1 --nocapture
```

## Notes

- **Feature required**: `--features spdk-rdma` is required even for TCP due to SPDK static library linking (contains both TCP and RDMA code)
- **Loopback**: Use `127.0.0.1` for local testing without network
- **Performance**: RDMA provides lower latency (~1-5μs) vs TCP (~10-50μs), but TCP works for development

## Troubleshooting

```bash
# Check target is running
sudo pkill -f nvmf_tgt
ps aux | grep nvmf_tgt

# Check hugepages
grep -i huge /proc/meminfo

# Check transport
sudo scripts/rpc.py nvmf_get_subsystems

# Check connectivity
sudo nvme discover -t tcp -a 127.0.0.1 -s 4420

# Enable verbose logging
RUST_LOG=debug cargo test ...
```