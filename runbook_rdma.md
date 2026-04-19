# SPDK over RDMA - Test Runbook

## Prerequisites

- RDMA hardware (InfiniBand, iWARP, or RoCE NIC)
- SPDK built and installed at `$SPDK_DIR`
- Mellanox OFED or similar RDMA drivers

## 1. SPDK Target Setup (RDMA)

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

# Create RDMA transport
sudo scripts/rpc.py nvmf_create_transport -t RDMA

# Add RDMA listener
# Replace 172.16.0.224 with your RDMA NIC IP
sudo scripts/rpc.py nvmf_subsystem_add_listener nqn.2024-01.io.curvine:test -t rdma -a 172.16.0.224 -s 4420

# Verify
sudo scripts/rpc.py nvmf_get_subsystems
```

## 2. Run orpc Tests (RDMA)

### Full test suite
```bash
SPDK_TARGET_ADDR=172.16.0.224 \
SPDK_TRANSPORT_TYPE=rdma \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib
```

### SPDK bdev integration test only
```bash
SPDK_TARGET_ADDR=172.16.0.224 \
SPDK_TRANSPORT_TYPE=rdma \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib -- io::spdk_bdev_test --nocapture
```

### All SPDK tests (env + bdev)
```bash
SPDK_TARGET_ADDR=172.16.0.224 \
SPDK_TRANSPORT_TYPE=rdma \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test --features spdk-rdma --package orpc --lib -- io::spdk
```

## 3. Run curvine-server Tests (RDMA)

### SPDK stress test
```bash
SPDK_TARGET_ADDR=172.16.0.224 \
SPDK_TRANSPORT_TYPE=rdma \
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
SPDK_TARGET_ADDR=172.16.0.224 \
SPDK_TRANSPORT_TYPE=rdma \
SPDK_HUGEPAGE_MB=64 \
SPDK_DIR=/home/thuongle/spdk_project/spdk \
SPDK_REACTOR_MASK=0x2 \
RUST_BACKTRACE=full \
RUST_LOG=info \
cargo test -p curvine-server --features spdk-rdma --test spdk_worker_test -- --test-threads=1 --nocapture
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SPDK_TARGET_ADDR` | RDMA NIC IP address | `172.16.0.224` |
| `SPDK_TARGET_PORT` | NVMe-oF port (default: 4420) | `4420` |
| `SPDK_TARGET_NQN` | Target NQN | `nqn.2024-01.io.curvine:test` |
| `SPDK_TRANSPORT_TYPE` | Transport type | `rdma` |
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
SPDK_TARGET_ADDR=172.16.0.224 SPDK_TRANSPORT_TYPE=rdma SPDK_HUGEPAGE_MB=64 SPDK_DIR=/home/thuongle/spdk_project/spdk SPDK_REACTOR_MASK=0x2 RUST_BACKTRACE=full RUST_LOG=info cargo test --features spdk-rdma --package orpc --lib

# Single command - curvine-server stress test
SPDK_TARGET_ADDR=172.16.0.224 SPDK_TRANSPORT_TYPE=rdma SPDK_HUGEPAGE_MB=64 SPDK_DIR=/home/thuongle/spdk_project/spdk SPDK_REACTOR_MASK=0x2 STRESS_NUM_THREADS=8 STRESS_BLOCKS_PER_THREAD=20 STRESS_ROUNDS=3 RUST_BACKTRACE=full RUST_LOG=info cargo test -p curvine-server --features spdk-rdma --test spdk_stress_test -- --test-threads=1 --nocapture
```