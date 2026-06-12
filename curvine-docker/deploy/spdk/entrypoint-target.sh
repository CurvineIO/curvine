#!/bin/bash

# entrypoint-target.sh — SPDK NVMe-oF target
#
# Environment variables:
#   NVME_PCI_ADDR   — PCI address of NVMe device (required).
#                     Bound to uio_pci_generic automatically inside the container.
#   SUBNQN          — NVMe-oF subsystem NQN (default: nqn.2026-05.curvine:target-1)
#   TRTYPE          — Transport type: tcp or rdma (default: tcp)
#   TARGET_PORT     — NVMe-oF listener port (default: 4420)
#   REACTOR_MASK    — CPU core mask (default: 0x3)
#   MEM_SIZE        — Memory size in MB (default: 128)
#   NR_HUGE_PAGES   — Number of 2MB hugepages to allocate (default: 1024)
#   SERIAL          — NVMe subsystem serial number (default: SPDK0001)
#   MAX_QUEUE_DEPTH         — NVMe-oF transport max queue depth (SPDK default: 128)
#   MAX_QPAIRS_PER_CTRLR    — NVMe-oF transport max qpairs per controller (SPDK default: 128)
#   IN_CAPSULE_DATA_SIZE    — In-capsule data size in bytes (SPDK default: 4096)
#   IO_UNIT_SIZE            — I/O unit size in bytes (SPDK default: 131072)
#   NUM_SHARED_BUFFERS      — Number of shared buffers (SPDK default: 511)
#   ZCOPY                   — Enable zero-copy (default: false)

set -euo pipefail

# ============================================================
# Defaults
# ============================================================
SPDK_DIR="${SPDK_DIR:-/opt/spdk}"
NVME_PCI_ADDR="${NVME_PCI_ADDR:-}"
REACTOR_MASK="${REACTOR_MASK:-0x3}"
MEM_SIZE="${MEM_SIZE:-128}"
NR_HUGE_PAGES="${NR_HUGE_PAGES:-1024}"
SERIAL="${SERIAL:-SPDK0001}"
MAX_QUEUE_DEPTH="${MAX_QUEUE_DEPTH:-128}"
MAX_QPAIRS_PER_CTRLR="${MAX_QPAIRS_PER_CTRLR:-128}"
IN_CAPSULE_DATA_SIZE="${IN_CAPSULE_DATA_SIZE:-4096}"
IO_UNIT_SIZE="${IO_UNIT_SIZE:-131072}"
NUM_SHARED_BUFFERS="${NUM_SHARED_BUFFERS:-511}"
ZCOPY="${ZCOPY:-false}"

RPC="${SPDK_DIR}/scripts/rpc.py"
NVMF_TGT="${SPDK_DIR}/build/bin/nvmf_tgt"

print_info()    { echo -e "\033[34m[TARGET]\033[0m $1"; }
print_success() { echo -e "\033[32m[TARGET]\033[0m $1"; }
print_error()   { echo -e "\033[31m[TARGET]\033[0m $1"; }

# ============================================================
# Target identity — set via environment variables
# ============================================================
SUBNQN="${SUBNQN:-nqn.2026-05.curvine:$(hostname -s)}"
TARGET_PORT="${TARGET_PORT:-4420}"
TRTYPE="${TRTYPE:-tcp}"
TARGET_IP="${TARGET_IP:-0.0.0.0}"

# Normalize to lowercase
TRTYPE="${TRTYPE,,}"

# Validate the TRTYPE
case "$TRTYPE" in
    tcp|rdma) ;;
    *) print_error "Invalid TRTYPE='$TRTYPE' (expected tcp|rdma)"; exit 1 ;;
esac

# Validate required PCI address
if [ -z "$NVME_PCI_ADDR" ]; then
    print_error "NVME_PCI_ADDR is required (set to your NVMe device PCI address, e.g. 0000:01:00.0)"
    exit 1
fi

print_info "Config: SUBNQN=$SUBNQN TRTYPE=$TRTYPE PORT=$TARGET_PORT PCI=$NVME_PCI_ADDR"

# ============================================================
# Hugepages (host-side, needs privileged or IPC_LOCK)
# ============================================================
print_info "Configuring hugepages (${NR_HUGE_PAGES} pages)..."
sysctl -w vm.nr_hugepages="$NR_HUGE_PAGES" || {
    print_error "Failed to set hugepages. Container must run privileged."
    exit 1
}

# ============================================================
# Start nvmf_tgt with --wait-for-rpc
# ============================================================
# Clean up stale socket from previous run
rm -f /var/tmp/spdk.sock

# Load uio_pci_generic and bind NVMe device
modprobe uio_pci_generic 2>/dev/null || true
if [ -n "$NVME_PCI_ADDR" ]; then
    CURRENT_DRIVER=$(basename "$(readlink /sys/bus/pci/devices/"$NVME_PCI_ADDR"/driver 2>/dev/null)" 2>/dev/null)
    if [ "$CURRENT_DRIVER" != "uio_pci_generic" ]; then
        print_info "Binding $NVME_PCI_ADDR to uio_pci_generic..."
        [ -n "$CURRENT_DRIVER" ] && echo "$NVME_PCI_ADDR" > /sys/bus/pci/devices/"$NVME_PCI_ADDR"/driver/unbind 2>/dev/null || true
        echo "uio_pci_generic" > /sys/bus/pci/devices/"$NVME_PCI_ADDR"/driver_override 2>/dev/null || true
        echo "$NVME_PCI_ADDR" > /sys/bus/pci/drivers/uio_pci_generic/bind 2>/dev/null || {
            print_error "Failed to bind $NVME_PCI_ADDR to uio_pci_generic"
            exit 1
        }
        echo "" > /sys/bus/pci/devices/"$NVME_PCI_ADDR"/driver_override 2>/dev/null || true
        print_success "Device $NVME_PCI_ADDR bound to uio_pci_generic"
    else
        print_info "Device $NVME_PCI_ADDR already bound to uio_pci_generic"
    fi
fi

print_info "Starting nvmf_tgt (reactor_mask=$REACTOR_MASK, mem=${MEM_SIZE}MB)..."
"$NVMF_TGT" \
    -m "$REACTOR_MASK" \
    -s "$MEM_SIZE" \
    --wait-for-rpc \
    -r /var/tmp/spdk.sock &
NVMF_PID=$!
print_info "nvmf_tgt PID: $NVMF_PID"

# Wait for RPC socket (up to 15s)
for i in $(seq 1 30); do
    [ -S /var/tmp/spdk.sock ] && break
    sleep 0.5
done
if [ ! -S /var/tmp/spdk.sock ]; then
    print_error "nvmf_tgt RPC socket not ready after 15s"
    exit 1
fi
print_success "nvmf_tgt RPC socket ready"

# ============================================================
# Framework init
# ============================================================
print_info "Starting I/O processing..."
"$RPC" framework_start_init
print_success "framework_start_init complete"

# ============================================================
# Configure transport, subsystem, listener, bdev, namespace
# ============================================================

ZCOPY_OPT=""
if [ "$ZCOPY" = "true" ]; then
    ZCOPY_OPT="-z"
fi

case "$TRTYPE" in
    tcp)
        print_info "Creating TCP transport..."
        "$RPC" nvmf_create_transport -t TCP \
            -q "$MAX_QUEUE_DEPTH" \
            -m "$MAX_QPAIRS_PER_CTRLR" \
            -c "$IN_CAPSULE_DATA_SIZE" \
            -u "$IO_UNIT_SIZE" \
            -n "$NUM_SHARED_BUFFERS" \
            $ZCOPY_OPT
        ;;
    rdma)
        print_info "Creating RDMA transport..."
        "$RPC" nvmf_create_transport -t RDMA \
            -q "$MAX_QUEUE_DEPTH" \
            -m "$MAX_QPAIRS_PER_CTRLR" \
            -c "$IN_CAPSULE_DATA_SIZE" \
            -u "$IO_UNIT_SIZE" \
            -n "$NUM_SHARED_BUFFERS" \
            $ZCOPY_OPT
        ;;
esac

# Create subsystem
print_info "Creating subsystem $SUBNQN..."
"$RPC" nvmf_create_subsystem "$SUBNQN" -a -s "$SERIAL"
print_success "Subsystem created"

# Add listener matching TRTYPE
case "$TRTYPE" in
    tcp)
        print_info "Adding TCP listener on $TARGET_IP:$TARGET_PORT..."
        "$RPC" nvmf_subsystem_add_listener "$SUBNQN" -t TCP -a "$TARGET_IP" -s "$TARGET_PORT"
        ;;
    rdma)
        print_info "Adding RDMA listener on $TARGET_IP:$TARGET_PORT..."
        "$RPC" nvmf_subsystem_add_listener "$SUBNQN" -t RDMA -a "$TARGET_IP" -s "$TARGET_PORT"
        ;;
esac

# Attach NVMe controller
print_info "Attaching NVMe controller at $NVME_PCI_ADDR..."
"$RPC" bdev_nvme_attach_controller -b Nvme0 -t PCIe -a "$NVME_PCI_ADDR"
print_success "NVMe controller attached as Nvme0"

# Add namespace to subsystem
print_info "Adding Nvme0n1 to subsystem..."
"$RPC" nvmf_subsystem_add_ns "$SUBNQN" Nvme0n1
print_success "Namespace added"

# ============================================================
# Graceful shutdown handler
# ============================================================
cleanup() {
    print_info "Shutting down..."
    "$RPC" bdev_nvme_detach_controller Nvme0 2>/dev/null || true
    "$RPC" nvmf_delete_subsystem "$SUBNQN" 2>/dev/null || true
    kill -TERM "$NVMF_PID" 2>/dev/null || true
    wait "$NVMF_PID" 2>/dev/null || true
    print_success "Clean shutdown complete"
}
trap cleanup TERM INT

# ============================================================
# Print summary
# ============================================================
print_success "SPDK NVMe-oF Target running"
print_info "  Subsystem: $SUBNQN"
print_info "  Listener:  $TARGET_IP:$TARGET_PORT"
print_info "  PCI Addr:   $NVME_PCI_ADDR"

# ============================================================
# Wait for nvmf_tgt to exit
# ============================================================
wait "$NVMF_PID"
