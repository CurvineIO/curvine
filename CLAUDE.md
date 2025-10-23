# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Curvine is a high-performance, concurrent distributed cache system written in Rust. It uses a Master-Worker architecture with support for multi-level caching (memory, SSD, HDD), FUSE filesystem interface, and multiple underlying storage backends.

## Build Commands

### Building the Project

```bash
# Build all modules in release mode
make all

# Build specific modules
make build ARGS="-p core"                      # Build server, client, and cli
make build ARGS="-p server -p client"          # Build only server and client
make build ARGS="-p fuse"                      # Build FUSE module
make build ARGS="-p web"                       # Build web UI
make build ARGS="-p java"                      # Build Java SDK
make build ARGS="-p object"                    # Build S3 object gateway

# Build with specific UFS storage backend
make build ARGS="-p core -u s3"                # Build with AWS S3 native SDK
make build ARGS="-p core -u opendal-s3"        # Build with OpenDAL S3
make build ARGS="-p core -u opendal-oss"       # Build with Alibaba Cloud OSS

# Build in debug mode
make build ARGS="-d"

# Create distribution package
make dist
RELEASE_VERSION=v1.0.0 make dist

# Build using Docker
make docker-build                              # Uses curvine/curvine-compile:latest
make docker-build-cached                       # Uses cached dependencies
```

### Direct build.sh Usage

```bash
# Show all available options
sh build/build.sh -h

# Build examples
sh build/build.sh                              # Build all in release mode
sh build/build.sh -p core                      # Build core modules
sh build/build.sh -p core -p fuse -d          # Build core and fuse in debug mode
```

### CSI (Container Storage Interface)

```bash
make csi-build                                 # Build curvine-csi Go binary
make csi-run                                   # Run curvine-csi from source
make csi-docker-build                          # Build Docker image
make csi-fmt                                   # Format Go code
make csi-vet                                   # Run go vet
```

### Testing

```bash
# Run all tests with formatting and clippy checks
sh build/run-tests.sh

# Run tests with clippy (deny level)
sh build/run-tests.sh --clippy

# Run tests with custom clippy level
sh build/run-tests.sh --clippy --level warn

# Run Rust tests only
cargo test --release

# Format code
cargo fmt

# Run clippy
cargo clippy --release --all-targets -- --deny warnings
```

### Code Formatting

```bash
# Format code using pre-commit hooks
make format
```

## Architecture

### Master-Worker Design

Curvine uses a distributed master-worker architecture:

- **Master Node** (`curvine-server/src/master/`): Manages metadata, coordinates worker nodes, handles TTL operations, and provides cluster management through Raft consensus
- **Worker Node** (`curvine-server/src/worker/`): Stores and processes actual data across multiple storage tiers (memory, SSD, HDD)
- **Journal** (Raft): Provides high availability for master metadata using Raft consensus algorithm

### Core Modules

- **orpc**: Custom high-performance RPC framework built on Tokio
  - Async server/client implementation
  - Message encoding/decoding
  - Runtime management

- **curvine-common**: Shared libraries and protocols
  - Protocol buffer definitions
  - Configuration management (TOML-based)
  - File system abstractions
  - Error handling
  - Common utilities

- **curvine-server**: Server-side components (Master and Worker)
  - Master: metadata management, cluster coordination, TTL management
  - Worker: data storage, cache management, multi-tier storage

- **curvine-client**: Client library with RPC communications
  - Block storage interface
  - File system client
  - Unified filesystem layer
  - Support for multiple UFS backends via features

- **curvine-fuse**: FUSE filesystem interface (supports both fuse2 and fuse3)

- **curvine-cli**: Command-line interface (`cv` command)

- **curvine-web**: Web UI for monitoring and management

- **curvine-libsdk**: Multi-language SDK (Java, Python)

- **curvine-s3-gateway**: S3-compatible object storage gateway

- **curvine-ufs**: Unified File Storage abstraction layer
  - Supports multiple backends: AWS S3 (native SDK), OpenDAL (S3, OSS, GCS, Azure Blob)

- **curvine-tests**: Test suite and benchmarks

### Storage Backend Configuration

The client supports multiple storage backends configured via Cargo features:

- `s3`: AWS S3 using native AWS SDK (default)
- `opendal-s3`: S3 via OpenDAL
- `opendal-oss`: Alibaba Cloud OSS
- `opendal-gcs`: Google Cloud Storage
- `opendal-azblob`: Azure Blob Storage

Build configuration is passed through the `-u` flag to `build.sh` or via Cargo features.

### Configuration

Configuration is managed via TOML files in `etc/`:

- `curvine-cluster.toml`: Main cluster configuration
  - Master/Worker settings
  - Client configuration
  - Journal (Raft) configuration
  - TTL settings
  - S3 gateway settings
  - Log configuration

### Build Artifacts

After building, artifacts are placed in `build/dist/`:
- `bin/`: Executable scripts (curvine-master.sh, curvine-worker.sh, curvine-fuse.sh, cv)
- `lib/`: Compiled binaries (curvine-server, curvine-cli, curvine-fuse, curvine-bench, etc.)
- `conf/`: Configuration files copied from `etc/`
- `build-version`: Build metadata (git commit, OS, FUSE version, version number, UFS types)

## Development Workflow

### Feature Packages

The build system uses package-based builds:
- `core`: server + client + cli (most common for development)
- `all`: All packages including web, fuse, java, tests, object gateway

### FUSE Version Detection

The build system automatically detects FUSE version (fuse2 vs fuse3) and builds accordingly. FUSE 3 is preferred when available.

### Version Information

Version is defined in workspace `Cargo.toml` and used across all modules. Current version format: `0.2.1-beta`

### Commit Conventions

Follow conventional commit format (see `COMMIT_CONVENTION.md`):
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `chore:` - Build/tooling changes
- `ci:` - CI/CD changes

### Testing Strategy

The test suite (`build/run-tests.sh`) runs:
1. Code formatting check (`cargo fmt -- --check`)
2. Optional Clippy linting (`cargo clippy`)
3. Test cluster startup (example test cluster)
4. Full test suite (`cargo test --release`)

## Key Dependencies

- **Tokio**: Async runtime (version 1.42+)
- **Raft**: Consensus algorithm for master HA
- **RocksDB**: Metadata storage
- **Prost**: Protocol buffers
- **Axum**: Web framework (0.7 for most modules, 0.8 for S3 gateway)
- **Hyper**: HTTP library
- **Serde**: Serialization

## Running Curvine

```bash
cd build/dist

# Start master node
bin/curvine-master.sh start

# Start worker node
bin/curvine-worker.sh start

# Mount FUSE filesystem (default: /curvine-fuse)
bin/curvine-fuse.sh start

# Use CLI
bin/cv report                    # Cluster overview
bin/cv fs mkdir /a              # Create directory
bin/cv fs ls /                  # List directory

# Access Web UI
# http://localhost:9000
```

## Multi-platform Support

Curvine supports:
- **Platforms**: Linux (CentOS 7/8, Rocky Linux 9, RHEL 9, Ubuntu 22), macOS (limited)
- **Architectures**: x86_64, aarch64
- **FUSE**: Both fuse2 (CentOS 7) and fuse3 (newer systems)

Build system auto-detects platform and architecture for distribution naming.

---

## Detailed Code Architecture

### **curvine-common** - Shared Foundation Layer

This module provides foundational components shared across the entire system.

#### **1. Protocol Definitions**

**Location:** `curvine-common/proto/`

Protocol buffer files defining RPC contracts:
- `common.proto` - Common data structures and types
- `master.proto` - Master node RPC protocols (metadata operations)
- `worker.proto` - Worker node RPC protocols (block operations)
- `job.proto` - Job management and scheduling protocols
- `mount.proto` - Mount point management protocols
- `replication.proto` - Data replication protocols
- `raft.proto` - Raft consensus messages

**Generated Code:** Built via `build.rs` into Rust structs with automatic serde serialization support. All generated code is placed in `$OUT_DIR/protos/` and included via `curvine-common/src/lib.rs:27-33`.

#### **2. State Management** (`src/state/`)

Core data structures representing distributed system state:

**Worker Management:**
- `WorkerInfo` - Worker node metadata and capabilities
- `WorkerAddress` - Network address of worker nodes
- `WorkerStatus` - Worker health and operational status
- `HeartbeatStatus` - Heartbeat state tracking
- `WorkerCommand` - Commands sent to workers
- `WorkerNodeTree` - Hierarchical worker organization

**Data Management:**
- `BlockInfo` - Block metadata (storage unit information)
- `FileStatus` - File metadata and attributes
- `FileType` - File type enumeration (file, directory, symlink)
- `LastBlockStatus` - Status of file's last block

**Storage:**
- `StorageInfo` - Storage capacity and usage information
- `StoragePolicy` - Multi-tier storage policies and rules

**System:**
- `MasterInfo` - Master node information
- `TtlAction` - Time-to-live action types
- `ClientAddress` - Client connection addresses
- `CreateFlag` - File creation flags
- `Mount` - Mount point information
- `PosixPermission` - POSIX permission handling
- `Job` - Job definitions and state
- `Metrics` - System performance metrics

#### **3. Filesystem Abstraction** (`src/fs/`)

Core filesystem interfaces and types:
- `Path` - Path manipulation, validation, and parsing
- `FileSystem` - Core filesystem trait defining operations
- `Reader` - Async read interface for file data
- `Writer` - Async write interface for file data
- `RpcCode` - RPC response codes and status
- `CurvineURI` - Unified resource identifier (type alias for `Path`)

Used by both client and server for consistent filesystem semantics.

#### **4. Configuration System** (`src/conf/`)

TOML-based hierarchical configuration:
- `ClusterConf` - Root cluster configuration (loads from `curvine-cluster.toml`)
- `MasterConf` - Master node settings (metadata dir, log config, TTL settings)
- `WorkerConf` - Worker node settings (data dirs, storage tiers, reserved space)
- `ClientConf` - Client connection settings (master addresses, timeouts)
- `JournalConf` - Raft journal configuration (journal addresses, data dir)
- `FuseConf` - FUSE mount settings
- `UfsConf` - Underlying file storage configuration
- `JobConf` - Job scheduling configuration

**Helper Types:**
- `SizeString` - Human-readable size parsing ("1GB", "512MB")
- `DurationString` - Human-readable duration parsing ("30m", "1h")

Configuration files are in `etc/` and copied to `build/dist/conf/` during build.

#### **5. Raft Consensus** (`src/raft/`)

High-availability implementation for master metadata:

**Core Components:**
- `RaftNode` - Individual Raft node instance
- `RaftJournal` - Raft log persistence and replay
- `RaftGroup` - Raft cluster group management
- `RaftClient` - RPC client for Raft communication
- `RaftServer` - RPC server for Raft messages
- `RaftPeer` - Peer node representation
- `RaftCode` - Raft-specific status codes
- `RaftError` - Raft error types

**Storage** (`src/raft/storage/`):
- RocksDB-based log storage
- `RocksLogStorage` - Persistent Raft log
- `file/` - File-based storage backend

**Snapshot** (`src/raft/snapshot/`):
- Snapshot creation and restoration
- Snapshot transfer between nodes

**Monitoring:**
- `RoleMonitor` - Tracks Raft role changes (Leader/Follower/Candidate)

Uses the `raft` crate (0.7.0) with prost-codec for message serialization.

#### **6. RocksDB Integration** (`src/rocksdb/`)

Metadata persistence layer:
- `DbEngine` - RocksDB wrapper with async operations
- `DbConf` - Database configuration (paths, column families, options)
- `WriteBatch` - Batch write operations for atomicity
- `rocks_utils` - Helper utilities for RocksDB operations

Used by Master for metadata storage and Raft for log persistence.

#### **7. Error Handling** (`src/error/`)

Unified error handling:
- `FsError` - Comprehensive filesystem error type
- `FsResult<T>` - Standard result type (alias for `Result<T, FsError>`)

Covers: I/O errors, RPC errors, Raft errors, permission errors, path errors, etc.

#### **8. Utilities** (`src/utils/`)

Common helper functions:
- `proto_utils` - Protocol buffer conversion helpers
- `rpc_utils` - RPC utility functions
- `serde_utils` - Custom serialization/deserialization
- `display` - Display formatting for complex types

#### **9. Executor** (`src/executor/`)

Task execution framework for background operations.

---

### **curvine-server** - Master and Worker Implementation

Server-side logic for both Master (metadata) and Worker (data storage) nodes.

### **Master Node** (`src/master/`)

Responsible for **metadata management** and **cluster coordination**.

#### **Core Components**

**MasterServer** (`master_server.rs`):
- `MasterService` - Main service container holding all master components
- Manages: `MasterFilesystem`, `MountManager`, `JobManager`, `ReplicationManager`
- Implements `HandlerService` trait for per-connection RPC handling
- Creates `MasterHandler` instances for each client connection
- Integrates with `WebServer` for monitoring UI

Key fields in `MasterService`:
```rust
pub struct MasterService {
    conf: ClusterConf,
    fs: MasterFilesystem,
    retry_cache: Option<FsRetryCache>,
    mount_manager: Arc<MountManager>,
    job_manager: Arc<JobManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<MasterReplicationManager>,
}
```

**MasterHandler** (`master_handler.rs`):
- Processes RPC requests from clients and workers
- Routes requests to appropriate subsystems
- Maintains per-connection state via `ConnState`
- Handles: file operations, block allocation, worker registration

**RouterHandler** (`router_handler.rs`):
- Request routing and dispatch logic
- Load balancing decisions

**Monitoring:**
- `MasterMetrics` - Performance metrics collection (via Prometheus)
- `MasterMonitor` - Health monitoring and status tracking

**RpcContext** (`rpc_context.rs`):
- Context object passed through RPC call stack
- Contains authentication, tracing, and request metadata

#### **Metadata Management** (`src/master/meta/`)

The heart of the Master node - manages all filesystem metadata.

**FsDir** (`fs_dir.rs`):
- Core in-memory directory tree structure
- Root of the entire filesystem namespace hierarchy
- Manages inode tree and namespace operations
- Thread-safe via `ArcRwLock<FsDir>` (alias: `SyncFsDir`)

**Inode System** (`src/master/meta/inode/`):

Core abstractions:
- `Inode` trait - Common interface for all inode types
- `InodeView` - Type-erased inode wrapper (enum of File/Dir)
- `InodeFile` - File inode implementation
- `InodeDir` - Directory inode implementation
- `InodePath` - Path-to-inode resolution logic
- `InodesChildren` - Parent-child relationship management

Key constants defined in `curvine-server/src/master/meta/inode/mod.rs:35-43`:
```rust
pub const ROOT_INODE_ID: i64 = 1000;
pub const ROOT_INODE_NAME: &str = "";
pub const PATH_SEPARATOR: &str = "/";
pub const EMPTY_PARENT_ID: i64 = -1;
```

Type alias: `InodePtr = RawPtr<InodeView>` for unsafe pointer optimization.

**TTL (Time-To-Live) System** (`src/master/meta/inode/ttl/`):

Automatic expiration and cleanup of cached data:
- `TtlManager` - Central TTL coordination and state management
- `TtlChecker` - Periodic TTL bucket scanning (configurable interval)
- `TtlScheduler` - Schedules TTL operations across time buckets
- `TtlExecutor` - Executes deletion operations for expired files
- `TtlBucket` - Time-bucketed TTL entries for efficient scanning
- `TtlTypes` - TTL-related data structures and enums

TTL configuration in `etc/curvine-cluster.toml:15-19`:
```toml
ttl_checker_interval = "1h"        # Checker execution interval
ttl_checker_retry_attempts = 3     # Max retry attempts
ttl_bucket_interval = "1h"         # Bucket time interval
ttl_max_retry_duration = "30m"     # Max retry duration
ttl_retry_interval = "5s"          # Retry interval
```

**BlockMeta** (`block_meta.rs`):
- Maps block IDs to worker locations
- Tracks block replication status
- Maintains block allocation state

**Storage** (`src/master/meta/store/`):
- RocksDB-based persistence for metadata
- Implements write-ahead logging via Raft journal
- Crash recovery and replay logic

**Feature Management** (`src/master/meta/feature/`):
- Feature flags and capability negotiation
- Version compatibility handling

**InodeId** (`inode_id.rs`):
- Inode ID generation and management
- Ensures unique ID allocation

**FileSystemStats** (`fs_stats.rs`):
- Aggregated filesystem statistics
- Capacity, usage, file counts

#### **Filesystem Layer** (`src/master/fs/`)

High-level filesystem operations built on metadata layer.

**MasterFilesystem** (`master_filesystem.rs`):
- High-level filesystem API (create, delete, rename, chmod, etc.)
- Coordinates between metadata (`FsDir`) and workers (`WorkerManager`)
- Implements filesystem semantics (POSIX-like)
- Transaction-like operations with rollback

**WorkerManager** (`worker_manager.rs`):
- Maintains registry of all workers in cluster
- Tracks worker health, capacity, and load
- Worker selection for block allocation (load balancing)
- Worker failure detection and handling
- Thread-safe via `ArcRwLock<WorkerManager>` (alias: `SyncWorkerManager`)

**MasterActor** (`master_actor.rs`):
- Actor-based async operation handling
- Message queue for sequential metadata operations
- Ensures consistency during concurrent access

**HeartbeatChecker** (`heartbeat_checker.rs`):
- Monitors worker health via periodic heartbeats
- Detects worker failures and network partitions
- Triggers failover and rebalancing

**FsRetryCache** (`fs_retry_cache.rs`):
- Caches state for idempotent operations
- Enables exactly-once semantics for retried requests
- Important for distributed operation consistency

**DeleteResult** (`delete_result.rs`):
- Results from delete operations
- Tracks deleted files, blocks, and errors

**Policy** (`src/master/fs/policy/`):
- Storage tier selection policies (Memory/SSD/HDD)
- Replication factor policies
- Block placement strategies

**Context** (`src/master/fs/context/`):
- Filesystem operation context
- User identity, permissions, request metadata

**State** (`src/master/fs/state/`):
- Runtime filesystem state management
- Tracks ongoing operations

#### **Journal System** (`src/master/journal/`)

Raft-based write-ahead logging for metadata durability:

- `JournalSystem` - Manages Raft journal lifecycle
- `JournalWriter` - Writes metadata changes to journal
- `JournalLoader` - Loads and applies journal entries during recovery
- `Entry` - Journal entry types (metadata operations)
- `SenderTask` - Asynchronous journal replication to followers

Type alias: `MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>`

All metadata mutations go through the journal for:
1. Durability (survives crashes)
2. Replication (high availability)
3. Consistency (linearizable operations)

#### **Mount Management** (`src/master/mount/`)

- `MountManager` - Manages client mount points and sessions
- Tracks active connections
- Mount point isolation and permissions

#### **Job Management** (`src/master/job/`)

Background job scheduling and execution:
- `JobManager` - Schedules and manages background jobs
- `JobHandler` - Executes jobs (compaction, cleanup, metrics collection)
- Job types: TTL cleanup, block rebalancing, capacity planning

#### **Replication** (`src/master/replication/`)

- `MasterReplicationManager` - Coordinates block replication across workers
- Ensures data redundancy meets replication factor
- Handles under-replicated and over-replicated blocks
- Rebalancing logic

---

### **Worker Node** (`src/worker/`)

Handles **data storage** and **block-level I/O operations**.

#### **Core Components**

**WorkerServer** (`worker_server.rs`):
- Main worker service implementation
- Handles block read/write/delete requests from clients
- Registers with master via heartbeat
- Manages storage tiers (memory, SSD, HDD)

**WorkerMetrics** (`worker_metrics.rs`):
- Performance metrics (IOPS, throughput, latency)
- Capacity metrics (used/free space per tier)
- Exported to Prometheus

#### **Storage Management** (`src/worker/storage/`)

Multi-tier storage implementation.

**VfsDataset** (`vfs_dataset.rs`):
- Virtual filesystem layer for block storage
- Manages multiple storage directories across tiers
- Type alias: `BlockDataset = VfsDataset`

**VfsDir** (`vfs_dir.rs`):
- Individual storage directory management
- Handles one storage tier (e.g., /data/ssd1)

**Dataset** (`dataset.rs`):
- Block dataset abstraction
- Provides unified interface over storage tiers

**Storage Directory Structure** (from `curvine-server/src/worker/storage/mod.rs:38-42`):
```rust
pub const FINALIZED_DIR: &str = "finalized";  // Completed blocks
pub const RBW_DIR: &str = "rbw";              // Replica Being Written
```

Each storage dir contains:
- `finalized/` - Immutable completed blocks
- `rbw/` - Blocks currently being written

**Policy** (`policy.rs`):
- Storage tier selection logic (hot data → memory, warm → SSD, cold → HDD)
- Eviction policies when tier is full

**DirList** (`dir_list.rs`):
- Directory listing and iteration
- Efficient block enumeration

**Version** (`version.rs`):
- Storage format versioning
- Migration support for format changes

**DirState** (`dir_state.rs`):
- Directory state tracking (healthy, degraded, failed)

#### **Block Management** (`src/worker/block/`)

Physical block storage and lifecycle.

**BlockStore** (`block_store.rs`):
- Physical block storage operations
- Read/write/delete blocks from disk
- Checksum verification
- Block metadata tracking

**BlockMeta** (`block_meta.rs`):
- Worker-local block metadata
- Tracks block location, size, checksum

**BlockActor** (`block_actor.rs`):
- Actor-based async block operation handler
- Serializes operations on same block
- Prevents race conditions

**MasterClient** (`master_client.rs`):
- RPC client for master communication
- Reports block completion/deletion
- Requests work assignments

**HeartbeatTask** (`heartbeat_task.rs`):
- Sends periodic heartbeats to master
- Reports: capacity, block count, health status
- Receives commands from master

#### **Handler** (`src/worker/handler/`)

RPC request handlers for worker operations:
- Block read/write handlers
- Replication handlers
- Admin command handlers

#### **Replication** (`src/worker/replication/`)

Block replication between workers:
- Receives replication requests from master
- Pushes blocks to peer workers
- Pipeline replication for efficiency
- Ensures data durability

#### **Task Management** (`src/worker/task/`)

Background task execution:
- Block verification tasks
- Cleanup tasks (expired blocks)
- Compaction tasks
- Async task scheduling with Tokio

---

### **Common Server Components** (`src/common/`)

Shared between Master and Worker.

**UFS (Underlying File Storage) Integration:**
- `UfsManager` - Manages UFS client connections and lifecycle
- `UfsClient` - Unified client interface to various UFS backends
- `UfsFactory` - Creates UFS client instances based on configuration

Supports multiple backends (configured via Cargo features):
- AWS S3 (native SDK)
- OpenDAL: S3, OSS, GCS, Azure Blob

Used for cold data tier when local storage is insufficient.

---

## Data Flow Architecture

### **Write Path:**

1. **Client → Master:** "Create file `/data/file.txt`"
2. **Master:**
   - Allocates inode ID (e.g., 1234)
   - Creates inode in `FsDir`
   - Selects workers based on policy (e.g., Worker1, Worker2, Worker3)
   - Writes to Raft journal for durability
3. **Master → Client:** Returns block locations `[{blockId: 5678, workers: [W1, W2, W3]}]`
4. **Client → Worker(s):** Writes data blocks to workers
5. **Worker:**
   - Writes to `rbw/` directory (Replica Being Written)
   - After successful write, moves to `finalized/`
   - Stores on appropriate tier (memory/SSD/HDD)
6. **Worker → Master:** "Block 5678 completed"
7. **Master:**
   - Updates block metadata in `BlockMeta`
   - Commits to Raft journal

### **Read Path:**

1. **Client → Master:** "Read file `/data/file.txt`"
2. **Master:**
   - Resolves path to inode 1234
   - Retrieves block locations from `BlockMeta`
   - Returns: `[{blockId: 5678, workers: [W1, W2, W3]}]`
3. **Client → Worker(s):** Reads data blocks directly (bypasses master)
4. **Worker:**
   - Checks memory tier first
   - Falls back to SSD/HDD if not in memory
   - Falls back to UFS if not local
   - Returns block data

### **Heartbeat Flow:**

1. **Worker → Master:** Periodic heartbeat (every N seconds)
   - Reports: total capacity, used space, block count, health
2. **Master → WorkerManager:** Updates worker status
3. **Master:**
   - Detects failed workers (missed heartbeats)
   - Triggers replication for under-replicated blocks
   - Triggers rebalancing if cluster is unbalanced

### **TTL Flow:**

1. **TtlChecker:** Scans TTL buckets periodically (configurable interval)
   - Checks current time bucket for expired files
2. **TtlScheduler:** Schedules deletion tasks
3. **TtlExecutor:**
   - Deletes expired files from `FsDir`
   - Deletes metadata from `BlockMeta`
   - Writes deletion to Raft journal
4. **Master → Workers:** Sends delete commands for expired blocks
5. **Workers:** Remove blocks from storage tiers

---

## Key Architectural Patterns

1. **Actor Model:** `MasterActor`, `BlockActor` use async message passing for sequential consistency
2. **Custom RPC Framework:** `orpc` provides high-performance, low-latency RPC built on Tokio
3. **Raft Consensus:** Master metadata replicated via Raft for high availability and linearizability
4. **Multi-tier Storage:** Hierarchical cache (Memory → SSD → HDD → UFS) with automatic promotion/demotion
5. **Protocol Buffers:** Type-safe, efficient RPC serialization via `prost`
6. **RocksDB:** Persistent, embedded key-value store for metadata
7. **Async/Await:** Tokio runtime throughout for high concurrency
8. **Lock-free Structures:** `DashMap`, `ArcRwLock` for concurrent access patterns
9. **Zero-copy:** Direct buffer passing where possible

---

## Important Type Aliases

```rust
// curvine-common/src/lib.rs
pub type FsResult<T> = Result<T, FsError>;

// curvine-common/src/fs/mod.rs
pub type CurvineURI = Path;

// curvine-server/src/master/mod.rs
pub type MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>;
pub type SyncFsDir = ArcRwLock<FsDir>;
pub type SyncWorkerManager = ArcRwLock<WorkerManager>;

// curvine-server/src/worker/storage/mod.rs
pub type BlockDataset = VfsDataset;

// curvine-server/src/master/meta/inode/mod.rs
pub type InodePtr = RawPtr<InodeView>;
```

---

## Code Navigation Guide

### **Starting Points:**

1. **Server Entry Point:** `curvine-server/src/bin/curvine-server.rs`
   - Parses config, starts Master or Worker based on role

2. **Master Entry:** `curvine-server/src/master/master_server.rs:MasterService`
   - Central master service container

3. **Worker Entry:** `curvine-server/src/worker/worker_server.rs`
   - Central worker service container

### **Key Files to Understand:**

**Protocols:**
- `curvine-common/proto/master.proto` - Master RPC interface
- `curvine-common/proto/worker.proto` - Worker RPC interface

**Core Abstractions:**
- `curvine-common/src/fs/filesystem.rs` - Filesystem trait
- `curvine-common/src/fs/path.rs` - Path handling

**Metadata Core:**
- `curvine-server/src/master/meta/fs_dir.rs` - Directory tree
- `curvine-server/src/master/meta/inode/inode_file.rs` - File inode
- `curvine-server/src/master/meta/inode/inode_dir.rs` - Directory inode

**Storage Core:**
- `curvine-server/src/worker/storage/vfs_dataset.rs` - Storage management
- `curvine-server/src/worker/block/block_store.rs` - Block I/O

**RPC Framework:**
- `orpc/src/server/mod.rs` - RPC server
- `orpc/src/client/mod.rs` - RPC client
- `orpc/src/handler/mod.rs` - Request handling

**Configuration:**
- `curvine-common/src/conf/cluster_conf.rs` - Config loading

### **Tracing Request Flow:**

**Create File Flow:**
1. Start: `curvine-server/src/master/master_handler.rs` - RPC handler
2. → `curvine-server/src/master/fs/master_filesystem.rs` - Filesystem operation
3. → `curvine-server/src/master/meta/fs_dir.rs` - Metadata update
4. → `curvine-server/src/master/journal/journal_writer.rs` - Raft journal write

**Write Block Flow:**
1. Start: `curvine-server/src/worker/handler/` - RPC handler
2. → `curvine-server/src/worker/block/block_actor.rs` - Async block operation
3. → `curvine-server/src/worker/block/block_store.rs` - Physical write
4. → `curvine-server/src/worker/storage/vfs_dataset.rs` - Storage tier selection

---

This architecture follows **clean separation of concerns**: metadata management (Master) is fully decoupled from data storage (Worker), with a custom high-performance RPC framework (`orpc`) enabling efficient communication between components. The use of Raft ensures metadata consistency and high availability, while the multi-tier storage system optimizes for both performance and cost.
