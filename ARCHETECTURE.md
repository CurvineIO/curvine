# Curvine Architecture

Curvine implements a distributed caching filesystem composed of Raft-backed master nodes, SSD-informed worker nodes, and a client stack that streams blocks directly from workers after coordinating metadata through the masters. This document expands on the high-level design, focusing on the relationships and request flows between the major components.

## System Topology

- **Masters (`curvine-server/src/master/`)** coordinate namespace metadata, placement policy, replication, mount state, and job management. Each master embeds a `JournalSystem` that replays Raft logs into an in-memory `MasterFilesystem`.
- **Workers (`curvine-server/src/worker/`)** manage local block stores under directories defined in the cluster configuration. They register with masters, issue heartbeats, and expose data through RPC handlers.
- **Shared crates (`curvine-common/`, `orpc/`)** provide configuration models, Raft primitives, networking, and RPC plumbing.
- **Clients (`curvine-client/`)** communicate with masters to resolve metadata, then stream or replicate blocks against workers. Unified filesystem support adds cache-on-read semantics against external storage backends.

## Master Nodes

### Runtime Composition

`Master::with_conf` (`curvine-server/src/master/master_server.rs`) initializes logging, metrics, and the `JournalSystem`. It constructs the core services:

- `MasterFilesystem`: namespace and placement logic, backed by `FsDir` plus worker/mount managers.
- `MasterActor`: background executor for maintenance tasks.
- `JobManager`: submits and tracks load jobs, leveraging mount metadata.
- `MasterService`: wraps RPC/web handlers (`MasterHandler`, `MasterRouterHandler`) and exposes the runtime to ORPC.

When `Master::start` runs, it first awaits the Raft role listener, then starts the RPC server, web server, actor, mount restore, and job manager in that order.

### Leading Master Responsibilities

The elected leader serves client metadata calls via `MasterHandler` (`curvine-server/src/master/master_handler.rs`). Key flows include:

- **File creation/write**: `CreateFile` allocates inodes and leases; `AddBlock` validates leases and previous block commitments before selecting workers through `WorkerManager::choose_worker`.
- **Block reporting**: masters accept periodic updates (`WorkerBlockReport`) to synchronize placement.
- **Completion**: `CompleteFile` verifies lease ownership and length consistency before marking files complete.
- **Mounts and jobs**: requests delegate to `MountManager` and `JobHandler`.

### Standby Masters

Every standby master runs the same stack. `RoleMonitor` (`curvine-common/src/raft/role_monitor.rs`) tracks leadership transitions; a follower remains in sync by continuously applying Raft entries via the journal loader. When promoted, it already holds an up-to-date namespace snapshot and only needs to start serving RPC traffic.

## Journal and Raft Subsystem

- **Log Storage**: `RocksLogStorage` (`curvine-common/src/raft/storage`) persists Raft entries and snapshots under the configured journal directory.
- **Entry Production**: `JournalWriter` serializes metadata mutations and forwards them to the Raft group (`curvine-server/src/master/journal/journal_writer.rs`).
- **Entry Application**: `JournalLoader` replays committed entries into `FsDir`, updates mount tables, and injects TTL bucket updates to enforce expiration policies (`curvine-server/src/master/journal/journal_loader.rs`).
- **Snapshots**: During startup, existing snapshots trigger a wipe of the RocksDB data directory before restoration, ensuring no duplicate in-memory trees are created.
- **Role Observation**: `RoleMonitor` and `MasterMonitor` expose listeners so higher-level services only start once the node enters `Leader` or `Follower` state. Heartbeats and web endpoints use this status to report cluster health.

## Worker Nodes

### Startup

`Worker::with_conf` (`curvine-server/src/worker/worker_server.rs`) prepares a runtime, constructs `WorkerService`, and instantiates RPC and web servers. It derives the advertised hostname using the `POD_IP` environment variable when present, otherwise `NetUtils::local_ip`. The resulting `WorkerAddress` includes RPC and web ports sourced from the configuration (`curvine-common/src/conf/cluster_conf.rs`).

### Block Storage Management

- `BlockStore` (`curvine-server/src/worker/block/block_store.rs`) wraps directory datasets, handles block lifecycle (create, append, finalize), and computes storage statistics.
- `BlockActor` (`curvine-server/src/worker/block/block_actor.rs`) is the orchestrator that:
  - Registers the worker with `HeartbeatStatus::Start`, sending storage metadata (`get_and_check_storages`).
  - Issues a full block report upon startup, sliced according to `block_report_limit`.
  - Schedules periodic heartbeats and incremental reports through a `ScheduledExecutor`, keeping `WorkerManager` informed of changes.
- Shutdown hooks notify the master with `HeartbeatStatus::End`.

### Replication and Tasks

`WorkerReplicationManager` marries the worker’s runtime with the master’s replication instructions so background copy/delete tasks execute asynchronously (`curvine-server/src/worker/replication`). Task management uses `TaskManager` to coordinate job execution and resource limits.

## Worker Registration and Metadata Tracking

On the master side, `WorkerManager::heartbeat` (`curvine-server/src/master/fs/worker_manager.rs`) validates cluster IDs, logs registration events, normalizes storage info, and inserts or updates entries in `WorkerMap`. `WorkerMap::insert` enforces consistent worker IDs and addresses, preserves decommission status, and maintains hot/lost worker sets. These structures feed placement policies (random, load-based, local) via `WorkerPolicyAdapter`.

## Client Access Patterns

### Runtime and RPC

Clients build an `FsContext` (`curvine-client/src/file/fs_context.rs`) that encapsulates cluster configuration, commit hooks, metrics, and a `ClusterConnector`. `FsClient` then issues RPCs against masters using ORPC (`curvine-client/src/file/fs_client.rs`).

### Read Flow

1. `FsClient::get_block_locations` fetches the block layout from the master.
2. `FsReader` (`curvine-client/src/file/fs_reader.rs`) constructs buffered/paralleled readers (`FsReaderBuffer`, `FsReaderParallel`) to stream data from each replica.
3. Reader adapters manage chunked transfers, read-ahead, and seeking while sharing error monitors so partial failures bubble up cleanly (`curvine-client/src/file/fs_reader_buffer.rs`).
4. Failed worker addresses are tracked inside `FsContext`, allowing future replica selections to exclude unhealthy nodes.

### Write Flow

1. `FsWriter` (`curvine-client/src/file/fs_writer.rs`) requests block assignments through `FsClient::add_block`, passing the client’s `ClientAddress`.
2. `BlockWriter` (`curvine-client/src/block/block_writer.rs`) constructs per-replica adapters: `BlockWriterLocal` for short-circuit writes when the worker is local, or `BlockWriterRemote` otherwise.
3. Each write chunk is forwarded to all replicas concurrently; failures mark the worker as unhealthy and surface to the caller.
4. `FsWriterBase::complete` finalizes the block and calls `FsClient::complete_file`, which drives `MasterFilesystem::complete_file` to validate lease ownership and file length before marking the inode complete (`curvine-server/src/master/fs/master_filesystem.rs`).

### Unified Filesystem and Caching

`UnifiedFileSystem` (`curvine-client/src/unified/unified_filesystem.rs`) adds mount-aware behavior:

- `MountCache` maintains bidirectional mappings between Curvine (`cv://`) and underlying UFS paths, refreshing on a TTL.
- `open` checks cache validity by comparing UFS metadata against cached Curvine metadata; hits stream from Curvine, misses optionally fall back to the UFS reader and may trigger an asynchronous cache warmup (`async_cache`).
- Asynchronous warmups use the job RPC channel (`JobMasterClient::submit_load_job`, `curvine-client/src/rpc/job_master_client.rs`) to ask the master to schedule background loading of UFS data into Curvine storage.

## Supporting Services and Observability

- **Replication orchestration**: `MasterReplicationManager` assigns recovery tasks, using worker status and policies to balance replicas (`curvine-server/src/master/replication`).
- **Mount lifecycle**: `MountManager` wraps mount additions, removals, and TTL policies, persisting changes via journal entries (`curvine-server/src/master/mount`).
- **Job system**: `JobManager` and `JobHandler` orchestrate data loading workflows, reporting progress through RPC.
- **Metrics**: `MasterMetrics` and `WorkerMetrics` expose Prometheus counters for filesystem and block operations, while web routers offer operational APIs (`curvine-web`).

## Request Flow Summary

1. **Client write**: client obtains metadata from leader master → master allocates block and returns worker list → client streams data to workers → client calls complete → master logs completion through Raft → replicas acknowledged.
2. **Client read**: client gets block locations → selects replicas based on health/tracking → streams data directly from workers → reader handles retries and read-ahead.
3. **Worker lifecycle**: worker starts → registers with master → sends full block report → periodic heartbeats/updates maintain metadata accuracy → on shutdown, sends termination heartbeat.
4. **Failover**: Raft leadership change promotes a standby master → new leader starts serving RPC once `RoleMonitor` signals readiness → clients reconnect using cluster connector logic that retries masters from the configured list.

This architecture balances strong metadata consistency (via Raft) with high-throughput data transfer (direct client-to-worker streaming), providing a flexible cache layer that can hydrate from external storage and scale horizontally through additional workers or replicated masters.
