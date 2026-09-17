mod store;
pub use self::store::*;

mod memory_store;
pub use self::memory_store::MemoryTransferStore;

#[cfg(feature = "transfer-store-sqlite")]
mod sqlite_store;
#[cfg(feature = "transfer-store-sqlite")]
pub use self::sqlite_store::SqliteTransferStore;

#[cfg(feature = "transfer-store-mysql")]
mod mysql_store;
#[cfg(feature = "transfer-store-mysql")]
pub use self::mysql_store::MysqlTransferStore;

#[cfg(feature = "transfer-store-postgres")]
mod postgres_store;
#[cfg(feature = "transfer-store-postgres")]
pub use self::postgres_store::PostgresTransferStore;

mod metrics;
pub use self::metrics::TransferMetrics;

mod backend;
pub(crate) use self::backend::is_store_unavailable_error;
pub use self::backend::TransferStoreBackend;
pub(crate) use curvine_model::transfer_failure_message;

mod cluster_cache;
pub use self::cluster_cache::ClusterMetadataCache;
pub(crate) use self::cluster_cache::MountSnapshotError;

mod job_snapshot;
pub(crate) use self::job_snapshot::job_mount_snapshot_with_version_async;
pub use self::job_snapshot::{job_mount_snapshot, job_mount_snapshot_async};

mod planner;
pub use self::planner::{
    plan_segment_groups, segment_offsets, PlannedTransfer, TransferPlanner, SEGMENT_BYTES,
};

#[cfg(test)]
#[path = "tests/planner_test.rs"]
mod planner_test;

#[cfg(test)]
#[path = "tests/report_batch_test.rs"]
mod report_batch_test;

#[cfg(test)]
#[path = "tests/group_recovery_test.rs"]
mod group_recovery_test;

mod service;
pub use self::service::{progress_to_proto, task_summary_to_proto, TransferService};

#[cfg(test)]
#[path = "tests/service_test.rs"]
mod service_test;

mod scheduler;
pub use self::scheduler::TransferScheduler;

// Running jobs only need scheduler attention for task state changes or lease renewal.
pub(crate) const RUNNING_TRANSFER_CHECK_INTERVAL_MS: i64 = 5_000;

pub(crate) fn running_transfer_check_interval_ms(lease_ms: i64) -> i64 {
    RUNNING_TRANSFER_CHECK_INTERVAL_MS.min((lease_ms / 2).max(1))
}

mod handler;
pub use self::handler::TransferHandler;

#[cfg(feature = "transfer-web")]
mod router_handler;
#[cfg(feature = "transfer-web")]
pub use self::router_handler::TransferRouterHandler;

#[cfg(feature = "transfer-server")]
mod transfer_server;
#[cfg(feature = "transfer-server")]
pub use self::transfer_server::TransferServer;
