#[cfg(any(
    feature = "transfer-store-sqlite",
    feature = "transfer-store-mysql",
    feature = "transfer-store-postgres"
))]
use std::error::Error;

mod store;
pub use self::store::*;

mod task_cache;
pub use self::task_cache::{merge_summaries, JobRunKey, TaskCache};

#[cfg(feature = "transfer-store-mysql")]
mod forwarder;
#[cfg(feature = "transfer-store-mysql")]
pub use self::forwarder::{is_forwarded, Forwarder};

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
pub use self::postgres_store::{postgres_rustls_connector, PostgresTransferStore};

mod metrics;
pub(crate) use self::metrics::record_master_rpc;
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

mod runtime_config;
pub use self::runtime_config::{
    build_transfer_runtime_registry, TransferRuntimeConfig, MAX_PLAN_ENTRIES_CONFIG,
    MAX_TASKS_PER_TRANSFER_CONFIG, WORKER_MAX_CONCURRENT_TASKS_CONFIG,
};

mod dispatch;
mod plan;
pub use self::plan::{
    plan_segment_groups, segment_offsets, PlannedTransfer, TransferPlanner, SEGMENT_BYTES,
};

#[cfg(test)]
#[path = "tests/store_filter_test.rs"]
mod store_filter_test;

#[cfg(test)]
#[path = "tests/planner_test.rs"]
mod planner_test;

#[cfg(test)]
#[path = "tests/report_batch_test.rs"]
mod report_batch_test;

#[cfg(test)]
#[path = "tests/file_task_cache_test.rs"]
mod file_task_cache_test;

#[cfg(test)]
#[path = "tests/group_recovery_test.rs"]
mod group_recovery_test;

#[cfg(test)]
#[path = "tests/terminal_summary_test.rs"]
mod terminal_summary_test;

mod service;
pub use self::service::{progress_to_proto, task_summary_to_proto, TransferService};

#[cfg(test)]
#[path = "tests/finalize_test.rs"]
mod finalize_test;

#[cfg(test)]
#[path = "tests/report_phase_test.rs"]
mod report_phase_test;

#[cfg(test)]
#[path = "tests/service_test.rs"]
mod service_test;

#[cfg(test)]
#[path = "tests/planning_lease_test.rs"]
mod planning_lease_test;

#[cfg(test)]
#[path = "tests/fs_mode_command_test.rs"]
mod fs_mode_command_test;

#[cfg(test)]
#[path = "tests/fs_mode_store_test.rs"]
mod fs_mode_store_test;

pub use self::dispatch::TransferScheduler;

// Running jobs only need scheduler attention for task state changes or lease renewal.
pub(crate) const RUNNING_TRANSFER_CHECK_INTERVAL_MS: i64 = 5_000;

pub(crate) fn running_transfer_check_interval_ms(lease_ms: i64) -> i64 {
    RUNNING_TRANSFER_CHECK_INTERVAL_MS.min((lease_ms / 2).max(1))
}

/// Renders an error with its source chain.
///
/// Store backends report one stable "unavailable" message so callers can match
/// on the error kind; the backend cause (driver error, SQL state, OS error) is
/// appended here so that the failure reason survives into the client error and
/// the startup log instead of only reaching the server log.
#[cfg(any(
    feature = "transfer-store-sqlite",
    feature = "transfer-store-mysql",
    feature = "transfer-store-postgres"
))]
pub(crate) fn store_error_detail(err: &(dyn Error + 'static)) -> String {
    let mut detail = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    detail
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
