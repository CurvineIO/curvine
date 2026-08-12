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

mod metrics;
pub use self::metrics::TransferMetrics;

mod backend;
pub(crate) use self::backend::is_store_unavailable_error;
pub use self::backend::TransferStoreBackend;
pub(crate) use curvine_model::transfer_failure_message;

mod cluster_cache;
pub use self::cluster_cache::ClusterMetadataCache;

mod job_snapshot;
pub use self::job_snapshot::job_mount_snapshot;

mod planner;
pub use self::planner::{PlannedTransfer, TransferPlanner};

#[cfg(test)]
#[path = "tests/planner_test.rs"]
mod planner_test;

mod service;
pub use self::service::{progress_to_proto, task_summary_to_proto, TransferService};

mod scheduler;
pub use self::scheduler::TransferScheduler;

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

pub(crate) fn apply_task_report_progress(
    summary: &mut curvine_model::TransferProgress,
    previous: &curvine_model::TransferProgress,
    current: &curvine_model::TransferProgress,
    now_ms: i64,
) {
    summary.loaded_size = summary
        .loaded_size
        .saturating_sub(previous.loaded_size)
        .saturating_add(current.loaded_size)
        .max(0);
    summary.total_size = summary
        .total_size
        .saturating_sub(previous.total_size)
        .saturating_add(current.total_size)
        .max(0);
    summary.update_time = now_ms;
    summary.message = current.message.clone();
}
