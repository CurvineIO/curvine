mod store;
pub use self::store::*;

mod memory_store;
pub use self::memory_store::MemoryTransferStore;

mod sqlite_store;
pub use self::sqlite_store::SqliteTransferStore;

mod mysql_store;
pub use self::mysql_store::MysqlTransferStore;

mod metrics;
pub use self::metrics::{MetadataReplicaRefreshObservation, TransferMetrics};

mod backend;
pub(crate) use self::backend::is_store_unavailable_error;
pub use self::backend::TransferStoreBackend;

mod cluster_cache;
pub use self::cluster_cache::ClusterMetadataCache;

mod cv_metadata_reader;
pub use self::cv_metadata_reader::{
    CvMetadataReader, DisabledCvMetadataReader, MasterCvMetadataReader, MetadataReplicaReader,
};

mod job_snapshot;
pub use self::job_snapshot::job_mount_snapshot;

mod planner;
pub use self::planner::{PlannedTransfer, TransferPlanner};

mod service;
pub use self::service::{progress_to_proto, task_summary_to_proto, TransferService};

mod scheduler;
pub use self::scheduler::TransferScheduler;

mod handler;
pub use self::handler::TransferHandler;

mod router_handler;
pub use self::router_handler::TransferRouterHandler;

mod transfer_server;
pub use self::transfer_server::TransferServer;

pub(crate) fn apply_task_report_progress(
    summary: &mut curvine_common::state::TransferProgress,
    previous: &curvine_common::state::TransferProgress,
    current: &curvine_common::state::TransferProgress,
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

pub(crate) fn transfer_failure_message(
    kind: curvine_common::state::TransferKind,
    source_path: &str,
    target_path: &str,
    err: &curvine_common::error::FsError,
) -> String {
    use curvine_common::error::ErrorKind;

    let source_label = match kind {
        curvine_common::state::TransferKind::Load => "source object",
        curvine_common::state::TransferKind::Export => "source file",
    };
    match err.kind() {
        ErrorKind::FileNotFound | ErrorKind::Expired => {
            format!("{source_label} not found: {source_path}")
        }
        ErrorKind::FileAlreadyExists => format!("target already exists: {target_path}"),
        ErrorKind::ParentNotDir | ErrorKind::NotADirectory => {
            format!("target parent is not a directory: {target_path}")
        }
        ErrorKind::IsADirectory => format!("target is a directory: {target_path}"),
        ErrorKind::ReadOnly => format!("target is read-only: {target_path}"),
        ErrorKind::DiskOutOfSpace => {
            format!("Not enough Curvine worker disk space to transfer {source_path}; free space and retry")
        }
        ErrorKind::Timeout => format!(
            "Timed out while accessing {source_path}; verify storage connectivity and retry"
        ),
        ErrorKind::IO | ErrorKind::Ufs | ErrorKind::Pipeline => format!(
            "Cannot access transfer storage for {source_path}; verify the mount, credentials, and network connectivity"
        ),
        ErrorKind::TransferStoreUnavailable => {
            "Transfer metadata store is unavailable; retry after it recovers".to_string()
        }
        ErrorKind::TransferOverloaded => "Transfer service is busy; retry later".to_string(),
        _ => format!(
            "Transfer from {source_path} to {target_path} failed; check the Transfer service logs"
        ),
    }
}
