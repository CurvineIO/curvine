use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_model::{ContentSummary, ContentSummarySource};
use curvine_unified_fs::UnifiedFileSystem;
use std::future::Future;

pub(super) fn current_process_acl(client: &UnifiedFileSystem) -> (String, String, u32) {
    let uid = curvine_sys::get_uid();
    let gid = curvine_sys::get_gid();
    (
        curvine_sys::get_username_by_uid(uid).unwrap_or_else(|| uid.to_string()),
        curvine_sys::get_groupname_by_gid(gid).unwrap_or_else(|| gid.to_string()),
        client.conf().client.get_mode(),
    )
}

fn merge_live_summary(into: &mut ContentSummary, other: &ContentSummary) {
    into.logical_bytes = into.logical_bytes.saturating_add(other.logical_bytes);
    into.file_count = into.file_count.saturating_add(other.file_count);
    into.directory_count = into.directory_count.saturating_add(other.directory_count);
}

/// Live recursive traversal used by `du --live` and snapshot fallback paths.
pub async fn calculate_content_summary_live(
    client: &UnifiedFileSystem,
    path: &CurvineURI,
) -> FsResult<ContentSummary> {
    let status = client.get_status(path).await?;

    if !status.is_dir {
        return Ok(ContentSummary::live_file(status.len.max(0) as u64));
    }

    let mut summary = ContentSummary::live_recursive(0, 0, 1);
    let children = client.list_status(path).await?;

    for child in children {
        if child.is_dir {
            let child_path = CurvineURI::new(&child.path)?;
            let child_summary =
                Box::pin(calculate_content_summary_live(client, &child_path)).await?;
            merge_live_summary(&mut summary, &child_summary);
        } else {
            summary.logical_bytes = summary
                .logical_bytes
                .saturating_add(child.len.max(0) as u64);
            summary.file_count = summary.file_count.saturating_add(1);
        }
    }

    Ok(summary)
}

async fn resolve_content_summary<S, L>(
    live: bool,
    snapshot: impl FnOnce() -> S,
    live_fn: impl FnOnce() -> L,
) -> FsResult<ContentSummary>
where
    S: Future<Output = FsResult<Option<ContentSummary>>>,
    L: Future<Output = FsResult<ContentSummary>>,
{
    if live {
        return live_fn().await;
    }

    match snapshot().await {
        Ok(Some(summary)) => Ok(summary),
        Ok(None) => live_fn().await,
        Err(FsError::Unsupported(_)) | Err(FsError::SnapshotNotReady(_)) => live_fn().await,
        Err(err) => Err(err),
    }
}

/// Resolves a content summary via dir-space snapshot when available, otherwise live scan.
pub async fn resolve_content_summary_for_path(
    client: &UnifiedFileSystem,
    path: &CurvineURI,
    live: bool,
    warn_fallback: &mut bool,
) -> CommonResult<ContentSummary> {
    let path_label = path.full_path().to_string();
    resolve_content_summary(
        live,
        || async { client.try_get_content_summary_snapshot(path).await },
        || async {
            if *warn_fallback {
                eprintln!(
                    "fs: dir-space snapshot unavailable for '{path_label}', using live recursive scan"
                );
                *warn_fallback = false;
            }
            calculate_content_summary_live(client, path).await
        },
    )
    .await
    .map_err(Into::into)
}

pub fn format_content_summary_source(summary: &ContentSummary) -> String {
    summary
        .source
        .as_ref()
        .map(|source| match source {
            ContentSummarySource::Unknown(value) => value.clone(),
            other => other.as_str().to_string(),
        })
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

/// Formats a size in bytes to a human-readable string (KB, MB, GB, etc.)
pub fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if size >= TB {
        format!("{:.1} TB", size as f64 / TB as f64)
    } else if size >= GB {
        format!("{:.1} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.1} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.1} KB", size as f64 / KB as f64)
    } else {
        format!("{size} B")
    }
}

/// Formats Unix permission bits to a rwx string.
pub fn format_permission(mode: u32) -> String {
    let mode = mode & 0o7777;

    let user_read = if mode & 0o400 != 0 { 'r' } else { '-' };
    let user_write = if mode & 0o200 != 0 { 'w' } else { '-' };
    let user_exec = match (mode & 0o4000 != 0, mode & 0o100 != 0) {
        (true, true) => 's',
        (true, false) => 'S',
        (false, true) => 'x',
        (false, false) => '-',
    };

    let group_read = if mode & 0o040 != 0 { 'r' } else { '-' };
    let group_write = if mode & 0o020 != 0 { 'w' } else { '-' };
    let group_exec = match (mode & 0o2000 != 0, mode & 0o010 != 0) {
        (true, true) => 's',
        (true, false) => 'S',
        (false, true) => 'x',
        (false, false) => '-',
    };

    let other_read = if mode & 0o004 != 0 { 'r' } else { '-' };
    let other_write = if mode & 0o002 != 0 { 'w' } else { '-' };
    let other_exec = match (mode & 0o1000 != 0, mode & 0o001 != 0) {
        (true, true) => 't',
        (true, false) => 'T',
        (false, true) => 'x',
        (false, false) => '-',
    };

    [
        user_read,
        user_write,
        user_exec,
        group_read,
        group_write,
        group_exec,
        other_read,
        other_write,
        other_exec,
    ]
    .iter()
    .collect()
}

/// Formats a Unix epoch timestamp in milliseconds using the local timezone.
pub fn format_epoch_ms_local(timestamp_ms: i64, fmt: &str) -> String {
    if timestamp_ms <= 0 {
        return "-".to_string();
    }

    let Some(datetime) = chrono::DateTime::from_timestamp_millis(timestamp_ms) else {
        return "-".to_string();
    };

    datetime
        .with_timezone(&chrono::Local)
        .format(fmt)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::ContentSummarySnapshot;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_format_permission() {
        assert_eq!(format_permission(0o755), "rwxr-xr-x");
        assert_eq!(format_permission(0o777), "rwxrwxrwx");
        assert_eq!(format_permission(0o644), "rw-r--r--");
        assert_eq!(format_permission(0o600), "rw-------");
        assert_eq!(format_permission(0o000), "---------");

        assert_eq!(format_permission(0o4755), "rwsr-xr-x");
        assert_eq!(format_permission(0o4644), "rwSr--r--");
        assert_eq!(format_permission(0o2775), "rwxrwsr-x");
        assert_eq!(format_permission(0o2664), "rw-rwSr--");
        assert_eq!(format_permission(0o1755), "rwxr-xr-t");
        assert_eq!(format_permission(0o1666), "rw-rw-rwT");
    }

    #[tokio::test]
    async fn content_summary_resolve_prefers_snapshot_by_default() {
        static SNAPSHOT_CALLS: AtomicUsize = AtomicUsize::new(0);
        static LIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
        SNAPSHOT_CALLS.store(0, Ordering::SeqCst);
        LIVE_CALLS.store(0, Ordering::SeqCst);

        let summary = resolve_content_summary(
            false,
            || async {
                SNAPSHOT_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(Some(ContentSummary::live_file(42)))
            },
            || async {
                LIVE_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_recursive(1, 1, 1))
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.logical_bytes, 42);
        assert_eq!(SNAPSHOT_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(LIVE_CALLS.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn content_summary_live_skips_snapshot() {
        static SNAPSHOT_CALLS: AtomicUsize = AtomicUsize::new(0);
        static LIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
        SNAPSHOT_CALLS.store(0, Ordering::SeqCst);
        LIVE_CALLS.store(0, Ordering::SeqCst);

        let summary = resolve_content_summary(
            true,
            || async {
                SNAPSHOT_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(Some(ContentSummary::live_file(99)))
            },
            || async {
                LIVE_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_recursive(7, 2, 1))
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.logical_bytes, 7);
        assert_eq!(SNAPSHOT_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(LIVE_CALLS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn content_summary_snapshot_not_ready_falls_back_once() {
        let summary = resolve_content_summary(
            false,
            || async { Err(FsError::snapshot_not_ready("/a")) },
            || async { Ok(ContentSummary::live_recursive(10, 1, 1)) },
        )
        .await
        .unwrap();

        assert_eq!(summary.logical_bytes, 10);
    }

    #[test]
    fn content_summary_verbose_source_and_generation() {
        let summary = ContentSummary::from_snapshot(
            100,
            2,
            1,
            ContentSummarySource::Global,
            ContentSummarySnapshot {
                generation: 9,
                started_ms: 1_000,
                finished_ms: 2_000,
                anomaly_count: 0,
                degraded: false,
            },
        );
        assert_eq!(format_content_summary_source(&summary), "GLOBAL");
        assert_eq!(summary.snapshot.as_ref().unwrap().generation, 9);
    }
}
