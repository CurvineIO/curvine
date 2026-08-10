use super::content_summary::ContentSummary;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_unified_fs::UnifiedFileSystem;

pub(super) fn current_process_acl(client: &UnifiedFileSystem) -> (String, String, u32) {
    let uid = curvine_sys::get_uid();
    let gid = curvine_sys::get_gid();
    (
        curvine_sys::get_username_by_uid(uid).unwrap_or_else(|| uid.to_string()),
        curvine_sys::get_groupname_by_gid(gid).unwrap_or_else(|| gid.to_string()),
        client.conf().client.get_mode(),
    )
}

/// Calculates content summary (directory size, file count, directory count) on the client side
pub async fn calculate_content_summary(
    client: &UnifiedFileSystem,
    path: &CurvineURI,
) -> CommonResult<ContentSummary> {
    calculate_content_summary_impl(client, path).await
}

/// Implementation of calculate_content_summary that handles recursion properly
async fn calculate_content_summary_impl(
    client: &UnifiedFileSystem,
    path: &CurvineURI,
) -> CommonResult<ContentSummary> {
    // First check if the path exists and get its status
    let status = client.get_status(path).await?;

    if !status.is_dir {
        // For a file, return a simple summary with just the file's length
        return Ok(ContentSummary::for_file(status.len));
    }

    // For a directory, we need to recursively calculate the summary
    let mut summary = ContentSummary::for_empty_dir();
    let children = client.list_status(path).await?;

    for child in children {
        if child.is_dir {
            // Recursively get summary for subdirectories
            let child_path = CurvineURI::new(&child.path)?;
            // Use Box::pin to handle recursive async call
            let child_summary =
                Box::pin(calculate_content_summary_impl(client, &child_path)).await?;
            summary.merge(&child_summary);
        } else {
            // For files, just add their length and count
            summary.length += child.len;
            summary.file_count += 1;
        }
    }

    Ok(summary)
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
        format!("{} B", size)
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
    use super::format_permission;

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
}
