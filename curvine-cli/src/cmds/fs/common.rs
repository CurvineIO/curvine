use super::content_summary::ContentSummary;
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::CurvineURI;
use orpc::CommonResult;

/// Calculates content summary (directory size, file count, directory count) on the client side
pub async fn calculate_content_summary(
    client: &CurvineFileSystem,
    path: &CurvineURI,
) -> CommonResult<ContentSummary> {
    calculate_content_summary_impl(client, path).await
}

/// Implementation of calculate_content_summary that handles recursion properly
async fn calculate_content_summary_impl(
    client: &CurvineFileSystem,
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
