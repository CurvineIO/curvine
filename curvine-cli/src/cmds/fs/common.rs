use super::content_summary::ContentSummary;
use bytes::BytesMut;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::io::{AsyncWrite, AsyncWriteExt};

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

/// Copy the remaining bytes of `reader` to `out` unchanged and return the
/// number of bytes copied.
///
/// File contents are arbitrary bytes and a multi-byte UTF-8 character can
/// straddle two chunks, so the data must never be decoded on the way through.
/// Only the bytes actually returned by the reader are written, and a reader
/// that ends before its reported length is an error rather than a short copy.
pub(super) async fn copy_to<R: Reader, W: AsyncWrite + Unpin>(
    reader: &mut R,
    out: &mut W,
    chunk_size: usize,
) -> CommonResult<u64> {
    let mut buffer = BytesMut::zeroed(chunk_size);
    let mut copied = 0u64;
    while reader.has_remaining() {
        let to_read = chunk_size.min(reader.remaining() as usize);
        let read_len = reader.read_full(&mut buffer[..to_read]).await?;
        if read_len == 0 {
            return Err(format!(
                "unexpected end of file at {} of {} bytes",
                reader.pos(),
                reader.len()
            )
            .into());
        }
        out.write_all(&buffer[..read_len]).await?;
        copied += read_len as u64;
    }
    out.flush().await?;
    Ok(copied)
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
    use super::{copy_to, format_permission};
    use bytes::Bytes;
    use curvine_error::FsResult;
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::state::FileStatus;
    use curvine_fs_api::{Path, Reader};
    use curvine_io::DataSlice;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    /// Reports `len` bytes but serves only `data`, then hits end of file.
    struct TruncatedReader {
        path: Path,
        status: FileStatus,
        data: Option<Bytes>,
        len: i64,
        pos: i64,
        chunk: DataSlice,
    }

    impl TruncatedReader {
        fn new(data: &'static [u8], len: i64) -> Self {
            Self {
                path: Path::from_str("/data").unwrap(),
                status: FileStatus::default(),
                data: Some(Bytes::from_static(data)),
                len,
                pos: 0,
                chunk: DataSlice::Empty,
            }
        }
    }

    impl Reader for TruncatedReader {
        fn status(&self) -> &FileStatus {
            &self.status
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn len(&self) -> i64 {
            self.len
        }

        fn chunk_mut(&mut self) -> &mut DataSlice {
            &mut self.chunk
        }

        fn chunk_size(&self) -> usize {
            self.len as usize
        }

        fn pos(&self) -> i64 {
            self.pos
        }

        fn pos_mut(&mut self) -> &mut i64 {
            &mut self.pos
        }

        async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
            Ok(self.data.take().map_or(DataSlice::Empty, DataSlice::bytes))
        }

        async fn seek(&mut self, _pos: i64) -> FsResult<()> {
            unimplemented!()
        }

        async fn complete(&mut self) -> FsResult<()> {
            Ok(())
        }
    }

    struct BrokenPipe;

    impl AsyncWrite for BrokenPipe {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn local_reader(dir: &tempfile::TempDir, content: &[u8], chunk_size: usize) -> LocalReader {
        let file = dir.path().join("data");
        std::fs::write(&file, content).unwrap();

        let path = Path::from_str(file.to_str().unwrap()).unwrap();
        LocalReader::new(&path, chunk_size).unwrap()
    }

    async fn copy_bytes(content: &[u8], reader_chunk: usize, copy_chunk: usize) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, content, reader_chunk);
        let mut out = Vec::new();
        let copied = copy_to(&mut reader, &mut out, copy_chunk).await.unwrap();
        assert_eq!(copied, content.len() as u64);
        out
    }

    #[tokio::test]
    async fn copies_multi_byte_text_split_across_chunks() {
        // 4-byte reads split the 3-byte characters across chunk boundaries.
        let content = "中文目录/数据\n".as_bytes();
        assert_eq!(copy_bytes(content, 5, 4).await, content);
    }

    #[tokio::test]
    async fn copies_binary_data_unchanged() {
        let content: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert_eq!(copy_bytes(&content, 64, 100).await, content);
    }

    #[tokio::test]
    async fn copies_empty_file() {
        assert!(copy_bytes(b"", 64, 100).await.is_empty());
    }

    #[tokio::test]
    async fn fails_on_premature_end_of_file() {
        let mut reader = TruncatedReader::new(b"12345", 10);
        let mut out = Vec::new();
        let err = copy_to(&mut reader, &mut out, 4).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("unexpected end of file at 5 of 10 bytes"));
        assert_eq!(out, b"12345");
    }

    #[tokio::test]
    async fn copy_to_file_writes_only_bytes_read() {
        // A short read must not leave a zero-filled tail in the local file.
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("download");
        let mut file = tokio::fs::File::create(&target).await.unwrap();
        let mut reader = TruncatedReader::new(b"12345", 10);
        assert!(copy_to(&mut reader, &mut file, 8).await.is_err());
        file.flush().await.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"12345");
    }

    #[tokio::test]
    async fn returns_write_error_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, b"hello", 64);
        assert!(copy_to(&mut reader, &mut BrokenPipe, 100).await.is_err());
    }

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
