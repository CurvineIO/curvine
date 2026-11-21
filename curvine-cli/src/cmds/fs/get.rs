use super::common::copy_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Subcommand, Debug)]
pub enum GetCommand {
    /// Get file to local
    Get {
        #[clap(help = "Path of the file to get")]
        path: String,
        #[clap(help = "Local destination path")]
        local_path: PathBuf,
    },
}

impl GetCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            GetCommand::Get { path, local_path } => {
                let chunk_size = client.conf().client.read_chunk_size;
                get_file(&client, path, local_path, chunk_size).await
            }
        }
    }
}

/// Download `path_str` from `client` to `local_path`.
///
/// The remote reader is opened before anything is written locally, and the
/// downloaded bytes land in a temporary file next to the destination that is
/// renamed over it only after the download completes. A failed open or a
/// mid-copy error therefore fails before touching the local side and never
/// destroys a pre-existing destination file.
async fn get_file<FS, W, R>(
    client: &FS,
    path_str: &str,
    local_path: &Path,
    chunk_size: usize,
) -> CommonResult<()>
where
    FS: FileSystem<W, R>,
    R: Reader,
{
    let path = CurvineURI::new(path_str)?;

    // Check if the path exists and is a file
    let status = match client.get_status(&path).await {
        Ok(status) => status,
        Err(e) => return Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
    };
    if status.is_dir {
        return Err(format!("Cannot download '{}': Is a directory", path.full_path()).into());
    }

    // Determine the final local path. This only resolves and validates the
    // destination; it does not create or truncate anything.
    let final_local_path = resolve_local_dest(path.name(), local_path).await?;

    // Open the remote file before creating any local file, so a failed open
    // leaves a pre-existing local destination untouched.
    let mut reader = match client.open(&path).await {
        Ok(reader) => reader,
        Err(e) => {
            return Err(format!("Cannot open remote file '{}': {}", path.full_path(), e).into());
        }
    };

    println!("Downloading to {}", final_local_path.display());
    let bytes_read =
        download_to_local(&mut reader, path.full_path(), &final_local_path, chunk_size).await?;
    println!("Successfully downloaded {} bytes", bytes_read);
    Ok(())
}

/// Resolve the local destination of a download of a remote file named
/// `remote_name`: an existing directory or a trailing separator gets the
/// remote file name appended, a bare file name lands in the current
/// directory, anything else is used as is. Fails when the parent directory
/// does not exist, or when a trailing separator names a directory that
/// cannot be created. Never creates or truncates the destination itself.
async fn resolve_local_dest(remote_name: &str, local_path: &Path) -> CommonResult<PathBuf> {
    let local_str = local_path.to_string_lossy();
    let mut final_local_path = if (local_path.exists() && local_path.is_dir())
        || local_str.ends_with(std::path::MAIN_SEPARATOR)
    {
        // If local_path is a directory or ends with a path separator, append the filename from the remote path
        local_path.join(remote_name)
    } else if !local_str.contains(std::path::MAIN_SEPARATOR) {
        // If local_path is just a filename without path, use current directory
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(local_str.as_ref())
    } else {
        local_path.to_path_buf()
    };

    // Check if parent directory exists, refuse to execute if it doesn't
    if let Some(parent) = final_local_path.parent() {
        if !parent.exists() {
            return Err(format!(
                "Parent directory '{}' does not exist. Please create it first.",
                parent.display()
            )
            .into());
        }
    }

    // If the final_local_path itself is a directory (ends with separator), create it
    if final_local_path
        .to_string_lossy()
        .ends_with(std::path::MAIN_SEPARATOR)
    {
        if let Err(e) = fs::create_dir_all(&final_local_path).await {
            return Err(format!(
                "Cannot create directory '{}': {}",
                final_local_path.display(),
                e
            )
            .into());
        }
        // Append the filename to the directory path
        final_local_path = final_local_path.join(remote_name);
    }

    Ok(final_local_path)
}

/// Path of the temporary file a download writes to before it is renamed over
/// `dest`. It lives in the same directory as `dest`, so the rename stays on
/// one filesystem and is atomic.
fn temp_download_path(dest: &Path) -> PathBuf {
    let file_name = dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());
    let tmp_name = format!(".{}.cv-get-tmp.{}", file_name, std::process::id());
    match dest.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(tmp_name),
        _ => PathBuf::from(tmp_name),
    }
}

/// Copy `reader` into `dest` without ever destroying a pre-existing `dest`:
/// the bytes are written to a temporary file next to `dest` that is renamed
/// over `dest` only after the reader has been fully copied and completed.
/// Any failure removes the temporary file and leaves `dest` — and its
/// previous content, if any — untouched.
async fn download_to_local<R: Reader>(
    reader: &mut R,
    remote_path: &str,
    dest: &Path,
    chunk_size: usize,
) -> CommonResult<u64> {
    let tmp_path = temp_download_path(dest);
    let mut file = match fs::File::create(&tmp_path).await {
        Ok(file) => file,
        Err(e) => {
            return Err(format!("Cannot create local file '{}': {}", tmp_path.display(), e).into())
        }
    };

    let result = copy_and_complete(reader, &mut file, remote_path, chunk_size).await;
    // Close the file before renaming or removing it.
    drop(file);

    let bytes_read = match result {
        Ok(bytes_read) => bytes_read,
        Err(e) => {
            // Do not leave a partial download behind, and never remove the
            // destination itself: a failed download must keep a pre-existing
            // local file intact.
            let _ = fs::remove_file(&tmp_path).await;
            return Err(e);
        }
    };

    // The download completed: atomically replace the destination.
    match fs::rename(&tmp_path, dest).await {
        Ok(_) => Ok(bytes_read),
        Err(e) => {
            let _ = fs::remove_file(&tmp_path).await;
            Err(format!(
                "Cannot move '{}' to '{}': {}",
                tmp_path.display(),
                dest.display(),
                e
            )
            .into())
        }
    }
}

/// Copy the remaining bytes of `reader` to `file`, complete the reader, and
/// flush the file to disk, so the caller can rename it over the destination.
async fn copy_and_complete<R: Reader>(
    reader: &mut R,
    file: &mut fs::File,
    remote_path: &str,
    chunk_size: usize,
) -> CommonResult<u64> {
    let bytes_read = copy_to(reader, file, chunk_size)
        .await
        .map_err(|e| format!("Cannot download '{}': {}", remote_path, e))?;
    reader
        .complete()
        .await
        .map_err(|e| format!("Cannot complete read of '{}': {}", remote_path, e))?;
    file.sync_all().await?;
    Ok(bytes_read)
}

#[cfg(test)]
mod tests {
    use super::{download_to_local, get_file, resolve_local_dest, temp_download_path, GetCommand};
    use bytes::Bytes;
    use clap::Parser;
    use curvine_error::{FsError, FsResult};
    use curvine_fs_api::local::{LocalFilesystem, LocalReader, LocalWriter};
    use curvine_fs_api::{FileSystem, FsKind, Path as FsPath, Reader};
    use curvine_io::DataSlice;
    use curvine_model::{DeleteResult, FileStatus, SetAttrOpts};
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: GetCommand,
    }

    #[test]
    fn parses_get_arguments() {
        let cli = Cli::try_parse_from(["get", "get", "/data/f", "local.bin"]).unwrap();
        let GetCommand::Get { path, local_path } = cli.command;
        assert_eq!(path, "/data/f");
        assert_eq!(local_path, PathBuf::from("local.bin"));
    }

    /// A reader with preprogrammed chunk results: every read pops the next
    /// entry, an empty queue ends the stream, and `complete_result` controls
    /// `complete()`.
    struct MockReader {
        path: FsPath,
        status: FileStatus,
        chunks: VecDeque<FsResult<DataSlice>>,
        len: i64,
        complete_result: Option<FsResult<()>>,
        pos: i64,
        chunk: DataSlice,
    }

    impl MockReader {
        fn new(data: &'static [u8]) -> Self {
            Self {
                path: FsPath::from_str("/remote/data").unwrap(),
                status: FileStatus::default(),
                chunks: VecDeque::from([Ok(DataSlice::bytes(Bytes::from_static(data)))]),
                len: data.len() as i64,
                complete_result: None,
                pos: 0,
                chunk: DataSlice::Empty,
            }
        }

        /// Serves `data` once and then fails the next read, like a source
        /// that dies mid-copy while claiming `reported_len` bytes.
        fn failing_mid_copy(data: &'static [u8], reported_len: i64) -> Self {
            let mut reader = Self::new(data);
            reader.len = reported_len;
            reader
                .chunks
                .push_back(Err(FsError::common("simulated read failure")));
            reader
        }

        fn failing_complete(data: &'static [u8]) -> Self {
            let mut reader = Self::new(data);
            reader.complete_result = Some(Err(FsError::common("simulated complete failure")));
            reader
        }
    }

    impl Reader for MockReader {
        fn status(&self) -> &FileStatus {
            &self.status
        }

        fn path(&self) -> &FsPath {
            &self.path
        }

        fn len(&self) -> i64 {
            self.len
        }

        fn chunk_mut(&mut self) -> &mut DataSlice {
            &mut self.chunk
        }

        fn chunk_size(&self) -> usize {
            8
        }

        fn pos(&self) -> i64 {
            self.pos
        }

        fn pos_mut(&mut self) -> &mut i64 {
            &mut self.pos
        }

        async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
            self.chunks.pop_front().unwrap_or(Ok(DataSlice::Empty))
        }

        async fn seek(&mut self, _pos: i64) -> FsResult<()> {
            unimplemented!()
        }

        async fn complete(&mut self) -> FsResult<()> {
            self.complete_result.take().unwrap_or(Ok(()))
        }
    }

    /// A filesystem whose metadata lookups work but whose `open` always
    /// fails, to pin that a failed open leaves the local destination intact.
    struct FailingOpenFs {
        inner: LocalFilesystem,
    }

    impl FileSystem<LocalWriter, LocalReader> for FailingOpenFs {
        fn fs_kind(&self) -> FsKind {
            self.inner.fs_kind()
        }

        async fn mkdir(&self, path: &FsPath, create_parent: bool) -> FsResult<bool> {
            self.inner.mkdir(path, create_parent).await
        }

        async fn create(&self, path: &FsPath, overwrite: bool) -> FsResult<LocalWriter> {
            self.inner.create(path, overwrite).await
        }

        async fn append(&self, path: &FsPath) -> FsResult<LocalWriter> {
            self.inner.append(path).await
        }

        async fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path).await
        }

        async fn open(&self, _path: &FsPath) -> FsResult<LocalReader> {
            Err(FsError::common("simulated open failure"))
        }

        async fn rename(&self, src: &FsPath, dst: &FsPath) -> FsResult<bool> {
            self.inner.rename(src, dst).await
        }

        async fn delete(&self, path: &FsPath, recursive: bool) -> FsResult<DeleteResult> {
            self.inner.delete(path, recursive).await
        }

        async fn get_status(&self, path: &FsPath) -> FsResult<FileStatus> {
            self.inner.get_status(path).await
        }

        async fn list_status(&self, path: &FsPath) -> FsResult<Vec<FileStatus>> {
            self.inner.list_status(path).await
        }

        async fn set_attr(&self, path: &FsPath, opts: SetAttrOpts) -> FsResult<()> {
            self.inner.set_attr(path, opts).await
        }
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn file_uri(path: &Path) -> String {
        format!("file://{}", path.display())
    }

    // ---- destination resolution ----

    #[tokio::test]
    async fn resolves_absolute_file_path_as_is() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("dest.txt");
        let dest = resolve_local_dest("remote.txt", &target).await.unwrap();
        assert_eq!(dest, target);
    }

    #[tokio::test]
    async fn resolves_existing_directory_by_appending_remote_name() {
        let dir = tempfile::tempdir().unwrap();
        let dest = resolve_local_dest("remote.txt", dir.path()).await.unwrap();
        assert_eq!(dest, dir.path().join("remote.txt"));
    }

    #[tokio::test]
    async fn resolves_trailing_separator_like_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let with_sep = format!("{}{}", dir.path().display(), std::path::MAIN_SEPARATOR);
        let dest = resolve_local_dest("remote.txt", Path::new(&with_sep))
            .await
            .unwrap();
        assert_eq!(dest, dir.path().join("remote.txt"));
    }

    #[tokio::test]
    async fn resolves_bare_filename_into_current_dir() {
        let dest = resolve_local_dest("remote.txt", Path::new("out.txt"))
            .await
            .unwrap();
        assert_eq!(dest, std::env::current_dir().unwrap().join("out.txt"));
    }

    #[tokio::test]
    async fn missing_parent_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("no/such/dir/dest.txt");
        let err = resolve_local_dest("remote.txt", &target).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not exist"), "{}", msg);
    }

    #[test]
    fn temp_path_lives_next_to_the_destination() {
        let dest = Path::new("/tmp/some/dir/file.txt");
        let tmp = temp_download_path(dest);
        assert_eq!(tmp.parent(), Some(Path::new("/tmp/some/dir")));
        let name = tmp.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with(".file.txt."), "{}", name);
        assert!(name.contains("cv-get-tmp"), "{}", name);
    }

    // ---- download core ----

    #[tokio::test]
    async fn successful_download_renames_temp_over_destination() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.txt");
        std::fs::write(&dest, b"old content").unwrap();

        let mut reader = MockReader::new(b"new content");
        let bytes = download_to_local(&mut reader, "/remote/data", &dest, 4)
            .await
            .unwrap();
        assert_eq!(bytes, 11);
        assert_eq!(std::fs::read(&dest).unwrap(), b"new content");
        // The temporary file was renamed away, nothing else is left behind.
        assert_eq!(entries(dir.path()), vec!["out.txt".to_string()]);
    }

    #[tokio::test]
    async fn mid_copy_read_error_keeps_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        std::fs::write(&dest, b"original content").unwrap();

        // Claims 100 bytes, serves 5, then the read fails mid-copy.
        let mut reader = MockReader::failing_mid_copy(b"12345", 100);
        let err = download_to_local(&mut reader, "/remote/data", &dest, 4)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Cannot download '/remote/data'"), "{}", msg);
        assert!(msg.contains("simulated read failure"), "{}", msg);
        // The pre-existing file keeps its bytes...
        assert_eq!(std::fs::read(&dest).unwrap(), b"original content");
        // ...and no partial download is left behind.
        assert_eq!(entries(dir.path()), vec!["keep.txt".to_string()]);
    }

    #[tokio::test]
    async fn mid_copy_read_error_does_not_create_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("new.txt");

        let mut reader = MockReader::failing_mid_copy(b"12345", 100);
        assert!(download_to_local(&mut reader, "/remote/data", &dest, 4)
            .await
            .is_err());
        assert!(!dest.exists());
        assert!(entries(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn failed_complete_keeps_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        std::fs::write(&dest, b"original content").unwrap();

        let mut reader = MockReader::failing_complete(b"full content");
        let err = download_to_local(&mut reader, "/remote/data", &dest, 4)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Cannot complete read of '/remote/data'"),
            "{}",
            msg
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"original content");
        assert_eq!(entries(dir.path()), vec!["keep.txt".to_string()]);
    }

    // ---- end-to-end get through a FileSystem ----

    #[tokio::test]
    async fn get_file_downloads_to_a_new_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote.txt");
        std::fs::write(&remote, b"remote bytes").unwrap();
        let client = LocalFilesystem::new(4);

        let dest = dir.path().join("dest.txt");
        get_file(&client, &file_uri(&remote), &dest, 4)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"remote bytes");
        assert_eq!(
            entries(dir.path()),
            vec!["dest.txt".to_string(), "remote.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn get_file_into_an_existing_directory_appends_the_remote_name() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote.txt");
        std::fs::write(&remote, b"remote bytes").unwrap();
        let client = LocalFilesystem::new(4);

        let dest_dir = dir.path().join("downloads");
        std::fs::create_dir(&dest_dir).unwrap();
        get_file(&client, &file_uri(&remote), &dest_dir, 4)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(dest_dir.join("remote.txt")).unwrap(),
            b"remote bytes"
        );
    }

    #[tokio::test]
    async fn get_file_overwrites_an_existing_local_file_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote.txt");
        std::fs::write(&remote, b"remote bytes").unwrap();
        let client = LocalFilesystem::new(4);

        let dest = dir.path().join("dest.txt");
        std::fs::write(&dest, b"stale").unwrap();
        get_file(&client, &file_uri(&remote), &dest, 4)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"remote bytes");
        assert_eq!(
            entries(dir.path()),
            vec!["dest.txt".to_string(), "remote.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn failed_open_leaves_an_existing_local_file_intact() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote.txt");
        std::fs::write(&remote, b"remote bytes").unwrap();
        // get_status succeeds, but open always fails (e.g. all replicas are
        // on a stopped worker).
        let client = FailingOpenFs {
            inner: LocalFilesystem::new(4),
        };

        let dest = dir.path().join("keep.txt");
        std::fs::write(&dest, b"keep me").unwrap();
        let err = get_file(&client, &file_uri(&remote), &dest, 4)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Cannot open remote file"), "{}", msg);
        assert!(msg.contains("simulated open failure"), "{}", msg);
        // The pre-existing destination was not truncated or removed...
        assert_eq!(std::fs::read(&dest).unwrap(), b"keep me");
        // ...and no temporary file was created.
        assert_eq!(
            entries(dir.path()),
            vec!["keep.txt".to_string(), "remote.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn failed_open_does_not_create_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote.txt");
        std::fs::write(&remote, b"remote bytes").unwrap();
        let client = FailingOpenFs {
            inner: LocalFilesystem::new(4),
        };

        let dest = dir.path().join("new.txt");
        assert!(get_file(&client, &file_uri(&remote), &dest, 4)
            .await
            .is_err());
        assert!(!dest.exists());
        assert_eq!(entries(dir.path()), vec!["remote.txt".to_string()]);
    }

    #[tokio::test]
    async fn missing_remote_file_fails_before_touching_the_local_side() {
        let dir = tempfile::tempdir().unwrap();
        let client = LocalFilesystem::new(4);

        let dest = dir.path().join("dest.txt");
        let missing = dir.path().join("missing.txt");
        let err = get_file(&client, &file_uri(&missing), &dest, 4)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Cannot access"), "{}", err);
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn remote_directory_is_rejected_without_touching_the_local_side() {
        let dir = tempfile::tempdir().unwrap();
        let client = LocalFilesystem::new(4);

        let remote_dir = dir.path().join("adir");
        std::fs::create_dir(&remote_dir).unwrap();
        let dest = dir.path().join("dest.txt");
        let err = get_file(&client, &file_uri(&remote_dir), &dest, 4)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Is a directory"), "{}", err);
        assert!(!dest.exists());
    }
}
