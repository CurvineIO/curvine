use bytes::BytesMut;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use std::io::Write;

#[derive(Subcommand, Debug)]
pub enum CatCommand {
    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,
    },
}

impl CatCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CatCommand::Cat { path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            eprintln!("cat: Cannot display '{}': Is a directory", path.full_path());
                            return Ok(());
                        }

                        // Open the file for reading
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut stdout = std::io::stdout();
                                if let Err(e) = copy_to(&mut reader, &mut stdout, chunk_size).await
                                {
                                    eprintln!("cat: Cannot display '{}': {}", path.full_path(), e);
                                    return Err(e);
                                }

                                // Complete the read
                                if let Err(e) = reader.complete().await {
                                    eprintln!("Error completing read: {}", e);
                                    return Err(e.into());
                                }

                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("Error opening file: {}", e);
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("cat: Cannot access '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}

/// Copy the remaining bytes of `reader` to `out` unchanged.
///
/// File contents are arbitrary bytes and a multi-byte UTF-8 character can
/// straddle two chunks, so the data must never be decoded on the way through.
async fn copy_to<R: Reader, W: Write>(
    reader: &mut R,
    out: &mut W,
    chunk_size: usize,
) -> CommonResult<()> {
    let mut buffer = BytesMut::zeroed(chunk_size);
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
        out.write_all(&buffer[..read_len])?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::copy_to;
    use bytes::Bytes;
    use curvine_error::FsResult;
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::state::FileStatus;
    use curvine_fs_api::{Path, Reader};
    use curvine_io::DataSlice;
    use std::io::{self, Write};

    /// Reports `len` bytes but serves only `data`, then hits end of file.
    struct TruncatedReader {
        path: Path,
        status: FileStatus,
        data: Option<Bytes>,
        len: i64,
        pos: i64,
        chunk: DataSlice,
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

    impl Write for BrokenPipe {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn local_reader(dir: &tempfile::TempDir, content: &[u8], chunk_size: usize) -> LocalReader {
        let file = dir.path().join("data");
        std::fs::write(&file, content).unwrap();

        let path = Path::from_str(file.to_str().unwrap()).unwrap();
        LocalReader::new(&path, chunk_size).unwrap()
    }

    async fn cat_bytes(content: &[u8], reader_chunk: usize, cat_chunk: usize) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, content, reader_chunk);
        let mut out = Vec::new();
        copy_to(&mut reader, &mut out, cat_chunk).await.unwrap();
        out
    }

    #[tokio::test]
    async fn copies_multi_byte_text_split_across_chunks() {
        // 4-byte reads split the 3-byte characters across chunk boundaries.
        let content = "中文目录/数据\n".as_bytes();
        assert_eq!(cat_bytes(content, 5, 4).await, content);
    }

    #[tokio::test]
    async fn copies_binary_data_unchanged() {
        let content: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert_eq!(cat_bytes(&content, 64, 100).await, content);
    }

    #[tokio::test]
    async fn copies_empty_file() {
        assert!(cat_bytes(b"", 64, 100).await.is_empty());
    }

    #[tokio::test]
    async fn fails_on_premature_end_of_file() {
        let mut reader = TruncatedReader {
            path: Path::from_str("/data").unwrap(),
            status: FileStatus::default(),
            data: Some(Bytes::from_static(b"12345")),
            len: 10,
            pos: 0,
            chunk: DataSlice::Empty,
        };
        let mut out = Vec::new();
        let err = copy_to(&mut reader, &mut out, 4).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("unexpected end of file at 5 of 10 bytes"));
        assert_eq!(out, b"12345");
    }

    #[tokio::test]
    async fn returns_write_error_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, b"hello", 64);
        assert!(copy_to(&mut reader, &mut BrokenPipe, 100).await.is_err());
    }
}
