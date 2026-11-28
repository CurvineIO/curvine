use super::common::copy_n_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::io::AsyncWrite;

#[derive(Subcommand, Debug)]
pub enum CatCommand {
    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,

        #[clap(long, default_value_t = 0, help = "Byte offset to start reading at")]
        offset: u64,

        #[clap(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Maximum number of bytes to print [default: the rest of the file]"
        )]
        length: Option<u64>,
    },
}

/// Copies at most `length` bytes of `reader`, starting at `offset`, to `out`
/// unchanged and returns the number of bytes copied. `None` copies the rest
/// of the file. An offset at or past the end of the file copies nothing.
async fn copy_range<R: Reader, W: AsyncWrite + Unpin>(
    reader: &mut R,
    out: &mut W,
    offset: u64,
    length: Option<u64>,
    chunk_size: usize,
) -> CommonResult<u64> {
    let offset = i64::try_from(offset).unwrap_or(i64::MAX);
    if offset >= reader.len() {
        return Ok(0);
    }
    if offset > 0 {
        reader.seek(offset).await?;
    }
    copy_n_to(reader, out, chunk_size, length.unwrap_or(u64::MAX)).await
}

impl CatCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CatCommand::Cat {
                path,
                offset,
                length,
            } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            return Err(format!(
                                "Cannot display '{}': Is a directory",
                                path.full_path()
                            )
                            .into());
                        }

                        // Open the file for reading
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut stdout = tokio::io::stdout();
                                let copy = copy_range(
                                    &mut reader,
                                    &mut stdout,
                                    *offset,
                                    *length,
                                    chunk_size,
                                );
                                if let Err(e) = copy.await {
                                    return Err(format!(
                                        "Cannot display '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                // Complete the read
                                if let Err(e) = reader.complete().await {
                                    return Err(format!(
                                        "Cannot complete read of '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                Ok(())
                            }
                            Err(e) => {
                                Err(format!("Cannot open '{}': {}", path.full_path(), e).into())
                            }
                        }
                    }
                    Err(e) => Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{copy_range, CatCommand};
    use clap::Parser;
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::Path;

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: CatCommand,
    }

    fn parse_range(args: &[&str]) -> Result<(u64, Option<u64>), clap::Error> {
        let cli = Cli::try_parse_from(args)?;
        // `CatCommand` has a single variant, so the pattern is irrefutable.
        let CatCommand::Cat { offset, length, .. } = cli.command;
        Ok((offset, length))
    }

    /// Runs `copy_range` over a local file holding `content`.
    async fn range_bytes(content: &[u8], offset: u64, length: Option<u64>) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("data");
        std::fs::write(&file, content).unwrap();

        let path = Path::from_str(file.to_str().unwrap()).unwrap();
        let mut reader = LocalReader::new(&path, 64).unwrap();
        let mut out = Vec::new();
        let copy = copy_range(&mut reader, &mut out, offset, length, 100);
        let copied = copy.await.unwrap();
        assert_eq!(copied, out.len() as u64);
        out
    }

    fn sample(len: usize) -> Vec<u8> {
        (0..=255u8).cycle().take(len).collect()
    }

    #[test]
    fn range_defaults_to_the_whole_file() {
        assert_eq!(parse_range(&["cat", "cat", "/a.bin"]).unwrap(), (0, None));
    }

    #[test]
    fn parses_offset_and_length() {
        let args = ["cat", "cat", "--offset", "100", "--length", "20", "/a"];
        assert_eq!(parse_range(&args).unwrap(), (100, Some(20)));
    }

    #[test]
    fn rejects_negative_offset_and_zero_or_negative_length() {
        let args = ["cat", "cat", "--offset", "-1", "/a.bin"];
        assert!(parse_range(&args).is_err());
        let args = ["cat", "cat", "--length", "0", "/a.bin"];
        assert!(parse_range(&args).is_err());
        let args = ["cat", "cat", "--length", "-1", "/a.bin"];
        assert!(parse_range(&args).is_err());
        let args = ["cat", "cat", "--offset", "abc", "/a.bin"];
        assert!(parse_range(&args).is_err());
    }

    #[tokio::test]
    async fn prints_the_whole_file_by_default() {
        let content = sample(300);
        assert_eq!(range_bytes(&content, 0, None).await, content);
    }

    #[tokio::test]
    async fn reads_the_first_bytes() {
        let content = sample(300);
        assert_eq!(range_bytes(&content, 0, Some(10)).await, &content[..10]);
    }

    #[tokio::test]
    async fn seeks_and_reads_exactly_the_requested_bytes() {
        let content = sample(300);
        let out = range_bytes(&content, 100, Some(20)).await;
        assert_eq!(out, &content[100..120]);
    }

    #[tokio::test]
    async fn reads_to_the_end_without_a_length() {
        let content = sample(300);
        assert_eq!(range_bytes(&content, 250, None).await, &content[250..]);
    }

    #[tokio::test]
    async fn length_past_end_returns_the_remaining_bytes() {
        let content = sample(300);
        let out = range_bytes(&content, 290, Some(4096)).await;
        assert_eq!(out, &content[290..]);
    }

    #[tokio::test]
    async fn offset_at_or_past_end_prints_nothing() {
        let content = sample(300);
        assert!(range_bytes(&content, 300, None).await.is_empty());
        assert!(range_bytes(&content, 301, Some(5)).await.is_empty());
        assert!(range_bytes(&content, u64::MAX, None).await.is_empty());
        assert!(range_bytes(b"", 0, Some(5)).await.is_empty());
    }

    #[tokio::test]
    async fn keeps_a_binary_range_unchanged() {
        // The range starts inside a multi-byte character and holds invalid UTF-8.
        let mut content = "中文目录".as_bytes().to_vec();
        content.extend_from_slice(&[0x00, 0xff, 0xfe, 0x80]);
        let out = range_bytes(&content, 4, Some(10)).await;
        assert_eq!(out, &content[4..14]);
    }
}
