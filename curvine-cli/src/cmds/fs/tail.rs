use super::common::copy_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::io::AsyncWrite;

/// Number of trailing bytes printed by default, the same as `hdfs dfs -tail`.
pub(super) const DEFAULT_TAIL_BYTES: u64 = 1024;

#[derive(Subcommand, Debug)]
pub enum TailCommand {
    /// Display the last bytes of a file
    Tail {
        #[clap(help = "Path of the file to display")]
        path: String,

        #[clap(
            short = 'n',
            long,
            default_value_t = DEFAULT_TAIL_BYTES,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Number of trailing bytes to print"
        )]
        bytes: u64,
    },
}

/// Offset at which the last `bytes` bytes of a file of `len` bytes start.
/// A file shorter than `bytes` starts at 0.
fn tail_offset(len: i64, bytes: u64) -> i64 {
    let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
    len.saturating_sub(bytes).max(0)
}

/// Copies the last `bytes` bytes of `reader` to `out` unchanged and returns
/// the number of bytes copied. The reader seeks to the start of that window
/// first, so the bytes before it are never read.
async fn copy_tail<R: Reader, W: AsyncWrite + Unpin>(
    reader: &mut R,
    out: &mut W,
    bytes: u64,
    chunk_size: usize,
) -> CommonResult<u64> {
    let offset = tail_offset(reader.len(), bytes);
    if offset > 0 {
        reader.seek(offset).await?;
    }
    copy_to(reader, out, chunk_size).await
}

impl TailCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            TailCommand::Tail { path, bytes } => {
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
                                let tail = copy_tail(&mut reader, &mut stdout, *bytes, chunk_size);
                                if let Err(e) = tail.await {
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
    use super::{copy_tail, tail_offset, TailCommand, DEFAULT_TAIL_BYTES};
    use clap::Parser;
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::Path;

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: TailCommand,
    }

    fn parse_bytes(args: &[&str]) -> Result<u64, clap::Error> {
        let cli = Cli::try_parse_from(args)?;
        // `TailCommand` has a single variant, so the pattern is irrefutable.
        let TailCommand::Tail { bytes, .. } = cli.command;
        Ok(bytes)
    }

    /// Runs `copy_tail` over a local file holding `content`.
    async fn tail_bytes(content: &[u8], bytes: u64) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("data");
        std::fs::write(&file, content).unwrap();

        let path = Path::from_str(file.to_str().unwrap()).unwrap();
        let mut reader = LocalReader::new(&path, 64).unwrap();
        let mut out = Vec::new();
        let copied = copy_tail(&mut reader, &mut out, bytes, 100).await.unwrap();
        assert_eq!(copied, out.len() as u64);
        out
    }

    #[test]
    fn bytes_defaults_to_1024() {
        let bytes = parse_bytes(&["tail", "tail", "/a.log"]);
        assert_eq!(bytes.unwrap(), 1024);
        assert_eq!(DEFAULT_TAIL_BYTES, 1024);
    }

    #[test]
    fn parses_short_and_long_bytes_option() {
        let short = parse_bytes(&["tail", "tail", "-n", "5", "/a.log"]);
        assert_eq!(short.unwrap(), 5);
        let long = parse_bytes(&["tail", "tail", "--bytes", "65536", "/a.log"]);
        assert_eq!(long.unwrap(), 65536);
    }

    #[test]
    fn rejects_zero_and_negative_bytes() {
        let zero = parse_bytes(&["tail", "tail", "-n", "0", "/a.log"]);
        assert!(zero.is_err());
        let negative = parse_bytes(&["tail", "tail", "-n", "-1", "/a.log"]);
        assert!(negative.is_err());
    }

    #[test]
    fn tail_offset_starts_at_the_trailing_window() {
        assert_eq!(tail_offset(10, 3), 7);
        assert_eq!(tail_offset(10, 10), 0);
        assert_eq!(tail_offset(3, 10), 0);
        assert_eq!(tail_offset(0, 5), 0);
        assert_eq!(tail_offset(10, u64::MAX), 0);
    }

    #[tokio::test]
    async fn prints_the_last_1024_bytes_by_default() {
        let content: Vec<u8> = (0..=255u8).cycle().take(3000).collect();
        let out = tail_bytes(&content, DEFAULT_TAIL_BYTES).await;
        assert_eq!(out, &content[3000 - 1024..]);
    }

    #[tokio::test]
    async fn prints_exactly_the_requested_bytes() {
        assert_eq!(tail_bytes(b"hello world", 5).await, b"world");
    }

    #[tokio::test]
    async fn prints_the_whole_file_when_it_is_shorter() {
        assert_eq!(tail_bytes(b"short", 4096).await, b"short");
    }

    #[tokio::test]
    async fn prints_nothing_for_an_empty_file() {
        assert!(tail_bytes(b"", 1024).await.is_empty());
    }

    #[tokio::test]
    async fn keeps_a_binary_suffix_unchanged() {
        // The window starts inside a multi-byte character and holds invalid UTF-8.
        let mut content = "中文目录".as_bytes().to_vec();
        content.extend_from_slice(&[0x00, 0xff, 0xfe, 0x80]);
        let out = tail_bytes(&content, 8).await;
        assert_eq!(out, &content[content.len() - 8..]);
    }
}
