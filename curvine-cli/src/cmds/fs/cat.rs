use super::common::{copy_to, copy_to_limited};
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum CatCommand {
    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,

        #[clap(long, default_value_t = 0, help = "Byte offset to start reading from")]
        offset: u64,

        #[clap(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Maximum bytes to read (default: to the end of the file)"
        )]
        length: Option<u64>,
    },
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
                                let file_len = reader.len();

                                // A range that starts at or past EOF is empty, not
                                // an error, so it prints nothing and succeeds. Seeking
                                // there would fail in the reader instead.
                                if *offset as i64 >= file_len {
                                    if let Err(e) = reader.complete().await {
                                        return Err(format!(
                                            "Cannot complete read of '{}': {}",
                                            path.full_path(),
                                            e
                                        )
                                        .into());
                                    }
                                    return Ok(());
                                }

                                if *offset > 0 {
                                    if let Err(e) = reader.seek(*offset as i64).await {
                                        return Err(format!(
                                            "Cannot seek '{}': {}",
                                            path.full_path(),
                                            e
                                        )
                                        .into());
                                    }
                                }

                                let limit = match length {
                                    Some(length) => *length,
                                    None => reader.remaining().max(0) as u64,
                                };
                                let result = match length {
                                    Some(_) => {
                                        copy_to_limited(&mut reader, &mut stdout, chunk_size, limit)
                                            .await
                                    }
                                    None => copy_to(&mut reader, &mut stdout, chunk_size).await,
                                };
                                if let Err(e) = result {
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
    use super::CatCommand;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        cmd: CatCommand,
    }

    fn range(args: &[&str]) -> (u64, Option<u64>) {
        let cli = Cli::try_parse_from(args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        match cli.cmd {
            CatCommand::Cat { offset, length, .. } => (offset, length),
        }
    }

    #[test]
    fn the_default_range_is_the_whole_file() {
        assert_eq!(range(&["cv", "cat", "/data/a"]), (0, None));
    }

    #[test]
    fn a_range_is_parsed() {
        assert_eq!(
            range(&["cv", "cat", "--offset", "100", "--length", "20", "/data/a"]),
            (100, Some(20))
        );
    }

    #[test]
    fn a_negative_offset_is_a_usage_error() {
        assert!(Cli::try_parse_from(["cv", "cat", "--offset", "-1", "/data/a"]).is_err());
    }

    #[test]
    fn a_zero_length_is_a_usage_error() {
        assert!(Cli::try_parse_from(["cv", "cat", "--length", "0", "/data/a"]).is_err());
    }
}
