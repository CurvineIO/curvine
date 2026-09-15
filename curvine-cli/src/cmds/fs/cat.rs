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
                                let total_size = reader.len();
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut buffer = BytesMut::zeroed(chunk_size);
                                let mut bytes_read: i64 = 0;

                                // Read in chunks and print to stdout
                                while bytes_read < total_size {
                                    let remaining = total_size - bytes_read;
                                    let to_read = std::cmp::min(remaining as usize, chunk_size);
                                    let buf_slice = &mut buffer[..to_read];

                                    if let Err(e) = reader.read_full(buf_slice).await {
                                        eprintln!("Error reading from file: {}", e);
                                        return Err(e.into());
                                    }

                                    // Write raw bytes; the data is binary and
                                    // must not go through UTF-8 lossy string
                                    // conversion (it corrupts content and
                                    // inflates size via U+FFFD replacements).
                                    let stdout = std::io::stdout();
                                    let mut handle = stdout.lock();
                                    if let Err(e) = handle.write_all(buf_slice) {
                                        eprintln!("Error writing to stdout: {}", e);
                                        return Err(e.into());
                                    }
                                    let _ = handle.flush();

                                    bytes_read += to_read as i64;
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
