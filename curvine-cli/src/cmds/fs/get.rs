use bytes::BytesMut;
use clap::Subcommand;
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::{CurvineURI, Reader};
use orpc::CommonResult;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

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
    pub async fn execute(&self, client: CurvineFileSystem) -> CommonResult<()> {
        match self {
            GetCommand::Get { path, local_path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            eprintln!(
                                "get: Cannot download '{}': Is a directory",
                                path.full_path()
                            );
                            return Ok(());
                        }

                        // Determine the final local path
                        let mut final_local_path = if local_path.exists() && local_path.is_dir()
                            || local_path
                                .to_string_lossy()
                                .ends_with(std::path::MAIN_SEPARATOR)
                        {
                            // If local_path is a directory or ends with a path separator, append the filename from the remote path
                            let filename = path.name();
                            local_path.join(filename)
                        } else if !local_path
                            .to_string_lossy()
                            .contains(std::path::MAIN_SEPARATOR)
                        {
                            // If local_path is just a filename without path, use current directory
                            let filename = local_path.to_string_lossy().to_string();
                            std::env::current_dir()
                                .unwrap_or_else(|_| PathBuf::from("."))
                                .join(filename)
                        } else {
                            local_path.clone()
                        };

                        // Check if parent directory exists, refuse to execute if it doesn't
                        if let Some(parent) = final_local_path.parent() {
                            if !parent.exists() {
                                eprintln!("Error: Parent directory '{}' does not exist. Please create it first.", parent.display());
                                return Ok(());
                            }
                        }

                        // If the final_local_path itself is a directory (ends with separator), create it
                        if final_local_path
                            .to_string_lossy()
                            .ends_with(std::path::MAIN_SEPARATOR)
                        {
                            if let Err(e) = fs::create_dir_all(&final_local_path).await {
                                eprintln!("Error creating directory: {}", e);
                                return Err(e.into());
                            }
                            // Append the filename to the directory path
                            let filename = path.name();
                            final_local_path = final_local_path.join(filename);
                        }

                        // Open local file for writing
                        let mut file = match fs::File::create(&final_local_path).await {
                            Ok(file) => file,
                            Err(e) => {
                                eprintln!("Error creating local file: {}", e);
                                return Ok(());
                            }
                        };

                        println!("Downloading to {}", final_local_path.display());

                        // Open the remote file and read its content
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let total_size = reader.len();
                                let mut bytes_read: i64 = 0;
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut buffer = BytesMut::zeroed(chunk_size);

                                // Read in chunks and write to local file
                                while bytes_read < total_size {
                                    let remaining = total_size - bytes_read;
                                    let to_read = std::cmp::min(remaining as usize, chunk_size);
                                    let buf_slice = &mut buffer[..to_read];

                                    if let Err(e) = reader.read_full(buf_slice).await {
                                        eprintln!("Error reading from remote file: {}", e);
                                        return Err(e.into());
                                    }

                                    if let Err(e) = file.write_all(buf_slice).await {
                                        eprintln!("Error writing to local file: {}", e);
                                        return Err(e.into());
                                    }

                                    bytes_read += to_read as i64;
                                }

                                // Flush and complete
                                if let Err(e) = file.flush().await {
                                    eprintln!("Error flushing local file: {}", e);
                                    return Err(e.into());
                                }

                                if let Err(e) = reader.complete().await {
                                    eprintln!("Error completing read: {}", e);
                                    return Err(e.into());
                                }

                                println!("Successfully downloaded {} bytes", bytes_read);
                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("Error opening remote file: {}", e);
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
