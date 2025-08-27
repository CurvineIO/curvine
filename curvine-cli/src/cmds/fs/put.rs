use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem, Writer};
use orpc::CommonResult;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

#[derive(Subcommand, Debug)]
pub enum PutCommand {
    /// Upload a file to the filesystem
    Put {
        #[clap(help = "Local file path")]
        local_path: PathBuf,

        #[clap(help = "Remote destination path")]
        remote_path: String,
    },
}

impl PutCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            PutCommand::Put {
                local_path,
                remote_path,
            } => {
                // Check if local file exists
                if !local_path.exists() {
                    eprintln!(
                        "Error: Local file '{}' does not exist",
                        local_path.display()
                    );
                    return Ok(());
                }

                // Check if local path is a file (not directory)
                if !local_path.is_file() {
                    eprintln!("Error: '{}' is not a file", local_path.display());
                    return Ok(());
                }

                // Get file size for progress tracking
                let file_size = match local_path.metadata() {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        eprintln!(
                            "Error getting file metadata '{}': {}",
                            local_path.display(),
                            e
                        );
                        return Ok(());
                    }
                };

                println!(
                    "Uploading {} ({} bytes) to {}",
                    local_path.display(),
                    file_size,
                    remote_path
                );

                // Open local file for streaming read
                let file = match fs::File::open(local_path).await {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("Error opening local file '{}': {}", local_path.display(), e);
                        return Ok(());
                    }
                };
                let mut reader = BufReader::new(file);

                // Determine the final remote path
                let final_remote_path = if remote_path.ends_with('/') {
                    // If remote path ends with '/', it's definitely a directory
                    let local_filename = local_path.file_name().unwrap().to_string_lossy();
                    format!("{}{}", remote_path, local_filename)
                } else {
                    // Check if remote path exists and is a directory
                    match CurvineURI::new(remote_path) {
                        Ok(remote_check_uri) => {
                            match client.get_status(&remote_check_uri).await {
                                Ok(status) if status.is_dir => {
                                    // Remote path exists and is a directory
                                    let local_filename =
                                        local_path.file_name().unwrap().to_string_lossy();
                                    format!("{}/{}", remote_path, local_filename)
                                }
                                _ => {
                                    // Remote path doesn't exist or is a file, use as-is
                                    remote_path.to_string()
                                }
                            }
                        }
                        Err(_) => {
                            // If URI parsing fails, treat as file path
                            remote_path.to_string()
                        }
                    }
                };

                // Create remote path URI
                let remote_uri = match CurvineURI::new(&final_remote_path) {
                    Ok(uri) => uri,
                    Err(e) => {
                        eprintln!("Error parsing remote path '{}': {}", final_remote_path, e);
                        return Ok(());
                    }
                };

                // Create writer for streaming upload
                match client.create(&remote_uri, true).await {
                    Ok(mut writer) => {
                        let chunk_size = client.conf().client.write_chunk_size;
                        let mut buffer = vec![0; chunk_size];
                        let mut total_bytes = 0u64;
                        let mut last_progress = 0u64;

                        loop {
                            match reader.read(&mut buffer).await {
                                Ok(0) => break, // EOF
                                Ok(bytes_read) => {
                                    // Write chunk to curvine
                                    match writer.write(&buffer[..bytes_read]).await {
                                        Ok(_) => {
                                            total_bytes += bytes_read as u64;

                                            // Show progress every 10MB or at completion
                                            if total_bytes - last_progress >= 10 * 1024 * 1024
                                                || total_bytes == file_size
                                            {
                                                let progress =
                                                    (total_bytes as f64 / file_size as f64 * 100.0)
                                                        as u32;
                                                println!(
                                                    "Progress: {}% ({}/{} bytes)",
                                                    progress, total_bytes, file_size
                                                );
                                                last_progress = total_bytes;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Error writing chunk: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error reading from local file: {}", e);
                                    return Ok(());
                                }
                            }
                        }

                        // Complete the upload
                        match writer.complete().await {
                            Ok(_) => {
                                println!(
                                    "Successfully uploaded {} bytes to {}",
                                    total_bytes, final_remote_path
                                );
                            }
                            Err(e) => {
                                eprintln!("Error completing upload: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error creating remote file '{}': {}", final_remote_path, e);
                    }
                }

                Ok(())
            }
        }
    }
}
