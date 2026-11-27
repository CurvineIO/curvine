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

/// Write a download next to its destination and return the staging file.
///
/// `NamedTempFile::new_in` creates the sibling with `O_EXCL` under a unique
/// name, so a stale or attacker-planted path cannot be truncated or followed
/// through a symlink, and concurrent downloads of the same file do not share
/// one staging path. Keeping the sibling in the destination directory also
/// means the publishing `rename` stays within one filesystem and cannot fail
/// with `EXDEV`.
async fn create_staging_file(final_path: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    let dir = final_path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let dir_clone = dir.clone();
    tokio::task::spawn_blocking(move || tempfile::NamedTempFile::new_in(dir_clone))
        .await
        .unwrap_or_else(|_join| {
            Err(std::io::Error::other(format!(
                "staging task failed in {}",
                dir.display()
            )))
        })
}

/// Permissions a restrictive destination already has, so overwriting it does
/// not widen access. Unix mode is the meaningful bit for local users; other
/// platforms have no narrower-but-preserving equivalent here.
#[cfg(unix)]
fn destination_mode(path: &Path) -> Option<std::fs::Permissions> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    std::fs::metadata(path)
        .ok()
        .filter(|meta| meta.is_file())
        .map(|meta| std::fs::Permissions::from_mode(meta.mode() & 0o777))
}

#[cfg(not(unix))]
fn destination_mode(_path: &Path) -> Option<std::fs::Permissions> {
    None
}

impl GetCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            GetCommand::Get { path, local_path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            return Err(format!(
                                "Cannot download '{}': Is a directory",
                                path.full_path()
                            )
                            .into());
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
                            let filename = path.name();
                            final_local_path = final_local_path.join(filename);
                        }

                        // Write to an exclusively created sibling and publish
                        // it with a rename once the transfer succeeded:
                        // truncating the destination first destroyed a
                        // pre-existing local file even when the download
                        // failed (#2315), and a predictable `create`d staging
                        // path could truncate or follow a planted symlink.
                        let existing_mode = destination_mode(&final_local_path);
                        let staging = match create_staging_file(&final_local_path).await {
                            Ok(staging) => staging,
                            Err(e) => {
                                return Err(format!(
                                    "Cannot create local file next to '{}': {}",
                                    final_local_path.display(),
                                    e
                                )
                                .into());
                            }
                        };
                        let part_path = staging.path().to_path_buf();
                        let mut file = fs::File::from_std(staging.into_file());

                        println!("Downloading to {}", final_local_path.display());

                        // The staging file is removed by `cleanup` whenever an
                        // error path returns before the rename consumes it;
                        // the destination itself is never touched.
                        let mut cleanup = Some(part_path.clone());

                        // Open the remote file and read its content
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let chunk_size = client.conf().client.read_chunk_size;
                                let copied = copy_to(&mut reader, &mut file, chunk_size).await;
                                let bytes_read = match copied {
                                    Ok(bytes_read) => bytes_read,
                                    Err(e) => {
                                        drop(file);
                                        if let Some(stale) = cleanup.take() {
                                            let _ = fs::remove_file(stale).await;
                                        }
                                        return Err(format!(
                                            "Cannot download '{}': {}",
                                            path.full_path(),
                                            e
                                        )
                                        .into());
                                    }
                                };

                                if let Err(e) = reader.complete().await {
                                    drop(file);
                                    if let Some(stale) = cleanup.take() {
                                        let _ = fs::remove_file(stale).await;
                                    }
                                    return Err(format!(
                                        "Cannot complete read of '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                // Preserve an existing restrictive destination:
                                // `rename` replaces it with the staging file,
                                // whose umask mode would widen access.
                                if let Some(mode) = existing_mode {
                                    if let Err(e) = fs::set_permissions(&part_path, mode).await {
                                        drop(file);
                                        if let Some(stale) = cleanup.take() {
                                            let _ = fs::remove_file(stale).await;
                                        }
                                        return Err(format!(
                                            "Cannot set permissions on '{}': {}",
                                            final_local_path.display(),
                                            e
                                        )
                                        .into());
                                    }
                                }

                                drop(file);
                                cleanup.take();
                                if let Err(e) = fs::rename(&part_path, &final_local_path).await {
                                    let _ = fs::remove_file(&part_path).await;
                                    return Err(format!(
                                        "Cannot write local file '{}': {}",
                                        final_local_path.display(),
                                        e
                                    )
                                    .into());
                                }

                                println!("Successfully downloaded {} bytes", bytes_read);
                                Ok(())
                            }
                            Err(e) => {
                                drop(file);
                                if let Some(stale) = cleanup.take() {
                                    let _ = fs::remove_file(stale).await;
                                }
                                Err(format!(
                                    "Cannot open remote file '{}': {}",
                                    path.full_path(),
                                    e
                                )
                                .into())
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
    use super::{create_staging_file, destination_mode};
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    #[tokio::test]
    async fn the_staging_file_is_a_unique_exclusively_created_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("file.bin");

        let first = create_staging_file(&final_path).await.unwrap();
        // Same directory, so the publishing rename cannot cross a filesystem.
        assert_eq!(first.path().parent(), Some(dir.path()));
        assert_ne!(first.path(), final_path.as_path());

        // A second download of the same file gets its own staging file: no
        // truncation of, or collision with, the first one.
        let second = create_staging_file(&final_path).await.unwrap();
        assert_ne!(first.path(), second.path());
        assert!(first.path().exists() && second.path().exists());

        // A planted symlink at the staging location is never followed: every
        // staging file is created exclusively and fails rather than opening an
        // existing path.
        let target = dir.path().join("victim");
        std::fs::write(&target, b"precious").unwrap();
        let link = dir.path().join("planted");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let planted = create_staging_file(&link).await.unwrap();
        assert_ne!(planted.path(), link.as_path());
        assert_eq!(std::fs::read(&target).unwrap(), b"precious");
    }

    #[tokio::test]
    async fn the_staging_file_stays_next_to_a_bare_filename() {
        let staging = create_staging_file(Path::new("just-a-name")).await.unwrap();
        // `new_in` canonicalizes the directory, so compare against the real cwd.
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(staging.path().parent(), Some(cwd.as_path()));
    }

    #[cfg(unix)]
    #[test]
    fn the_existing_destination_mode_is_captured_for_republishing() {
        let dir = tempfile::tempdir().unwrap();
        let private = dir.path().join("private.bin");
        std::fs::write(&private, b"data").unwrap();
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            destination_mode(&private).map(|mode| mode.mode() & 0o777),
            Some(0o600)
        );
        assert!(destination_mode(&dir.path().join("missing.bin")).is_none());
    }
}
