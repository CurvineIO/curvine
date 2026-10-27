use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::CurvineURI;
use curvine_unified_fs::UnifiedFileSystem;

use crate::cmds::fs::common::{format_epoch_ms_local, format_size};

#[derive(Subcommand, Debug)]
pub enum TrashCommand {
    /// List entries in the recycle bin
    Ls,

    /// Restore a trashed entry to its original location
    Restore {
        #[clap(help = "Trash entry path, e.g. /.Trash/1700000000000-42")]
        path: String,

        #[clap(
            long,
            help = "Restore to this path instead of the recorded original location"
        )]
        dst: Option<String>,

        #[clap(long, help = "Overwrite the destination if it already exists")]
        overwrite: bool,
    },

    /// Permanently delete every entry in the recycle bin
    Empty {
        #[clap(
            long,
            help = "Wait for the master to reclaim every entry and print the \
                    reclaimed size"
        )]
        wait: bool,
    },
}

impl TrashCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            TrashCommand::Ls => {
                let entries = client.list_trash().await?;
                if entries.is_empty() {
                    println!("Recycle bin is empty");
                    return Ok(());
                }

                println!(
                    "{:<12} {:<19} {:<24} TRASH PATH",
                    "SIZE", "DELETED", "ORIGIN"
                );
                for entry in entries {
                    println!(
                        "{:<12} {:<19} {:<24} {}",
                        format_size(entry.status.len as u64),
                        format_epoch_ms_local(entry.delete_ms, "%Y-%m-%d %H:%M:%S"),
                        entry.origin,
                        entry.path
                    );
                }
                Ok(())
            }

            TrashCommand::Restore {
                path,
                dst,
                overwrite,
            } => {
                let (path, dst) = parse_restore_paths(path, dst.as_deref())?;

                client
                    .restore_trash(&path, dst.as_ref(), *overwrite)
                    .await?;
                match dst {
                    Some(dst) => println!("Restored {} to {}", path.full_path(), dst.full_path()),
                    None => println!("Restored {} to its original location", path.full_path()),
                }
                Ok(())
            }

            TrashCommand::Empty { wait } => {
                if *wait {
                    let res = client.empty_trash().await?;
                    println!(
                        "Emptied recycle bin: {} files, {}",
                        res.inodes,
                        format_size(res.bytes)
                    );
                } else {
                    // Staging is a rename per entry, so the command returns
                    // before the master reclaims anything.
                    let (accepted, _) = client.empty_trash_async().await?;
                    println!(
                        "Staged {} entries for background purge; pass --wait to reclaim them now",
                        accepted
                    );
                }
                Ok(())
            }
        }
    }
}

/// Parses the restore arguments, rejecting non-Curvine URIs. The RPC serializes
/// paths with `Path::encode()`, which drops the scheme and authority, so an
/// `s3://bucket/data` destination would otherwise be applied to Curvine `/data`.
fn parse_restore_paths(
    path: &str,
    dst: Option<&str>,
) -> CommonResult<(CurvineURI, Option<CurvineURI>)> {
    let path = CurvineURI::new(path)?;
    let dst = match dst {
        Some(value) => Some(CurvineURI::new(value)?),
        None => None,
    };

    if !path.is_cv() || dst.as_ref().is_some_and(|dst| !dst.is_cv()) {
        return Err("trash restore: trash path and destination must both be curvine uris".into());
    }

    Ok((path, dst))
}

#[cfg(test)]
mod tests {
    use super::parse_restore_paths;

    #[test]
    fn accepts_curvine_scheme_and_bare_paths() {
        let (path, dst) = parse_restore_paths("/.Trash/1700000000000-42", Some("/data")).unwrap();
        assert_eq!(path.full_path(), "/.Trash/1700000000000-42");
        assert_eq!(dst.unwrap().full_path(), "/data");

        let (path, dst) = parse_restore_paths("cv://bucket/.Trash/1700000000000-42", None).unwrap();
        assert!(path.is_cv());
        assert!(dst.is_none());
    }

    #[test]
    fn rejects_non_curvine_trash_path() {
        assert!(parse_restore_paths("s3://bucket/.Trash/1700000000000-42", None).is_err());
    }

    #[test]
    fn rejects_non_curvine_destination() {
        assert!(parse_restore_paths("/.Trash/1700000000000-42", Some("s3://bucket/data")).is_err());
    }
}

#[cfg(test)]
mod command_tests {
    use super::TrashCommand;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: TrashCommand,
    }

    #[test]
    fn empty_defaults_to_async_ack() {
        let cli = Cli::try_parse_from(["trash", "empty"]).unwrap();
        let TrashCommand::Empty { wait } = cli.command else {
            panic!("expected the empty command");
        };
        assert!(!wait, "empty must acknowledge by default");
    }

    #[test]
    fn parses_empty_wait() {
        let cli = Cli::try_parse_from(["trash", "empty", "--wait"]).unwrap();
        let TrashCommand::Empty { wait } = cli.command else {
            panic!("expected the empty command");
        };
        assert!(wait);
    }
}
