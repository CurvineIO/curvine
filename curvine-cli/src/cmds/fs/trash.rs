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
    Empty,
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
                let path = CurvineURI::new(path)?;
                let dst = match dst {
                    Some(value) => Some(CurvineURI::new(value)?),
                    None => None,
                };

                client
                    .restore_trash(&path, dst.as_ref(), *overwrite)
                    .await?;
                match dst {
                    Some(dst) => println!("Restored {} to {}", path.full_path(), dst.full_path()),
                    None => println!("Restored {} to its original location", path.full_path()),
                }
                Ok(())
            }

            TrashCommand::Empty => {
                let res = client.empty_trash().await?;
                println!(
                    "Emptied recycle bin: {} files, {}",
                    res.inodes,
                    format_size(res.bytes)
                );
                Ok(())
            }
        }
    }
}
