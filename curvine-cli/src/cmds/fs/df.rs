use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum DfCommand {
    /// Show disk usage statistics
    Df {
        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,
        #[clap(long, action = clap::ArgAction::Help, help = "Print help")]
        help: Option<bool>,
    },
}

impl DfCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            DfCommand::Df { human_readable, .. } => {
                // Get filesystem info from the filesystem
                match client.get_filesystem_info().await {
                    Ok(filesystem_info) => {
                        // Format similar to HDFS df output
                        println!(
                            "Filesystem                Size             Used  Available  Use%"
                        );

                        // Calculate total used space (fs_used + non_fs_used)
                        let used = filesystem_info.fs_used + filesystem_info.non_fs_used;

                        // Calculate usage percentage
                        let usage_percent = if filesystem_info.capacity > 0 {
                            (used as f64 / filesystem_info.capacity as f64) * 100.0
                        } else {
                            0.0
                        };

                        // Format sizes based on human_readable flag
                        let (capacity, used_str, available) = if *human_readable {
                            (
                                crate::cmds::fs::common::format_size(
                                    filesystem_info.capacity as u64,
                                ),
                                crate::cmds::fs::common::format_size(used as u64),
                                crate::cmds::fs::common::format_size(
                                    filesystem_info.available as u64,
                                ),
                            )
                        } else {
                            (
                                filesystem_info.capacity.to_string(),
                                used.to_string(),
                                filesystem_info.available.to_string(),
                            )
                        };

                        println!(
                            "curvine://{}  {:>15}  {:>15}  {:>15}  {:.1}%",
                            filesystem_info.active_master,
                            capacity,
                            used_str,
                            available,
                            usage_percent
                        );

                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("Error getting filesystem info: {}", e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
