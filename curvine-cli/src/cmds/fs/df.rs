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

                        // Report the allocatable (writable) view: only Live
                        // workers are eligible for new writes, so Size/Available
                        // mirror what FUSE statfs reports. Used is derived from
                        // the allocatable view so Size = Used + Available stays
                        // consistent even when Blacklist/Decommission workers
                        // still hold data. On a legacy master that omits the
                        // allocatable fields, `FilesystemInfo` falls back to the
                        // aggregate totals, so this never regresses to zero.
                        let capacity = filesystem_info.allocatable_capacity;
                        let available = filesystem_info.allocatable_available;
                        let used = (capacity - available).max(0);

                        // Calculate usage percentage
                        let usage_percent = if capacity > 0 {
                            (used as f64 / capacity as f64) * 100.0
                        } else {
                            0.0
                        };

                        // Format sizes based on human_readable flag
                        let (capacity, used_str, available) = if *human_readable {
                            (
                                crate::cmds::fs::common::format_size(capacity as u64),
                                crate::cmds::fs::common::format_size(used as u64),
                                crate::cmds::fs::common::format_size(available as u64),
                            )
                        } else {
                            (
                                capacity.to_string(),
                                used.to_string(),
                                available.to_string(),
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
