use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum DuCommand {
    /// Calculate directory space usage
    Du {
        #[clap(help = "Path of the directory to calculate")]
        path: String,

        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,

        #[clap(short, long, help = "Display the names of columns as a header line")]
        verbose: bool,

        #[clap(long, action = clap::ArgAction::Help, help = "Print help")]
        help: Option<bool>,
    },
}

impl DuCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            DuCommand::Du {
                path,
                human_readable,
                verbose,
                ..
            } => {
                let path = CurvineURI::new(path)?;

                // Print header similar to HDFS du -v output, but only if verbose is true
                if *verbose {
                    println!(
                        "{:>15}  {:>15}  {:>15}  PATH",
                        "SIZE", "SINGLE_REPLICA", "ALL_REPLICAS"
                    );
                }

                match crate::cmds::fs::common::calculate_content_summary(&client, &path).await {
                    Ok(summary) => {
                        // Get file status to determine replication factor
                        let status = client.get_status(&path).await;
                        // Default replication factor is 3 if we can't get the status
                        let replicas = status.map(|s| s.replicas).unwrap_or(1) as i64;
                        // Calculate disk space consumed with all replicas
                        let disk_space = summary.length * replicas;

                        // Format similar to HDFS du output with single replica data added
                        if *human_readable {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                crate::cmds::fs::common::format_size(summary.length as u64),
                                crate::cmds::fs::common::format_size(summary.length as u64),
                                crate::cmds::fs::common::format_size(disk_space as u64),
                                path.full_path()
                            );
                        } else {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                summary.length,
                                summary.length,
                                disk_space,
                                path.full_path()
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("du: Cannot access '{}': {}", path.full_path(), e);
                        Err(e)
                    }
                }
            }
        }
    }
}
