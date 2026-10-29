use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_unified_fs::UnifiedFileSystem;

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

        #[clap(long, help = "Force live recursive scan instead of dir-space snapshot")]
        live: bool,

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
                live,
                ..
            } => {
                let path = CurvineURI::new(path)?;
                let mut warn_fallback = true;

                if *verbose {
                    println!(
                        "{:>15}  {:>15}  {:>15}  PATH",
                        "SIZE", "SINGLE_REPLICA", "ALL_REPLICAS"
                    );
                }

                match crate::cmds::fs::common::resolve_content_summary_for_path(
                    &client,
                    &path,
                    *live,
                    &mut warn_fallback,
                )
                .await
                {
                    Ok(summary) => {
                        let status = client.get_status(&path).await;
                        let replicas = status.map(|s| s.replicas).unwrap_or(1) as i64;
                        let logical = summary.logical_bytes as i64;
                        let disk_space = logical * replicas;

                        if *verbose {
                            let source =
                                crate::cmds::fs::common::format_content_summary_source(&summary);
                            let (generation, finished) = summary
                                .snapshot
                                .as_ref()
                                .map(|snap| {
                                    (
                                        snap.generation.to_string(),
                                        crate::cmds::fs::common::format_epoch_ms_local(
                                            snap.finished_ms,
                                            "%Y-%m-%d %H:%M:%S",
                                        ),
                                    )
                                })
                                .unwrap_or_else(|| ("-".to_string(), "-".to_string()));
                            eprintln!(
                                "du: source={source} generation={generation} finished={finished}"
                            );
                        }

                        if *human_readable {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                crate::cmds::fs::common::format_size(summary.logical_bytes),
                                crate::cmds::fs::common::format_size(summary.logical_bytes),
                                crate::cmds::fs::common::format_size(disk_space as u64),
                                path.full_path()
                            );
                        } else {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                logical,
                                logical,
                                disk_space,
                                path.full_path()
                            );
                        }
                        Ok(())
                    }
                    Err(e) => Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
