use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum MvCommand {
    /// Move a file or directory
    Mv {
        #[clap(help = "Source path to move")]
        source: String,

        #[clap(help = "Destination path")]
        destination: String,
    },
}

impl MvCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            MvCommand::Mv {
                source,
                destination,
            } => {
                let source = CurvineURI::new(source)?;
                let destination = CurvineURI::new(destination)?;

                if !source.is_cv() || !destination.is_cv() {
                    return Err("Source and destination must both be curvine URIs".into());
                }
                match client.get_status(&source).await {
                    Ok(_) => {
                        client.rename(&source, &destination).await?;
                        println!("Renamed {} to {} successfully", source, destination);
                        Ok(())
                    }
                    Err(e) => Err(format!("Cannot access source '{}': {}", source, e).into()),
                }
            }
        }
    }
}
