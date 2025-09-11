use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use orpc::CommonResult;

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
                    eprintln!("mv: Source and Destination must both be curvine uris");
                    return Err("Invalid URI format".into());
                }
                match client.get_status(&source).await {
                    Ok(_) => {
                        client.rename(&source, &destination).await?;
                        println!("Renamed {} to {} successfully", source, destination);
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("mv: Error checking source status: {}", e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
