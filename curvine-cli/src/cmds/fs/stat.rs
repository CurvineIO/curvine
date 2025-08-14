use clap::Subcommand;
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::CurvineURI;
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum StatCommand {
    /// Stat file or directory
    Stat {
        #[clap(help = "Path of the file or directory to stat")]
        path: String,
    },
}

impl StatCommand {
    pub async fn execute(&self, client: CurvineFileSystem) -> CommonResult<()> {
        match self {
            StatCommand::Stat { path } => {
                let path = CurvineURI::new(path)?;

                match client.get_status(&path).await {
                    Ok(status) => {
                        // Format similar to HDFS stat output
                        println!("File: {}", path.full_path());
                        println!("Size: {}", status.len);

                        // Format modification time
                        let mtime = chrono::DateTime::from_timestamp(status.mtime / 1000, 0)
                            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                        let formatted_mtime = mtime.format("%Y-%m-%d %H:%M:%S").to_string();

                        println!("Type: {}", if status.is_dir { "directory" } else { "file" });
                        println!("Modification time: {}", formatted_mtime);
                        println!(
                            "Permission: {}rwxr-xr-x",
                            if status.is_dir { "d" } else { "-" }
                        );
                        println!("Owner: curvine");
                        println!("Group: curvine");

                        if !status.is_dir {
                            println!("Replication: {}", status.replicas);
                        }

                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("stat: Cannot stat '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
