use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use curvine_common::state::SetAttrOpts;
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum ChownCommand {
    Chown {
        #[clap(help = "Owner/Group to set in format owner:group")]
        owner_group: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(help = "Recursively apply permissions to all files and directories")]
        recursive: bool,
    },
}

impl ChownCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            ChownCommand::Chown {
                owner_group,
                path,
                recursive,
            } => {
                let path = CurvineURI::new(path)?;

                let (owner, group) = Self::parse_owner_group(owner_group)?;

                if *recursive {
                    let opts = SetAttrOpts {
                        owner: owner.map(|s| s.to_string()),
                        group: group.map(|s| s.to_string()),
                        recursive: true,
                        ..Default::default()
                    };
                    client.set_attr(&path, opts).await?;
                } else {
                    let opts = SetAttrOpts {
                        owner: owner.map(|s| s.to_string()),
                        group: group.map(|s| s.to_string()),
                        ..Default::default()
                    };
                    client.set_attr(&path, opts).await?;
                }
                println!(
                    "Changed owner/group of '{}' set to '{:?}:{:?}'",
                    path, owner, group
                );

                Ok(())
            }
        }
    }

    fn parse_owner_group(owner_group: &str) -> CommonResult<(Option<&str>, Option<&str>)> {
        let parts: Vec<&str> = owner_group.split(':').collect();
        match parts.len() {
            1 => Ok((Some(parts[0]), None)),
            2 => {
                if parts[0].is_empty() && parts[1].is_empty() {
                    return Err("Both owner and group cannot be empty".into());
                }
                Ok((
                    if parts[0].is_empty() {
                        None
                    } else {
                        Some(parts[0])
                    },
                    if parts[1].is_empty() {
                        None
                    } else {
                        Some(parts[1])
                    },
                ))
            }

            _ => Err("Invalid owner:group format".into()),
        }
    }
}
