use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_model::SetAttrOpts;
use curvine_unified_fs::UnifiedFileSystem;

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
        // `chown ""` would otherwise reach the master as `owner: Some("")`.
        if owner_group.trim().is_empty() {
            return Err("owner:group must not be empty".into());
        }
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

#[cfg(test)]
mod tests {
    use super::ChownCommand;

    fn parse(input: &str) -> (Option<&str>, Option<&str>) {
        ChownCommand::parse_owner_group(input).unwrap_or_else(|e| panic!("{input}: {e}"))
    }

    fn parse_err(input: &str) -> String {
        match ChownCommand::parse_owner_group(input) {
            Ok(parsed) => panic!("{input} should be rejected, parsed as {parsed:?}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn an_owner_alone_leaves_the_group_unchanged() {
        assert_eq!(parse("alice"), (Some("alice"), None));
    }

    #[test]
    fn owner_and_group_are_both_parsed() {
        assert_eq!(parse("alice:staff"), (Some("alice"), Some("staff")));
    }

    /// `:group` and `owner:` are the documented ways to change one side only.
    #[test]
    fn an_empty_side_means_unchanged() {
        assert_eq!(parse(":staff"), (None, Some("staff")));
        assert_eq!(parse("alice:"), (Some("alice"), None));
    }

    #[test]
    fn an_empty_argument_is_rejected() {
        // Otherwise an empty owner reaches the master as `Some("")`.
        assert!(parse_err("").contains("must not be empty"));
        assert!(parse_err("   ").contains("must not be empty"));
        assert!(parse_err(":").contains("Both owner and group cannot be empty"));
    }

    #[test]
    fn more_than_one_colon_is_rejected() {
        assert!(parse_err("a:b:c").contains("Invalid owner:group format"));
    }
}
