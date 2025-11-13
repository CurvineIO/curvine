use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use curvine_common::state::SetAttrOpts;
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum ChmodCommand {
    /// Display file contents
    Chmod {
        #[clap(help = "Permissions to set (e.g., 755)")]
        mode: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(help = "Recursively apply permissions to all files and directories")]
        recursive: bool,
    },
}

impl ChmodCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            ChmodCommand::Chmod {
                mode,
                path,
                recursive,
            } => {
                let path = CurvineURI::new(path)?;

                let mode = Self::parse_mode(mode)?;
                if *recursive {
                    let opts = SetAttrOpts {
                        mode: Some(mode),
                        recursive: true,
                        ..Default::default()
                    };
                    client.set_attr(&path, opts).await?;
                } else {
                    let opts = SetAttrOpts {
                        mode: Some(mode),
                        ..Default::default()
                    };
                    client.set_attr(&path, opts).await?;
                }
                println!("Changed permission of '{}' set to {:o}", path, mode);

                Ok(())
            }
        }
    }

    fn parse_mode(mode_str: &str) -> CommonResult<u32> {
        let mode_str = mode_str.trim();
        if mode_str.is_empty() {
            return Err("Mode string is empty".into());
        }

        let s = mode_str.strip_prefix("0o").unwrap_or(mode_str);
        let s = if s.len() == 4 && s.starts_with('0') {
            &s[1..]
        } else {
            s
        };

        if let Ok(m) = u32::from_str_radix(s, 8) {
            return Ok(m);
        }

        let mut mode: u32 = 0;
        for part in s.split(',') {
            let (who, perms) = part
                .split_once('=')
                .ok_or_else(|| format!("Invalid mode part: {}", part))?;

            if who != "u" && who != "g" && who != "o" {
                return Err(format!("Invalid 'who' in mode part: {}", who).into());
            }

            let mut bits: u32 = 0;
            for c in perms.chars() {
                bits |= match c {
                    'r' => 4,
                    'w' => 2,
                    'x' => 1,
                    _ => return Err(format!("Invalid permission character: {}", c).into()),
                };
            }

            mode |= match who {
                "u" => bits << 6,
                "g" => bits << 3,
                "o" => bits,
                _ => 0,
            };
        }

        Ok(mode)
    }
}
