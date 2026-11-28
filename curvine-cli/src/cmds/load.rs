// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cmds::TransferStatusCommand;
use crate::util::*;
use clap::Parser;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::Path;
use curvine_model::{TransferCommand, TransferKind, TransferState};
use curvine_transfer_client::TransferClient;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Parser, Debug)]
pub struct LoadCommand {
    /// UFS source path (root for --file-list) or a UFS-backed Curvine path
    source_path: Option<String>,

    /// Optional Curvine target path for explicit UFS-to-Curvine load
    target_path: Option<String>,

    /// Load only the files listed in this file (one path per line, `-` for stdin).
    /// Paths are relative to the source root.
    #[arg(long = "file-list")]
    file_list: Option<String>,

    /// Watch transfer status after submission
    #[arg(long, short = 'w')]
    watch: bool,

    /// Do not overwrite an existing target file
    #[arg(long = "no-overwrite", default_value_t = true, action = clap::ArgAction::SetFalse)]
    overwrite: bool,

    /// Start a new incremental load when the matching transfer has already finished
    #[arg(long)]
    force: bool,
}

impl LoadCommand {
    pub async fn execute_transfer(
        &self,
        fs: UnifiedFileSystem,
        transfer_client: TransferClient,
    ) -> CommonResult<()> {
        let source_root = match self.source_path.as_deref() {
            Some(path) if !path.trim().is_empty() => path,
            _ => {
                eprintln!("Error: source path cannot be empty");
                std::process::exit(1);
            }
        };
        let files = match self.file_list.as_deref() {
            Some(path) => read_file_list(path)?,
            None => Vec::new(),
        };
        if self.file_list.is_some() && files.is_empty() {
            return err_box!("file list is empty");
        }
        if self
            .target_path
            .as_ref()
            .is_some_and(|target_path| target_path.trim().is_empty())
        {
            eprintln!("Error: Target path cannot be empty");
            std::process::exit(1);
        }

        println!("\nLoading file to Curvine");
        println!("Source path: {source_root}");
        if let Some(target_path) = &self.target_path {
            println!("Target path: {}", target_path);
        }
        if !files.is_empty() {
            println!("Files: {}", files.len());
        }

        let input = Path::from_str(source_root)?;
        let (source, target) = if let Some(target_path) = &self.target_path {
            (input, Path::from_str(target_path)?)
        } else {
            let peer = match fs.toggle_path(&input, true).await? {
                Some(peer) => peer,
                None => return err_box!("{} is not mounted", source_root),
            };
            if input.is_cv() {
                (peer, input)
            } else {
                (input, peer)
            }
        };
        if source.is_cv() || !target.is_cv() {
            return err_box!(
                "load requires a UFS source and Curvine target: {} -> {}",
                source.full_path(),
                target.full_path()
            );
        }
        let mut command = TransferCommand {
            kind: TransferKind::Load,
            source_path: source.clone_uri(),
            target_path: target.clone_uri(),
            client_request_id: TransferCommand::default_client_request_id_with_overwrite(
                TransferKind::Load,
                source.clone_uri(),
                target.clone_uri(),
                self.overwrite,
            ),
            submitter: "curvine-cli".to_string(),
            tenant: String::new(),
            options: Default::default(),
            files: files.clone(),
        };
        if let Some(digest) = command.files_digest() {
            command.client_request_id = format!("{}_files_{digest}", command.client_request_id);
        }
        command.set_overwrite(self.overwrite);
        let mut rep = handle_rpc_result(transfer_client.submit(command)).await;
        if self.force && TransferState::from(rep.state).is_terminal() {
            rep = handle_rpc_result(transfer_client.retry(&rep.job_id)).await;
        }
        println!("Job ID: {}", rep.job_id);
        println!("Target path: {}", target.full_path());
        println!("State: {:?}", TransferState::from(rep.state));

        if self.watch {
            let status_command = TransferStatusCommand::new(rep.job_id, false, "1s".to_string());

            status_command
                .execute_transfer_only(transfer_client)
                .await?;
        }

        Ok(())
    }
}

fn read_file_list(path: &str) -> CommonResult<Vec<String>> {
    use std::io::Read;

    let text = if path == "-" {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text)?;
        text
    } else {
        std::fs::read_to_string(path)?
    };
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_cv_target_path() {
        let cmd =
            LoadCommand::try_parse_from(["load", "s3://flink/user", "/flink/user", "--watch"])
                .expect("load command should accept an optional CV target path");

        assert_eq!(cmd.source_path.as_deref(), Some("s3://flink/user"));
        assert_eq!(cmd.target_path.as_deref(), Some("/flink/user"));
        assert!(cmd.watch);
    }

    #[test]
    fn parses_force() {
        let cmd = LoadCommand::try_parse_from(["load", "s3://flink/user", "--force"])
            .expect("load command should accept --force");

        assert!(cmd.force);
    }

    #[test]
    fn parses_file_list_root() {
        let cmd = LoadCommand::try_parse_from([
            "load",
            "s3://bucket/dir",
            "/data/dir",
            "--file-list",
            "files.txt",
        ])
        .expect("load command should accept --file-list");
        assert_eq!(cmd.source_path.as_deref(), Some("s3://bucket/dir"));
        assert_eq!(cmd.target_path.as_deref(), Some("/data/dir"));
        assert_eq!(cmd.file_list.as_deref(), Some("files.txt"));
    }

    #[test]
    fn file_list_skips_blank_and_comment_lines() {
        let dir = std::env::temp_dir().join(format!("curvine-load-list-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("files.txt");
        std::fs::write(&path, "a\n\n# comment\n b \n").unwrap();
        let files = read_file_list(path.to_str().unwrap()).unwrap();
        assert_eq!(files, vec!["a".to_string(), "b".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
