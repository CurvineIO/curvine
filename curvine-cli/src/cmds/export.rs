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

use crate::cmds::LoadStatusCommand;
use crate::util::*;
use clap::Parser;
use curvine_fs_api::Path;
use curvine_job_client::{JobMasterClient, TransferClient};
use curvine_model::{LoadJobCommand, TransferCommand, TransferKind};
use curvine_unified_fs::UnifiedFileSystem;
use orpc::{err_box, CommonResult};

#[derive(Parser, Debug)]
pub struct ExportCommand {
    /// Curvine source path to export to the mounted UFS
    path: String,

    /// Watch export job status after submission
    #[arg(long, short = 'w')]
    watch: bool,

    /// Do not overwrite an existing target file
    #[arg(long = "no-overwrite", default_value_t = true, action = clap::ArgAction::SetFalse)]
    overwrite: bool,
}

impl ExportCommand {
    pub async fn execute_legacy(&self, client: JobMasterClient) -> CommonResult<()> {
        if self.path.trim().is_empty() {
            eprintln!("Error: Path cannot be empty");
            std::process::exit(1);
        }

        println!("\nExporting Curvine file to mounted UFS");
        println!("Source path: {}", self.path);

        let command = LoadJobCommand::builder(&self.path)
            .overwrite(self.overwrite)
            .build();
        let rep = handle_rpc_result(client.submit_export_job(command)).await;

        println!("\nExport job submitted successfully");
        println!("Job ID: {}", rep.job_id);
        println!("Target path: {}", rep.target_path);
        println!(
            "\nTo check job status, run: curvine load-status {}",
            rep.job_id
        );

        if self.watch {
            let status_command =
                LoadStatusCommand::new(rep.job_id.clone(), false, "1s".to_string());

            status_command.execute(client).await?;
        }

        Ok(())
    }

    pub async fn execute_transfer(
        &self,
        fs: UnifiedFileSystem,
        transfer_client: TransferClient,
    ) -> CommonResult<()> {
        if self.path.trim().is_empty() {
            eprintln!("Error: Path cannot be empty");
            std::process::exit(1);
        }

        println!("\nExporting Curvine file to UFS");
        println!("Source path: {}", self.path);

        let source = Path::from_str(&self.path)?;
        if !source.is_cv() {
            return err_box!("export source must be a Curvine path: {}", self.path);
        }
        let target = match fs.toggle_path(&source, true).await? {
            Some(target) => target,
            None => return err_box!("{} is not mounted", self.path),
        };
        if target.is_cv() {
            return err_box!("export target must be a UFS path: {}", target.full_path());
        }

        let mut command = TransferCommand {
            kind: TransferKind::Export,
            source_path: source.clone_uri(),
            target_path: target.clone_uri(),
            client_request_id: TransferCommand::default_client_request_id(
                TransferKind::Export,
                source.clone_uri(),
                target.clone_uri(),
            ),
            submitter: "curvine-cli".to_string(),
            tenant: String::new(),
            options: Default::default(),
        };
        command.set_overwrite(self.overwrite);
        let rep = handle_rpc_result(transfer_client.submit(command)).await;
        println!("Job ID: {}", rep.job_id);
        println!("Target path: {}", target.full_path());
        println!("State: {}", rep.state);

        if self.watch {
            let status_command = LoadStatusCommand::new(rep.job_id, false, "1s".to_string());
            status_command
                .execute_transfer_only(transfer_client)
                .await?;
        }

        Ok(())
    }
}
