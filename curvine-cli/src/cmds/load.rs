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

use crate::util::*;
use clap::Parser;
use curvine_client::LoadClient;
use orpc::CommonResult;

#[derive(Parser, Debug)]
pub struct LoadCommand {
    path: String,

    #[arg(long, short = 't', default_value = "3d")]
    ttl: Option<String>,

    #[arg(long, short = 'r', default_value = "true")]
    recursive: Option<bool>,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,
}

impl LoadCommand {
    pub async fn execute(&self, client: LoadClient) -> CommonResult<()> {
        if self.path.trim().is_empty() {
            eprintln!("Error: Path cannot be empty");
            std::process::exit(1);
        }

        println!("\n Loading file to Curvine");
        println!("Source path: {}", self.path);

        let rep = handle_rpc_result(client.get_mount_point(self.path.as_str())).await;
        if rep.is_none() {
            println!(
                "Can not found mount point for path : {}, please mount first.",
                self.path
            );
            std::process::exit(1);
        }

        let rep =
            handle_rpc_result(client.submit_load(&self.path, self.ttl.clone(), self.recursive))
                .await;
        println!("{}", rep);
        Ok(())
    }
}
