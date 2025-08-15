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
use curvine_client::file::FsClient;
use curvine_common::fs::Path;
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct UnMountCommand {
    curvine_path: String,
}

impl UnMountCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        let path = Path::from_str(&self.curvine_path)?;
        let rep = handle_rpc_result(client.umount(&path)).await;
        println!("{}", rep);
        Ok(())
    }
}
