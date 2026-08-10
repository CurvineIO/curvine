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

use clap::Parser;
use curvine_alloc as _;
use curvine_core_error::{err_box, CommonResult};
use curvine_fuse::cli::{
    run_list_config_flags, run_mount, run_validate_config, FuseCli, FuseSubcommand,
};

// For local debugging, after starting the cluster, run the following to mount fuse:
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --conf /server/conf/curvine-cluster.toml
fn main() -> CommonResult<()> {
    let cli = FuseCli::parse();
    if cli.version_json {
        let json = match curvine_sys::version::component_version_json("fuse") {
            Ok(json) => json,
            Err(e) => return err_box!("Failed to serialize component version: {}", e),
        };
        println!("{}", json);
        return Ok(());
    }

    match &cli.cmd {
        None | Some(FuseSubcommand::Mount(_)) => run_mount(cli.resolve_runtime_args()),
        Some(FuseSubcommand::ValidateConfig(_)) => run_validate_config(cli.resolve_runtime_args()),
        Some(FuseSubcommand::ListConfigFlags(args)) => run_list_config_flags(args.clone()),
    }
}
