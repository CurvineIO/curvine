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
use curvine_fuse::cli::{run_mount, FuseCli};
use orpc::{err_box, CommonResult};

// fuse mount.
// Debugging, after starting the cluster, execute the following naming, mount fuse
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --conf /server/conf/curvine-cluster.toml
fn main() -> CommonResult<()> {
    let cli = FuseCli::parse();
    if cli.runs_mount() {
        run_mount(cli.resolve_mount_args())
    } else {
        err_box!("unsupported curvine-fuse subcommand")
    }
}
