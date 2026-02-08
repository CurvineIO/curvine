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
use curvine_common::version;

#[derive(Debug, Parser, Clone, Default)]
#[command(arg_required_else_help = true, version = version::VERSION)]
pub struct BenchArgs {
    #[arg(short, long, default_value = "")]
    pub action: String,

    #[arg(short, long, default_value = "")]
    pub conf: String,

    // cv:// Start, access using client mode.
    // file://start, local mount fuse mode access.
    #[arg(short, long, default_value = "/fuse-bench")]
    pub dir: String,

    #[arg(long, default_value = "10")]
    pub file_num: usize,

    // How many threads are used to simulate the client service
    #[arg(long, default_value = "10")]
    pub client_threads: usize,

    // The data size is read and written every time.Default 1mb
    #[arg(long, default_value = "128KB")]
    pub buf_size: String,

    // File size.Default is 10mb.
    #[arg(long, default_value = "100MB")]
    pub file_size: String,

    // Whether to delete the file after reading is completed.
    #[arg(long, default_value = "false")]
    pub delete_file: bool,

    // Whether to calculate checksum
    #[arg(long, action = clap::ArgAction::Set, default_value = "true")]
    pub checksum: bool,
}

impl BenchArgs {
    pub fn new() -> Self {
        BenchArgs::parse()
    }
}
