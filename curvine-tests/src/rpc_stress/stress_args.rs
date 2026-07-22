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
use curvine_core::server::ServerConf;

// Stress test-related configuration parameters.
#[derive(Debug, Parser, Clone)]
pub struct StressArgs {
    #[arg(long, default_value = "server")]
    pub action: String,

    #[arg(long, default_value = "localhost")]
    pub hostname: String,

    #[arg(long, default_value = "1133")]
    pub port: u16,

    #[arg(long, default_value = "")]
    pub dirs: String,

    #[arg(long, default_value = "10")]
    pub file_num: usize,

    // How many threads are used to simulate the client service
    #[arg(long, default_value = "2")]
    pub client_threads: usize,

    // The data size is read and written every time.Default 64kb
    #[arg(long, default_value = "131072")]
    pub block_size: usize,

    // File size.Default is 10mb.
    #[arg(long, default_value = "10485760")]
    pub file_size: usize,

    // Whether to delete the file after reading is completed.
    #[arg(long, default_value = "false")]
    pub delete_file: bool,
}

impl Default for StressArgs {
    fn default() -> Self {
        Self::new()
    }
}

impl StressArgs {
    pub fn get_dir_list(&self) -> Vec<String> {
        let list: Vec<String> = self
            .dirs
            .split(',')
            .map(|x| String::from(x.trim()))
            .collect();

        list
    }

    pub fn new() -> Self {
        StressArgs::parse()
    }

    pub fn server_conf(&self) -> ServerConf {
        ServerConf::with_hostname(&self.hostname, self.port)
    }
}
