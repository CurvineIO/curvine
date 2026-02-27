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

use crate::ActionType::{FsRead, FsWrite, FuseRead, FuseWrite};
use crate::BenchArgs;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use orpc::common::{ByteUnit, Utils};
use orpc::runtime::{AsyncRuntime, Runtime};
use std::ops::Range;
use std::sync::Arc;

#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum ActionType {
    FsWrite,
    FsRead,
    FuseWrite,
    FuseRead,
}

impl ActionType {
    pub fn from_args(args: &BenchArgs) -> Self {
        match args.action.as_str() {
            "fs.write" => FsWrite,
            "fs.read" => FsRead,
            "fuse.write" => FuseWrite,
            "fuse.read" => FuseRead,
            v => panic!("Unsupported operation {}", v),
        }
    }

    pub fn is_fuse(&self) -> bool {
        match self {
            FsWrite => false,
            FsRead => false,
            FuseWrite => true,
            FuseRead => true,
        }
    }
}

pub struct BenchAction {
    pub action: ActionType,
    pub args: BenchArgs,
    pub buf_size: usize,
    pub file_size: usize,
    pub file_num: usize,
    pub loom_num: usize,
    pub total_len: u64,
    pub checksum: bool,
}

impl BenchAction {
    pub fn new(args: BenchArgs) -> Self {
        let action = ActionType::from_args(&args);

        let buf_size = ByteUnit::string_to_byte(&args.buf_size) as usize;
        let file_size = ByteUnit::string_to_byte(&args.file_size) as usize;
        let file_num = args.file_num;
        let loom_num = file_size / buf_size;
        let total_len = (file_num * file_size) as u64;
        let checksum = args.checksum;

        Self {
            action,
            args,
            buf_size,
            file_size,
            file_num,
            loom_num,
            total_len,
            checksum,
        }
    }

    pub fn create_rt(&self) -> Arc<Runtime> {
        Arc::new(AsyncRuntime::new(
            "curvine-bench",
            self.args.client_threads,
            self.args.client_threads,
        ))
    }

    pub fn loop_range(&self) -> Range<usize> {
        0..self.loom_num
    }

    pub fn rand_data(&self) -> Vec<u8> {
        Utils::rand_str(self.buf_size).as_bytes().to_vec()
    }

    pub fn is_fuse(&self) -> bool {
        self.action.is_fuse()
    }

    pub fn get_path(&self, index: usize) -> String {
        let path = format!("{}/{}", self.args.dir, index);
        path.replace("//", "/")
    }

    pub fn get_fs(&self, rt: Arc<Runtime>) -> CurvineFileSystem {
        let conf = ClusterConf::from(&self.args.conf, None).unwrap();
        CurvineFileSystem::with_rt(conf, rt).unwrap()
    }
}
