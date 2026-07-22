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

use crate::{ActionType, BenchAction, BenchArgs};
use bytes::BytesMut;
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_core::common::{FileUtils, SpeedCounter, Utils};
use curvine_core::io::LocalFile;
use curvine_core::runtime::{JoinHandle, RpcRuntime, Runtime};
use curvine_core::{err_box, CommonResult};
use std::sync::Arc;

struct TaskResult {
    len: u64,
    checksum: u64,
}
impl TaskResult {
    pub fn new() -> Self {
        TaskResult {
            len: 0,
            checksum: 0,
        }
    }

    pub fn update_ck(&mut self, v: &[u8]) {
        self.checksum += Utils::crc32(v) as u64;
    }

    pub fn set_len(&mut self, len: u64) {
        self.len = len;
    }

    pub fn merge(&mut self, src: TaskResult) {
        self.checksum += src.checksum;
        self.len += src.len;
    }
}

pub struct CurvineBench {
    rt: Arc<Runtime>,
    action: Arc<BenchAction>,
}

impl CurvineBench {
    pub fn new(args: BenchArgs) -> Self {
        let action = BenchAction::new(args);
        let rt = action.create_rt();
        Self {
            rt,
            action: Arc::new(action),
        }
    }

    // Initialize the environment.
    pub fn init(&self) -> CommonResult<Option<CurvineFileSystem>> {
        let dir = &self.action.args.dir;
        if self.action.is_fuse() {
            FileUtils::create_dir(dir, true)?;
            Ok(None)
        } else {
            let fs = self.action.get_fs(self.rt.clone());
            let path = Path::from_str(dir)?;
            let _ = self.rt.block_on(fs.mkdir(&path, true))?;
            Ok(Some(fs))
        }
    }

    pub fn run(&self) -> CommonResult<()> {
        let fs = self.init()?;

        let speed = SpeedCounter::new();
        let res = if self.action.is_fuse() {
            self.rt
                .block_on(Self::fuse_run(self.rt.clone(), self.action.clone()))?
        } else {
            self.rt.block_on(Self::fs_run(
                fs.unwrap(),
                self.rt.clone(),
                self.action.clone(),
            ))?
        };

        speed.print(&self.action.args.action, res.len);
        assert_eq!(self.action.total_len, res.len);
        println!(
            "Action {}, len {}, checksum {}",
            self.action.args.action, res.len, res.checksum
        );
        println!("args: {:?}", self.action.args);
        Ok(())
    }

    async fn fuse_run(rt: Arc<Runtime>, action: Arc<BenchAction>) -> CommonResult<TaskResult> {
        let mut handle: Vec<JoinHandle<CommonResult<TaskResult>>> = vec![];
        for i in 0..action.file_num {
            let path = action.get_path(i);
            let h = match action.action {
                ActionType::FuseWrite => rt.spawn(Self::fuse_write(action.clone(), path)),

                ActionType::FuseRead => rt.spawn(Self::fuse_read(action.clone(), path)),

                _ => return err_box!("Unsupported operation"),
            };

            handle.push(h);
        }

        let mut res = TaskResult::new();
        for h in handle {
            let v = h.await??;
            res.merge(v);
        }

        Ok(res)
    }

    async fn fs_run(
        fs: CurvineFileSystem,
        rt: Arc<Runtime>,
        action: Arc<BenchAction>,
    ) -> CommonResult<TaskResult> {
        let mut handle: Vec<JoinHandle<CommonResult<TaskResult>>> = vec![];

        for i in 0..action.file_num {
            let path = action.get_path(i);
            let h = match action.action {
                ActionType::FsWrite => rt.spawn(Self::fs_write(fs.clone(), action.clone(), path)),

                ActionType::FsRead => rt.spawn(Self::fs_read(fs.clone(), action.clone(), path)),

                _ => return err_box!("Unsupported operation"),
            };

            handle.push(h);
        }

        let mut res = TaskResult::new();
        for h in handle {
            let v = h.await??;
            res.merge(v);
        }

        Ok(res)
    }

    async fn fuse_write(action: Arc<BenchAction>, path: String) -> CommonResult<TaskResult> {
        let mut file = LocalFile::with_write(path, true)?;
        let data = action.rand_data();
        let mut res = TaskResult::new();

        for _ in action.loop_range() {
            file.write_all(&data)?;
            if action.checksum {
                res.update_ck(&data);
            }
        }

        res.set_len(file.pos() as u64);
        Ok(res)
    }

    async fn fuse_read(action: Arc<BenchAction>, path: String) -> CommonResult<TaskResult> {
        let mut file = LocalFile::with_read(path, 0)?;
        let mut buf = BytesMut::zeroed(action.buf_size);
        let mut res = TaskResult::new();

        for _ in action.loop_range() {
            file.read_all(&mut buf[..])?;
            if action.checksum {
                res.update_ck(&buf);
            }
        }

        res.set_len(file.pos() as u64);
        Ok(res)
    }

    async fn fs_write(
        fs: CurvineFileSystem,
        action: Arc<BenchAction>,
        path: String,
    ) -> CommonResult<TaskResult> {
        let data = action.rand_data();
        let path = Path::from_str(path)?;
        let mut res = TaskResult::new();

        let mut writer = fs.create(&path, true).await?;
        for _ in action.loop_range() {
            writer.write(&data).await?;
            if action.checksum {
                res.update_ck(&data);
            }
        }
        writer.complete().await?;

        res.set_len(writer.pos() as u64);
        Ok(res)
    }

    async fn fs_read(
        fs: CurvineFileSystem,
        action: Arc<BenchAction>,
        path: String,
    ) -> CommonResult<TaskResult> {
        let path = Path::from_str(path)?;
        let mut res = TaskResult::new();
        let mut reader = fs.open(&path).await?;
        let mut buf = BytesMut::zeroed(action.buf_size);

        loop {
            let n = reader.read_full(&mut buf).await?;
            if n == 0 {
                break;
            }

            if action.checksum {
                res.update_ck(&buf);
            }
        }
        reader.complete().await?;

        res.set_len(reader.pos() as u64);
        Ok(res)
    }
}
