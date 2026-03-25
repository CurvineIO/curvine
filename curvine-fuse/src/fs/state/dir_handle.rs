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

use curvine_common::fs::{ListStream, Path};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use futures::StreamExt;
use log::info;
use orpc::err_box;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::mem;
use tokio::sync::{Mutex, MutexGuard};

struct InnerStream {
    stream: ListStream,
    buf: VecDeque<FileStatus>,
    index: usize,
}

impl InnerStream {
    pub fn new(stream: ListStream) -> Self {
        Self {
            stream,
            buf: VecDeque::new(),
            index: 0,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct DirHandle {
    pub ino: u64,
    pub fh: u64,
    pub path: String,

    #[serde(skip, default)]
    stream: Option<Mutex<InnerStream>>,
    limit: usize,
}

impl DirHandle {
    pub fn new(ino: u64, fh: u64, path: &Path, limit: usize, stream: ListStream) -> Self {
        Self {
            ino,
            fh,
            path: path.clone_uri(),
            stream: Some(Mutex::new(InnerStream::new(stream))),
            limit,
        }
    }

    async fn guard(&self) -> FsResult<MutexGuard<'_, InnerStream>> {
        match self.stream {
            Some(ref stream) => Ok(stream.lock().await),
            None => err_box!("path {} list stream not init", self.path),
        }
    }

    pub async fn get_batch(&self, off: usize) -> FsResult<VecDeque<FileStatus>> {
        let mut guard = self.guard().await?;

        if off > 0 && guard.index == 0 {
            info!(
                "readdir {} skipping {} list entries (resume offset / inner index reset)",
                self.path, off
            );
            while guard.index < off {
                match guard.stream.next().await {
                    Some(Ok(_)) => guard.index += 1,
                    Some(Err(e)) => return Err(e),
                    None => {
                        return err_box!(
                            "path {} list stream ended before offset {}",
                            self.path,
                            off
                        );
                    }
                }
            }
        }

        while guard.buf.len() < self.limit {
            match guard.stream.next().await {
                Some(Ok(s)) => {
                    guard.buf.push_back(s);
                    guard.index += 1;
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }

        Ok(mem::take(&mut guard.buf))
    }

    pub async fn set_buf(&self, buf: VecDeque<FileStatus>) -> FsResult<()> {
        let mut guard = self.guard().await?;
        guard.buf = buf;
        Ok(())
    }

    pub fn set_stream(&mut self, stream: ListStream) {
        self.stream.replace(Mutex::new(InnerStream::new(stream)));
    }
}
