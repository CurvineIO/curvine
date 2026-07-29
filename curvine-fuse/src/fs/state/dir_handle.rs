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
use orpc::err_box;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

use crate::FuseResult;

// Keep a few response batches for seekdir-heavy workloads while placing a
// strict per-handle bound on retained FileStatus values.
const CACHE_BATCHES: usize = 4;

struct InnerStream {
    stream: ListStream,
    cache: VecDeque<Arc<FileStatus>>,
    base_off: usize,
    next_off: usize,
    eof: bool,
}

impl InnerStream {
    pub fn new(stream: ListStream) -> Self {
        Self {
            stream,
            cache: VecDeque::new(),
            base_off: 0,
            next_off: 0,
            eof: false,
        }
    }

    fn push(&mut self, status: FileStatus, keep_from: usize, cache_limit: usize) {
        self.cache.push_back(Arc::new(status));
        self.next_off += 1;

        while self.cache.len() > cache_limit && self.base_off < keep_from {
            self.cache.pop_front();
            self.base_off += 1;
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

    fn cache_limit(&self) -> usize {
        self.limit.saturating_mul(CACHE_BATCHES)
    }

    pub async fn get_batch<F, Fut>(
        &self,
        off: usize,
        new_stream: F,
    ) -> FuseResult<VecDeque<Arc<FileStatus>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = FuseResult<ListStream>>,
    {
        let mut guard = self.guard().await?;

        // Cookies older than the bounded window remain seekable by replaying the
        // backend stream. The common forward path stays within the window.
        if off < guard.base_off {
            *guard = InnerStream::new(new_stream().await?);
        }

        let target = off.saturating_add(self.limit);
        let cache_limit = self.cache_limit();

        while guard.next_off < target && !guard.eof {
            match guard.stream.next().await {
                Some(Ok(status)) => guard.push(status, off, cache_limit),
                Some(Err(e)) => return Err(e.into()),
                None => guard.eof = true,
            }
        }

        let start = off.saturating_sub(guard.base_off);
        Ok(guard
            .cache
            .iter()
            .skip(start)
            .take(self.limit)
            .cloned()
            .collect())
    }

    pub fn set_stream(&mut self, stream: ListStream) {
        self.stream.replace(Mutex::new(InnerStream::new(stream)));
    }
}

#[cfg(test)]
mod tests {
    use super::DirHandle;
    use crate::FuseError;
    use curvine_common::fs::{ListStream, Path};
    use curvine_common::state::FileStatus;
    use orpc::runtime::{AsyncRuntime, RpcRuntime};
    use std::cell::Cell;
    use std::sync::Arc;

    fn entries(names: &[&str]) -> Vec<FileStatus> {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| FileStatus::with_name(index as i64 + 10, name.to_string(), false))
            .collect()
    }

    fn test_entries() -> Vec<FileStatus> {
        entries(&["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])
    }

    fn names(batch: impl IntoIterator<Item = Arc<FileStatus>>) -> Vec<String> {
        batch
            .into_iter()
            .map(|status| status.name.clone())
            .collect()
    }

    #[test]
    fn batches_support_backward_seek_and_rewind() {
        let rt = AsyncRuntime::single();
        rt.block_on(async {
            let path = Path::from_str("/d").unwrap();
            let handle = DirHandle::new(1, 1, &path, 2, ListStream::from_vec(test_entries()));
            let resets = Cell::new(0);

            let fresh = || {
                resets.set(resets.get() + 1);
                async { Ok::<_, FuseError>(ListStream::from_vec(test_entries())) }
            };

            assert_eq!(names(handle.get_batch(0, fresh).await.unwrap()), ["a", "b"]);
            assert_eq!(names(handle.get_batch(2, fresh).await.unwrap()), ["c", "d"]);
            assert_eq!(names(handle.get_batch(4, fresh).await.unwrap()), ["e", "f"]);
            assert_eq!(names(handle.get_batch(6, fresh).await.unwrap()), ["g", "h"]);
            assert_eq!(names(handle.get_batch(8, fresh).await.unwrap()), ["i", "j"]);
            assert!(handle.get_batch(10, fresh).await.unwrap().is_empty());

            // Offset 2 is still in the retained four-batch window.
            assert_eq!(names(handle.get_batch(2, fresh).await.unwrap()), ["c", "d"]);
            assert_eq!(resets.get(), 0);

            // Offset 0 has fallen out of the window and is recovered by replay.
            assert_eq!(names(handle.get_batch(0, fresh).await.unwrap()), ["a", "b"]);
            assert_eq!(resets.get(), 1);

            let guard = handle.guard().await.unwrap();
            assert!(guard.cache.len() <= handle.cache_limit());
            assert_eq!(guard.base_off, 0);
        });
    }

    #[test]
    fn fresh_stream_can_resume_from_nonzero_offset() {
        let rt = AsyncRuntime::single();
        rt.block_on(async {
            let path = Path::from_str("/d").unwrap();
            let handle = DirHandle::new(1, 1, &path, 2, ListStream::from_vec(test_entries()));
            let resets = Cell::new(0);

            let fresh = || {
                resets.set(resets.get() + 1);
                async { Ok::<_, FuseError>(ListStream::from_vec(test_entries())) }
            };

            assert_eq!(names(handle.get_batch(9, fresh).await.unwrap()), ["j"]);
            assert_eq!(resets.get(), 0);

            assert_eq!(names(handle.get_batch(1, fresh).await.unwrap()), ["b", "c"]);
            assert_eq!(resets.get(), 1);

            let guard = handle.guard().await.unwrap();
            assert!(guard.cache.len() <= handle.cache_limit());
            assert_eq!(guard.base_off, 0);
        });
    }
}
