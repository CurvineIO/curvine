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

use crate::file::{FsContext, FsReaderBase};
use crate::{FileChunk, FileSlice, FILE_MIN_ALIGN_SIZE};
use curvine_common::fs::Path;
use curvine_common::state::FileBlocks;
use curvine_common::FsResult;
use orpc::err_box;
use std::sync::Arc;

// Shards read data
// Split the file into multiple shards to realize concurrent reading of single files and improve single file reading performance.
// If local cache is enabled, the read data will be forced to be aligned according to chunk_size.
#[allow(dead_code)]
pub struct FsReaderParallel {
    inner: FsReaderBase,
    parallel_id: usize,
    chunk_size: i64,
    alloc_slices: Vec<FileSlice>,
    cur_idx: Option<usize>,
    ino: i64,
}

impl FsReaderParallel {
    pub fn from_base(
        inner: FsReaderBase,
        parallel_id: usize,
        chunk_size: i64,
        alloc_slices: Vec<FileSlice>,
        ino: i64,
    ) -> Self {
        Self {
            inner,
            parallel_id,
            alloc_slices,
            cur_idx: None,
            chunk_size,
            ino,
        }
    }

    pub fn create_all(
        path: Path,
        fs_context: Arc<FsContext>,
        file_blocks: FileBlocks,
        read_parallel: i64,
        slice_size: i64,
        chunk_size: usize,
    ) -> FsResult<Vec<Self>> {
        // Check the alignment
        if !chunk_size.is_multiple_of(FILE_MIN_ALIGN_SIZE) || chunk_size < FILE_MIN_ALIGN_SIZE {
            return err_box!(
                "chunk_size must be an integer multiple of {}",
                FILE_MIN_ALIGN_SIZE
            );
        }
        if slice_size % (chunk_size as i64) != 0 || slice_size < (chunk_size as i64) {
            return err_box!("The slice size must be an integer multiple of the chunk size.");
        }

        let ino = file_blocks.status.id;
        let slices = Self::split(file_blocks.status.len, slice_size, read_parallel);

        let mut readers = Vec::with_capacity(read_parallel as usize);
        for (parallel_id, slice) in slices.into_iter().enumerate() {
            if slice.is_empty() {
                // In some cases, there are 2 parallel degrees, but only 1 slice is required, and only an initial reader is needed.
                continue;
            }

            let base = FsReaderBase::new(path.clone(), fs_context.clone(), file_blocks.clone());

            let reader = Self::from_base(base, parallel_id, chunk_size as i64, slice, ino);

            readers.push(reader);
        }

        Ok(readers)
    }

    pub fn split(total_size: i64, slice_size: i64, read_parallel: i64) -> Vec<Vec<FileSlice>> {
        if total_size <= 0 {
            return vec![];
        }

        // The parallelism is 1, no need to be split.
        if read_parallel == 1 {
            return vec![vec![FileSlice::new(0, total_size)]];
        }

        let num_chunks = (total_size + slice_size - 1) / slice_size;
        let mut slices = Vec::with_capacity(read_parallel as usize);
        for _ in 0..read_parallel {
            slices.push(Vec::with_capacity(
                ((num_chunks + 1) / read_parallel) as usize,
            ));
        }

        for slice_id in 0..num_chunks {
            let start = slice_id * slice_size;
            let end = if slice_id == num_chunks - 1 {
                // The last 1 block may be less than the chunk_size size.
                total_size
            } else {
                start + slice_size
            };
            let reader_id = (slice_id % read_parallel) as usize;
            slices[reader_id].push(FileSlice::new(start, end));
        }

        slices
    }

    pub fn parallel_id(&self) -> usize {
        self.parallel_id
    }

    pub async fn read(&mut self) -> FsResult<FileChunk> {
        match self.cur_idx {
            None => {
                // To read the first slice, you need to seek to start position.
                let idx = *self.cur_idx.insert(0);
                self.inner.seek(self.alloc_slices[idx].start).await?;
            }

            Some(idx) if self.inner.pos() >= self.alloc_slices[idx].end => {
                // Switch to the next slice
                let next_idx = idx + 1;
                if next_idx >= self.alloc_slices.len() {
                    return Ok(FileChunk::default());
                } else {
                    let idx = *self.cur_idx.insert(next_idx);
                    self.inner.seek(self.alloc_slices[idx].start).await?;
                }
            }

            _ => (),
        }

        let pos = self.inner.pos();
        let bytes = self.inner.read().await?;
        let chunk = FileChunk::new(pos, bytes);

        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        let align_pos = pos;

        // Dichotomous query to find the first element that does not meet the condition (i.e. the slice of the first x.end > align_pos)
        let index = self.alloc_slices.partition_point(|x| x.end <= align_pos);
        match self.alloc_slices.get(index) {
            Some(v) => {
                let seek_pos = align_pos.max(v.start);
                self.inner.seek(seek_pos).await?;
                let _ = self.cur_idx.insert(index);
            }

            None => {
                if let Some(last_slice) = self.alloc_slices.last() {
                    self.inner.seek(last_slice.end).await?;
                    let _ = self.cur_idx.insert(self.alloc_slices.len() - 1);
                } else {
                    self.inner.seek(0).await?;
                    self.cur_idx = None;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_slices() {
        // total_size = 1000, slice_size = 300, read_parallel = 3
        let slices = FsReaderParallel::split(1000, 300, 3);
        let mut flattened: Vec<_> = slices.into_iter().flatten().collect();

        // Ensure slices are ordered and cover all bytes without overlap
        flattened.sort_by_key(|s| s.start);
        let mut current = 0;
        for slice in &flattened {
            assert_eq!(slice.start, current);
            assert!(slice.end > slice.start);
            current = slice.end;
        }
        assert_eq!(current, 1000);

        // test with parallelism = 1
        let single = FsReaderParallel::split(1000, 300, 1);
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].len(), 1);
        assert_eq!(single[0][0].start, 0);
        assert_eq!(single[0][0].end, 1000);

        // test with total_size = 0
        let empty = FsReaderParallel::split(0, 300, 3);
        assert!(empty.is_empty());
    }
}
