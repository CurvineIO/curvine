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

use crate::io::IOResult;
use crate::sys;
use crate::sys::CInt;
use std::fs::File;

const LONG_READ_THRESHOLD_LEN: i64 = 256 * 1024;

#[allow(unused)]
#[derive(Debug)]
pub struct ReadAheadTask {
    off: i64,
    len: i64,
    handle: IOResult<CInt>,
    last_read_off: i64,
}

/// Operating system page cache manager, currently only supports linux。
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct CacheManager {
    pub read_ahead_len: i64,
    pub drop_cache_len: i64,
    pub enable: bool,
    pub chunk_size: i64,
}

impl CacheManager {
    pub fn new(enable: bool, read_ahead_len: i64, drop_cache_len: i64, chunk_size: i64) -> Self {
        let enable = if cfg!(target_os = "linux") {
            if read_ahead_len <= 0 {
                false
            } else {
                enable
            }
        } else {
            false
        };

        CacheManager {
            read_ahead_len,
            drop_cache_len,
            enable,
            chunk_size,
        }
    }

    pub fn with_place() -> Self {
        Self::new(false, 4 * 1024 * 1024, 1024 * 1024, 128 * 1024 * 1024)
    }

    /// Performs read-ahead operation with simple sequential/random read detection.
    ///
    /// This method uses a simple strategy to detect random reads and disable read-ahead
    /// when random access patterns are detected. This helps reduce read amplification
    /// caused by unnecessary prefetching.
    ///
    /// # Random Read Detection Strategy
    ///
    /// The method detects random reads by comparing the current read position with
    /// the expected next position based on the previous read:
    /// - Sequential read: `cur_pos == last_read_end_pos` (where `last_read_end_pos = last_task.off + chunk_size`)
    /// - Random read: `cur_pos != last_read_end_pos` (position jump detected)
    ///
    /// When a random read is detected, read-ahead is disabled to avoid prefetching
    /// data that is unlikely to be used, reducing unnecessary I/O operations and
    /// read amplification.
    ///
    /// # Backward Seek Handling
    ///
    /// When a backward seek is detected (current position < last read position),
    /// read-ahead is triggered immediately to ensure prefetching works correctly
    /// after backward seeks. This allows read-ahead to resume properly when reading
    /// from a previous position.
    ///
    /// # Arguments
    ///
    /// * `file` - The file handle to perform read-ahead on
    /// * `cur_pos` - Current read position
    /// * `total_len` - Total file length
    /// * `last_task` - Previous read-ahead task (contains last read position and length)
    ///
    /// # Returns
    ///
    /// Returns `Some(ReadAheadTask)` if read-ahead should be performed, `None` otherwise.
    pub fn read_ahead(
        &self,
        file: &File,
        cur_pos: i64,
        total_len: i64,
        mut last_task: Option<ReadAheadTask>,
    ) -> Option<ReadAheadTask> {
        // The file is greater than 256kb, use the read-previous API.It is not necessary to use pre-reading of small files.
        if !self.enable || total_len < LONG_READ_THRESHOLD_LEN {
            // If read preview is not supported, no error will be returned.
            return None;
        };

        // Determine last offset and sequential status, handling backward seeks
        let (last_offset, is_sequential) = match last_task.as_ref() {
            Some(t) if cur_pos < t.last_read_off => (i64::MIN, false),

            Some(t) => (t.off, t.last_read_off + self.chunk_size == cur_pos),

            None => (i64::MIN, true),
        };

        // When cur_pos reaches halfway point, trigger read-ahead
        let next_offset = last_offset + self.read_ahead_len / 2;
        if cur_pos >= next_offset {
            let len = self.read_ahead_len.min(i64::MAX - cur_pos);
            if len <= 0 {
                None
            } else {
                let handle = if is_sequential {
                    sys::read_ahead(file, cur_pos, len)
                } else {
                    Ok(0)
                };

                Some(ReadAheadTask {
                    off: cur_pos,
                    len,
                    handle,
                    last_read_off: cur_pos,
                })
            }
        } else {
            if let Some(task) = last_task.as_mut() {
                task.last_read_off = cur_pos
            }
            last_task
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::with_place()
    }
}
