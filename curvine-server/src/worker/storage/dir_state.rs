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

use curvine_common::state::StorageType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct DirState {
    pub(crate) dir_id: u32,
    pub(crate) base_path: PathBuf,
    pub(crate) storage_type: StorageType,
    #[cfg(feature = "spdk")]
    pub(crate) bdev_name: Option<String>,
    #[cfg(not(feature = "spdk"))]
    #[allow(dead_code)]
    pub(crate) bdev_name: Option<String>,
    pub(crate) bdev_capacity: i64,
    pub(crate) offset_alloc: BdevOffsetAllocator,
}

impl DirState {
    /// Create an offset allocator appropriate for the storage type.
    /// SPDK dirs get a real allocator with bdev capacity; local dirs get a dummy.
    pub fn new_offset_alloc(
        storage_type: StorageType,
        bdev_capacity: i64,
        bdev_block_size: i64,
    ) -> BdevOffsetAllocator {
        if storage_type == StorageType::Spdk {
            BdevOffsetAllocator::new(bdev_capacity, bdev_block_size)
        } else {
            BdevOffsetAllocator::default()
        }
    }
}

/// Default block alignment (512 bytes - traditional HDD sector size).
pub const DEFAULT_BLOCK_ALIGN: i64 = 512;

/// Bdev offset allocator - assigns byte ranges on SPDK bdev to blocks.
/// Thread-safe bump allocator: allocate() advances cursor, free() removes mapping.
/// Space is not reclaimed (append-mostly workload, blocks rarely deleted).
pub struct BdevOffsetAllocator {
    inner: Mutex<OffsetAllocInner>,
}
struct OffsetAllocInner {
    /// Next free byte offset (bump cursor). Always block-aligned.
    cursor: i64,
    /// Alignment for all allocations (typically bdev block size, e.g. 512 or 4096).
    align: i64,
    /// Total bdev capacity in bytes.
    capacity: i64,
    /// Mapping between block_id and (offset, size) .
    map: HashMap<i64, (i64, i64)>,
}
impl BdevOffsetAllocator {
    /// Create a new allocator for a bdev with the given capacity and alignment.
    pub fn new(capacity: i64, align: i64) -> Self {
        Self {
            inner: Mutex::new(OffsetAllocInner {
                cursor: 0,
                align: if align > 0 && (align as u64).is_power_of_two() {
                    align
                } else {
                    DEFAULT_BLOCK_ALIGN
                },
                capacity,
                map: HashMap::new(),
            }),
        }
    }
    /// Allocate bytes for block_id. Returns bdev offset (aligned to block size).
    pub fn allocate(&self, block_id: i64, size: i64) -> Result<i64, String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.map.contains_key(&block_id) {
            return Err(format!(
                "block {} already allocated at offset {}",
                block_id, inner.map[&block_id].0
            ));
        }
        let aligned_size = align_up_i64(size, inner.align);
        let offset = inner.cursor;
        if offset + aligned_size > inner.capacity {
            return Err(format!(
                "bdev full: need {} bytes at offset {}, capacity {}",
                aligned_size, offset, inner.capacity
            ));
        }
        inner.cursor += aligned_size;
        inner.map.insert(block_id, (offset, aligned_size));
        Ok(offset)
    }
    /// Get offset for allocated block.
    pub fn get(&self, block_id: i64) -> Option<i64> {
        let inner = self.inner.lock().unwrap();
        inner.map.get(&block_id).map(|&(off, _)| off)
    }
    /// Get (offset, size) for allocated block.
    pub fn get_entry(&self, block_id: i64) -> Option<(i64, i64)> {
        let inner = self.inner.lock().unwrap();
        inner.map.get(&block_id).copied()
    }
    /// Remove mapping. Space NOT reclaimed (bump allocator).
    /// TODO: add free-list to reuse freed ranges
    pub fn free(&self, block_id: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.remove(&block_id);
    }
    /// Number of allocated blocks.
    pub fn allocated_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.map.len()
    }
    /// Current cursor (next free offset).
    pub fn cursor(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.cursor
    }
}
impl Default for BdevOffsetAllocator {
    fn default() -> Self {
        Self::new(0, DEFAULT_BLOCK_ALIGN)
    }
}

impl BdevOffsetAllocator {
    /// Export state for persistence: Vec<(block_id, offset, size)>.
    pub fn snapshot(&self) -> Vec<(i64, i64, i64)> {
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .iter()
            .map(|(&block_id, &(offset, size))| (block_id, offset, size))
            .collect()
    }

    /// Restore from snapshot. Cursor = max(offset + size) to avoid overlap.
    pub fn restore(&self, entries: &[(i64, i64, i64)]) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.clear();
        let mut max_end: i64 = 0;
        for &(block_id, offset, size) in entries {
            inner.map.insert(block_id, (offset, size));
            let end = offset + size;
            if end > max_end {
                max_end = end;
            }
        }
        if max_end > inner.cursor {
            inner.cursor = max_end;
        }
    }

    /// Alignment value used by this allocator.
    pub fn align(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.align
    }
}

impl std::fmt::Debug for BdevOffsetAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BdevOffsetAllocator")
            .field("cursor", &inner.cursor)
            .field("capacity", &inner.capacity)
            .field("blocks", &inner.map.len())
            .finish()
    }
}
#[inline]
fn align_up_i64(n: i64, align: i64) -> i64 {
    (n + align - 1) & !(align - 1)
}
// ---------------------------------------------------------------------------
// Tests
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn sequential_alloc() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        assert_eq!(a.allocate(1, 1000).unwrap(), 0);
        assert_eq!(a.allocate(2, 4096).unwrap(), 4096);
        assert_eq!(a.get(1), Some(0));
    }
    #[test]
    fn full_bdev() {
        let a = BdevOffsetAllocator::new(8192, 4096);
        a.allocate(1, 4096).unwrap();
        a.allocate(2, 4096).unwrap();
        assert!(a.allocate(3, 4096).is_err());
    }
    #[test]
    fn duplicate_block() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        a.allocate(1, 512).unwrap();
        assert!(a.allocate(1, 512).is_err());
    }
    #[test]
    fn free_mapping() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        a.allocate(1, 512).unwrap();
        a.free(1);
        assert_eq!(a.get(1), None);
    }
    #[test]
    fn restore_no_overlap() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a.allocate(1, 4096).unwrap();
        a.allocate(2, 8192).unwrap();
        let snap = a.snapshot();
        let a2 = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a2.restore(&snap);
        assert_eq!(a2.cursor(), 12288);
    }
}
