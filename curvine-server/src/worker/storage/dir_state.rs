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
use std::collections::{BTreeMap, HashMap};
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
        if storage_type == StorageType::SpdkDisk {
            BdevOffsetAllocator::new(bdev_capacity, bdev_block_size)
        } else {
            BdevOffsetAllocator::default()
        }
    }
}

/// Default block alignment (512 bytes - traditional HDD sector size).
pub const DEFAULT_BLOCK_ALIGN: i64 = 512;

/// Bdev offset allocator - assigns byte ranges on SPDK bdev to blocks.
/// Thread-safe bump allocator with BTreeMap free-list for reusing freed ranges.
/// allocate() first scans the free-list (first-fit), then falls back to bump cursor.
/// free() returns space to the free-list with adjacent coalescing.
/// TODO: optimize with two BTreeMaps and a hybrid of slab/bitmap vs BTreeMap/two-BTreeMap
/// TODO: handle fragmentation
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
    /// Free-list of reclaimed ranges: offset -> size (sorted by offset).
    free_list: BTreeMap<i64, i64>,
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
                free_list: BTreeMap::new(),
            }),
        }
    }
    /// Allocate bytes for block_id. Returns bdev offset (aligned to block size).
    /// First checks the free-list (first-fit), then falls back to bump cursor.
    pub fn allocate(&self, block_id: i64, size: i64) -> Result<i64, String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.map.contains_key(&block_id) {
            return Err(format!(
                "block {} already allocated at offset {}",
                block_id, inner.map[&block_id].0
            ));
        }
        let aligned_size = align_up_i64(size, inner.align);

        // Try free-list first
        let candidate = inner
            .free_list
            .iter()
            .find(|(_, &v)| v >= aligned_size)
            .map(|(&k, &v)| (k, v));
        if let Some((offset, fsize)) = candidate {
            inner.free_list.remove(&offset);
            if fsize > aligned_size {
                inner
                    .free_list
                    .insert(offset + aligned_size, fsize - aligned_size);
            }
            inner.map.insert(block_id, (offset, aligned_size));
            return Ok(offset);
        }

        // Fall back to bump
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
    /// Remove mapping and return space to the free-list with coalescing.
    pub fn free(&self, block_id: i64) {
        let mut inner = self.inner.lock().unwrap();
        let (offset, size) = match inner.map.remove(&block_id) {
            Some(v) => v,
            None => return,
        };

        // Find adjacent free entries for coalescing
        let prev = inner.free_list.range(..offset).last().and_then(|(&k, &v)| {
            if k + v == offset {
                Some((k, v))
            } else {
                None
            }
        });
        let next = inner
            .free_list
            .range(offset + size..)
            .next()
            .and_then(|(&k, &v)| {
                if k == offset + size {
                    Some((k, v))
                } else {
                    None
                }
            });

        if let Some((k, _)) = prev {
            inner.free_list.remove(&k);
        }
        if let Some((k, _)) = next {
            inner.free_list.remove(&k);
        }

        let new_offset = prev.map_or(offset, |(k, _)| k);
        let new_size = size + prev.map_or(0, |(_, v)| v) + next.map_or(0, |(_, v)| v);
        inner.free_list.insert(new_offset, new_size);
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

    /// Restore from snapshot. Rebuilds map, cursor = max(offset + size), and reconstructs
    /// the free-list from gaps between allocated blocks within [0, cursor).
    pub fn restore(&self, entries: &[(i64, i64, i64)]) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.clear();
        inner.free_list.clear();

        let mut sorted: Vec<(i64, i64, i64)> = entries.to_vec();
        sorted.sort_by_key(|&(_, offset, _)| offset);

        let mut prev_end: i64 = 0;
        for &(block_id, offset, size) in &sorted {
            inner.map.insert(block_id, (offset, size));
            if offset > prev_end {
                inner.free_list.insert(prev_end, offset - prev_end);
            }
            let end = offset + size;
            if end > prev_end {
                prev_end = end;
            }
        }

        if prev_end > inner.cursor {
            inner.cursor = prev_end;
        }
        // Gap between last block and cursor is also free
        if prev_end < inner.cursor {
            let remaining = inner.cursor - prev_end;
            inner.free_list.insert(prev_end, remaining);
        }
    }

    /// Alignment value used by this allocator.
    pub fn align(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.align
    }

    /// Size of the free-list (number of free ranges).
    pub fn free_list_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.free_list.len()
    }

    /// Total free bytes in the free-list.
    pub fn free_list_bytes(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.free_list.iter().map(|(_, &v)| v).sum()
    }

    #[cfg(test)]
    /// Expose free-list entries for test introspection.
    pub fn free_list_entries(&self) -> Vec<(i64, i64)> {
        let inner = self.inner.lock().unwrap();
        inner.free_list.iter().map(|(&k, &v)| (k, v)).collect()
    }
}

impl std::fmt::Debug for BdevOffsetAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BdevOffsetAllocator")
            .field("cursor", &inner.cursor)
            .field("capacity", &inner.capacity)
            .field("blocks", &inner.map.len())
            .field("free_ranges", &inner.free_list.len())
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
    fn free_list_reuse_offset() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        assert_eq!(a.allocate(1, 4096).unwrap(), 0);
        assert_eq!(a.allocate(2, 4096).unwrap(), 4096);
        a.free(1);
        // Next alloc should reuse offset 0 from free-list
        assert_eq!(a.allocate(3, 4096).unwrap(), 0);
    }
    #[test]
    fn free_list_first_fit() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        assert_eq!(a.allocate(1, 4096).unwrap(), 0);
        assert_eq!(a.allocate(2, 4096).unwrap(), 4096);
        assert_eq!(a.allocate(3, 4096).unwrap(), 8192);
        // Free blocks at 0 and 8192
        a.free(1);
        a.free(3);
        // First-fit should return offset 0 (smallest that fits)
        assert_eq!(a.allocate(4, 4096).unwrap(), 0);
        // Next should return offset 8192
        assert_eq!(a.allocate(5, 4096).unwrap(), 8192);
    }
    #[test]
    fn free_list_coalesce_adjacent() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        assert_eq!(a.allocate(1, 4096).unwrap(), 0);
        assert_eq!(a.allocate(2, 4096).unwrap(), 4096);
        assert_eq!(a.allocate(3, 4096).unwrap(), 8192);
        // Free middle, then left —> coalesce
        a.free(2);
        a.free(1);
        let entries = a.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (0, 8192));
        // Free right — single entry of 12288
        a.free(3);
        let entries = a.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (0, 12288));
    }
    #[test]
    fn free_list_coalesce_three_way() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        assert_eq!(a.allocate(1, 512).unwrap(), 0);
        assert_eq!(a.allocate(2, 512).unwrap(), 512);
        assert_eq!(a.allocate(3, 512).unwrap(), 1024);
        // Free all in reverse order — should coalesce into one
        a.free(3);
        a.free(2);
        a.free(1);
        let entries = a.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (0, 1536));
    }
    #[test]
    fn free_list_split_entry() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a.allocate(1, 8192).unwrap();
        a.free(1);
        // Allocate smaller — splits the free entry
        assert_eq!(a.allocate(2, 4096).unwrap(), 0);
        let entries = a.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (4096, 4096));
    }
    #[test]
    fn free_list_exact_fit() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a.allocate(1, 4096).unwrap();
        a.free(1);
        // Allocate exact same size — entry removed entirely
        assert_eq!(a.allocate(2, 4096).unwrap(), 0);
        assert_eq!(a.free_list_size(), 0);
    }
    #[test]
    fn free_list_bump_fallback() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        a.allocate(1, 512).unwrap();
        a.free(1); // free_list: [(0, 512)]
                   // Free entry too small — falls back to bump
        assert_eq!(a.allocate(2, 1024).unwrap(), 512);
        // Original 512-byte hole remains
        let entries = a.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (0, 512));
    }
    #[test]
    fn free_list_fragmented_stress() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        let mut total_freed: i64 = 0;
        let mut allocated: Vec<i64> = Vec::new();
        for i in 1..=20 {
            let block_id = i;
            a.allocate(block_id, 512).unwrap();
            allocated.push(block_id);
        }
        // Free every other block
        for (idx, &id) in allocated.iter().enumerate() {
            if idx % 2 == 0 {
                a.free(id);
                total_freed += 512;
            }
        }
        assert_eq!(a.free_list_bytes(), total_freed);
        assert_eq!(a.allocated_count(), 10);
    }
    #[test]
    fn free_nonexistent_block() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 512);
        a.free(999); // no panic
        assert_eq!(a.free_list_size(), 0);
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
        assert_eq!(a2.free_list_size(), 0); // no gaps, no free entries
    }
    #[test]
    fn restore_rebuilds_free_list() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a.allocate(1, 4096).unwrap(); // offset 0
        a.allocate(2, 4096).unwrap(); // offset 4096
        a.allocate(3, 4096).unwrap(); // offset 8192
        a.allocate(4, 4096).unwrap(); // offset 12288, cursor=16384
                                      // Free block 2 (gap between 1 and 3) and block 4 (past the rest)
        a.free(2);
        a.free(4);
        // Snapshot: only blocks 1 and 3 remain
        let snap = a.snapshot();
        assert_eq!(snap.len(), 2);
        // Restore — cursor = max_end = 12288
        let a2 = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a2.restore(&snap);
        // Free-list: gap between block 1 (0) and block 3 (8192)
        let entries = a2.free_list_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (4096, 4096));
        // Block 4's freed space at 12288 absorbed into bump (not persisted past max_end, usable via bump)
        // First alloc reuses 4096 from free-list
        assert_eq!(a2.allocate(5, 4096).unwrap(), 4096);
        // Second alloc bumps to 12288 (block 4's old offset)
        assert_eq!(a2.allocate(6, 4096).unwrap(), 12288);
    }
    #[test]
    fn restore_no_gaps() {
        let a = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a.allocate(1, 4096).unwrap();
        a.allocate(2, 4096).unwrap();
        let snap = a.snapshot();
        let a2 = BdevOffsetAllocator::new(1024 * 1024, 4096);
        a2.restore(&snap);
        // Tightly packed — no gaps
        assert_eq!(a2.free_list_size(), 0);
    }
    #[test]
    fn allocate_after_free_full_capacity() {
        let a = BdevOffsetAllocator::new(8192, 4096);
        a.allocate(1, 4096).unwrap();
        a.allocate(2, 4096).unwrap(); // full
        assert!(a.allocate(3, 4096).is_err());
        a.free(2); // free one
        assert_eq!(a.allocate(3, 4096).unwrap(), 4096); // reuse freed
        assert!(a.allocate(4, 4096).is_err()); // still full
    }
    #[test]
    fn fragmented_bdev_rejects_large_alloc() {
        let a = BdevOffsetAllocator::new(16384, 4096);
        a.allocate(1, 4096).unwrap();
        a.allocate(2, 4096).unwrap();
        a.allocate(3, 4096).unwrap();
        a.allocate(4, 4096).unwrap();
        // Free non-adjacent blocks: holes at 0 and 8192
        a.free(1);
        a.free(3);
        assert_eq!(a.free_list_bytes(), 8192); // total free = 8K
                                               // No single hole is >= 8K — should fail
        assert!(a.allocate(5, 8192).is_err());
        // Small allocs still work
        assert_eq!(a.allocate(5, 4096).unwrap(), 0);
        assert_eq!(a.allocate(6, 4096).unwrap(), 8192);
    }
}
