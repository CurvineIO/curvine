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

//! `Packed` child edges: file edges are inlined as variable-length byte records
//! inside capacity-graded chunks and directory objects live in a side vector,
//! cutting a file edge from ~124 B down to ~30 B.
//!
//! Pointer contract: `InodePtr` is a raw pointer without lifetime tracking.
//! Bytes inside the packed record area must never be exposed as `&InodeView` or
//! `InodePtr` because records move on insert/delete/split. File hits only expose
//! `name + id` (load the full inode from the store); directory pointers always
//! point at `Box`es owned by the side vector, whose addresses never move.
//!
//! Growing, splitting and compacting only move the `Box` value inside the side
//! vector, never the directory it points at, so a directory pointer outlives
//! every one of those. Only these three drop the pointee and must be treated as
//! the invalidation points, because none of them is visible in a signature:
//!
//! * `delete_child` on that name moves the directory out and frees the `Box`;
//! * cloning the container deep-copies the side vector, so a pointer handed out
//!   by one copy never addresses the other;
//! * cloning a file pointer clears `RawPtr::owned`, so the copy dangles once the
//!   original is dropped. File edges can only be handed out as owned pointers,
//!   which makes `Vec<InodePtr>` a mix of owned and borrowed elements: move them
//!   into queues and stacks instead of cloning them.

use crate::master::meta::inode::ChildItem;
use crate::master::meta::inode::PackedIter;
use crate::master::meta::inode::{InodePtr, InodeView};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::ListOptions;
use curvine_record::{Chunk, InsertOutcome, Layout, Record};
use glob::Pattern;
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct PackedChildren {
    area: Record,
    dirs: Vec<Option<Box<InodeView>>>,
    free_slots: Vec<u32>,
}

impl Default for PackedChildren {
    fn default() -> Self {
        Self {
            area: Record::new(Self::LAYOUT),
            dirs: Vec::new(),
            free_slots: Vec::new(),
        }
    }
}

impl Debug for PackedChildren {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedChildren")
            .field("len", &self.area.len())
            .field("chunks", &self.area.chunk_count())
            .field("dirs", &self.dirs.len())
            .field("holes", &self.free_slots.len())
            .finish()
    }
}

impl PackedChildren {
    const LAYOUT: Layout = Layout::new(1, 1);
    const KIND_OFFSET: usize = 0;
    const ID_OFFSET: usize = 1;
    const DIR_IDX_OFFSET: usize = Self::ID_OFFSET + 8;
    const FILE_VALUE_LEN: usize = Self::DIR_IDX_OFFSET;
    const DIR_VALUE_LEN: usize = Self::DIR_IDX_OFFSET + 4;
    const KIND_FILE: u8 = 0;
    const KIND_DIR: u8 = 1;
    pub(super) const NAME_MAX: usize = Self::LAYOUT.max_key_len();
    const HOLE_RATIO: usize = 4;
    const HOLE_LIMIT: usize = 1024;

    #[inline]
    fn record_value(buf: &[u8], off: usize) -> &[u8] {
        &buf[Self::LAYOUT.value_range(buf, off)]
    }

    #[inline]
    fn record_kind(buf: &[u8], off: usize) -> u8 {
        Self::record_value(buf, off)[Self::KIND_OFFSET]
    }

    #[inline]
    pub(super) fn record_len(buf: &[u8], off: usize) -> usize {
        Self::LAYOUT.record_len(buf, off)
    }

    #[inline]
    fn record_name(buf: &[u8], off: usize) -> &str {
        let key = &buf[Self::LAYOUT.key_range(buf, off)];
        // SAFETY: every key in the area was written by `add_child` from a `&str`
        // name, so the key bytes are always valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(key) }
    }

    #[inline]
    fn record_id(buf: &[u8], off: usize) -> i64 {
        let value = Self::record_value(buf, off);
        let mut id = [0u8; 8];
        id.copy_from_slice(&value[Self::ID_OFFSET..Self::DIR_IDX_OFFSET]);
        i64::from_le_bytes(id)
    }

    #[inline]
    fn record_dir_idx(buf: &[u8], off: usize) -> u32 {
        let value = Self::record_value(buf, off);
        let mut idx = [0u8; 4];
        idx.copy_from_slice(&value[Self::DIR_IDX_OFFSET..Self::DIR_VALUE_LEN]);
        u32::from_le_bytes(idx)
    }

    #[inline]
    pub(super) fn packed_item<'a>(
        dirs: &'a [Option<Box<InodeView>>],
        chunk: &'a Chunk,
        off: usize,
    ) -> ChildItem<'a> {
        let buf = chunk.buf();
        if Self::record_kind(buf, off) == Self::KIND_DIR {
            match dirs
                .get(Self::record_dir_idx(buf, off) as usize)
                .and_then(|slot| slot.as_deref())
            {
                Some(view) => return ChildItem::View(view),
                None => debug_assert!(false, "dir record points at a missing side-vector slot"),
            }
        }
        ChildItem::FileEntry {
            name: Self::record_name(buf, off),
            id: Self::record_id(buf, off),
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.area.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.area.len() == 0
    }

    fn alloc_slot(&mut self) -> u32 {
        match self.free_slots.pop() {
            Some(idx) => idx,
            None => {
                let idx = self.dirs.len() as u32;
                self.dirs.push(None);
                idx
            }
        }
    }

    pub fn get_child(&self, name: &str) -> Option<InodePtr> {
        let (ci, off) = self.area.find(name.as_bytes()).ok()?;
        Some(Self::packed_item(&self.dirs, &self.area.chunks()[ci], off).as_ptr())
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        if inode.name().len() > Self::NAME_MAX {
            return err_box!(
                "Child name too long: {} bytes (max {})",
                inode.name().len(),
                Self::NAME_MAX
            );
        }
        let name = inode.name().to_string();
        let id = inode.id();

        if inode.is_dir() {
            let boxed = Box::new(inode);
            let slot = self.alloc_slot();
            self.dirs[slot as usize] = Some(boxed);
            let inserted = matches!(
                self.area
                    .insert_with(name.as_bytes(), Self::DIR_VALUE_LEN, |value| {
                        value[Self::KIND_OFFSET] = Self::KIND_DIR;
                        value[Self::ID_OFFSET..Self::DIR_IDX_OFFSET]
                            .copy_from_slice(&id.to_le_bytes());
                        value[Self::DIR_IDX_OFFSET..].copy_from_slice(&slot.to_le_bytes());
                    }),
                InsertOutcome::Inserted
            );
            if !inserted {
                self.dirs[slot as usize] = None;
                self.free_slots.push(slot);
                return err_box!("Child {} already exists", name);
            }
            match self.dirs[slot as usize].as_deref() {
                Some(view) => Ok(InodePtr::from_ref(view)),
                None => err_box!("Packed dir slot {} is empty", slot),
            }
        } else {
            let inserted = matches!(
                self.area
                    .insert_with(name.as_bytes(), Self::FILE_VALUE_LEN, |value| {
                        value[Self::KIND_OFFSET] = Self::KIND_FILE;
                        value[Self::ID_OFFSET..].copy_from_slice(&id.to_le_bytes());
                    }),
                InsertOutcome::Inserted
            );
            if !inserted {
                return err_box!("Child {} already exists", name);
            }
            Ok(InodePtr::from_owned(inode))
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let (ci, off) = match self.area.find(child_name.as_bytes()) {
            Ok(v) => v,
            Err(_) => return err_box!("Child {} not exists", child_name),
        };

        let chunk = &self.area.chunks()[ci];
        let id = Self::record_id(chunk.buf(), off);
        if id != child_id {
            return err_box!(
                "Inode status error, expect id {}, actually delete {}",
                child_id,
                id
            );
        }

        if Self::record_kind(chunk.buf(), off) == Self::KIND_DIR {
            let slot = Self::record_dir_idx(chunk.buf(), off) as usize;
            let Some(boxed) = self.dirs.get_mut(slot).and_then(Option::take) else {
                return err_box!("Packed dir slot {} is missing or empty", slot);
            };
            self.free_slots.push(slot as u32);
            self.area.remove(ci, off);
            self.maybe_compact();
            Ok(*boxed)
        } else {
            self.area.remove(ci, off);
            Ok(InodeView::new_entry(child_name.to_string(), id))
        }
    }

    pub fn iter(&self) -> PackedIter<'_> {
        PackedIter::new(&self.dirs, self.area.chunks(), self.area.len())
    }

    /// Move directory boxes without changing their addresses; file edges are
    /// reconstructed because they are stored inline in the record area.
    pub(super) fn into_entries(mut self) -> Vec<(String, Box<InodeView>)> {
        let mut entries = Vec::with_capacity(self.area.len());
        for chunk in self.area.chunks() {
            let mut offset = 0;
            while offset < chunk.used() {
                let buf = chunk.buf();
                let name = Self::record_name(buf, offset).to_owned();
                let view = if Self::record_kind(buf, offset) == Self::KIND_DIR {
                    let slot = Self::record_dir_idx(buf, offset) as usize;
                    self.dirs[slot]
                        .take()
                        .expect("packed directory slot missing")
                } else {
                    Box::new(InodeView::new_entry(
                        name.clone(),
                        Self::record_id(buf, offset),
                    ))
                };
                entries.push((name, view));
                offset += Self::record_len(buf, offset);
            }
        }
        entries
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<InodePtr> {
        let mut out = Vec::new();
        let limit = opts.limit.unwrap_or(usize::MAX);
        let chunks = self.area.chunks();

        let (mut ci, mut off) = match opts.start_after.as_deref() {
            None => (0, 0),
            Some(start) => match self.area.find(start.as_bytes()) {
                Ok((c, o)) => (c, o + Self::record_len(chunks[c].buf(), o)),
                Err((c, o)) => (c, o),
            },
        };

        while ci < chunks.len() && out.len() < limit {
            let chunk = &chunks[ci];
            if off >= chunk.used() {
                ci += 1;
                off = 0;
                continue;
            }
            out.push(Self::packed_item(&self.dirs, chunk, off).as_ptr());
            off += Self::record_len(chunk.buf(), off);
        }
        out
    }

    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodePtr> {
        self.iter()
            .filter(|item| glob_pattern.matches(item.name()))
            .map(|item| item.as_ptr())
            .collect()
    }

    fn maybe_compact(&mut self) {
        let holes = self.free_slots.len();
        if holes > Self::HOLE_LIMIT && holes > self.dirs.len() / Self::HOLE_RATIO {
            self.compact_dirs();
        }
    }

    fn compact_dirs(&mut self) {
        let mut remap = vec![u32::MAX; self.dirs.len()];
        let live = self.dirs.len().saturating_sub(self.free_slots.len());
        let mut dirs = Vec::with_capacity(live);
        for (old_idx, slot) in self.dirs.drain(..).enumerate() {
            if let Some(boxed) = slot {
                remap[old_idx] = dirs.len() as u32;
                dirs.push(Some(boxed));
            }
        }
        self.dirs = dirs;
        self.free_slots.clear();
        self.area.rewrite(|value| {
            if value[Self::KIND_OFFSET] == Self::KIND_DIR {
                let mut slot = [0u8; 4];
                slot.copy_from_slice(&value[Self::DIR_IDX_OFFSET..Self::DIR_VALUE_LEN]);
                // A slot outside the remap table means the record no longer owns
                // a live directory. Keep the sentinel instead of the stale index:
                // the stale index would silently resolve to another real dir.
                let new = remap
                    .get(u32::from_le_bytes(slot) as usize)
                    .copied()
                    .unwrap_or(u32::MAX);
                debug_assert_ne!(new, u32::MAX, "dir record points at a dead slot");
                let slot_end = Self::DIR_VALUE_LEN;
                value[Self::DIR_IDX_OFFSET..slot_end].copy_from_slice(&new.to_le_bytes());
            }
        });
    }
}
