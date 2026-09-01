use crate::{FuseMetrics, FuseResult};
use bytes::{Bytes, BytesMut};
use curvine_core_error::err_box;
use curvine_io::LocalFile;
use curvine_runtime::common::Utils;
use log::warn;
use std::{fmt, fs, mem};

pub(crate) enum StoreInner {
    Buf(BytesMut),
    Bytes(Bytes),
    Disk(LocalFile),
}

impl fmt::Debug for StoreInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreInner::Buf(_) => f.debug_tuple("Store").field(&format_args!("Buf")).finish(),
            StoreInner::Bytes(_) => f
                .debug_tuple("Store")
                .field(&format_args!("Bytes"))
                .finish(),
            StoreInner::Disk(_) => f.debug_tuple("Store").field(&format_args!("File")).finish(),
        }
    }
}

impl StoreInner {
    pub fn write(&mut self, off: u64, data: &[u8]) -> FuseResult<()> {
        match self {
            StoreInner::Buf(bytes) => {
                let off = off as usize;
                let end = off + data.len();
                if end > bytes.len() {
                    bytes.resize(end, 0);
                }
                bytes[off..end].copy_from_slice(data);
                Ok(())
            }

            StoreInner::Disk(file) => {
                let fd = curvine_sys::get_raw_io(file)?;
                curvine_sys::pwrite_full(fd, data, off)?;
                Ok(())
            }

            StoreInner::Bytes(_) => err_box!("write to bytes store is not supported"),
        }
    }

    pub fn read(&self, off: u64, len: usize) -> FuseResult<Bytes> {
        match self {
            StoreInner::Buf(bytes) => {
                let off = off as usize;
                let end = off + len;
                if end > bytes.len() {
                    return err_box!("out of range");
                }
                Ok(Bytes::copy_from_slice(&bytes[off..end]))
            }

            StoreInner::Bytes(bytes) => {
                let off = off as usize;
                let end = off + len;
                if end > bytes.len() {
                    return err_box!("out of range");
                }
                Ok(bytes.slice(off..end))
            }

            StoreInner::Disk(file) => {
                let raw_io = curvine_sys::get_raw_io(file)?;
                let mut bytes = Utils::uninit_bytes_mut(len);
                curvine_sys::pread_full(raw_io, &mut bytes, off)?;
                Ok(bytes.freeze())
            }
        }
    }
}

#[derive(Debug)]
pub struct Page {
    pub(crate) page_id: u64,
    pub(crate) store: StoreInner,
    valid_ranges: Vec<(u64, u64)>,
    dirty_ranges: Vec<(u64, u64)>,
    pub(crate) flushing: bool,
    counted: bool,
}

impl Page {
    pub(crate) fn new(page_id: u64, store: StoreInner) -> Self {
        FuseMetrics::with(|m| m.write_back_cache_page_num.inc());
        Page {
            page_id,
            store,
            valid_ranges: Vec::new(),
            dirty_ranges: Vec::new(),
            flushing: false,
            counted: true,
        }
    }

    pub fn with_disk(page_id: u64, file: LocalFile) -> Self {
        Self::new(page_id, StoreInner::Disk(file))
    }

    pub fn with_memory(page_id: u64, bytes: BytesMut) -> Self {
        Self::new(page_id, StoreInner::Buf(bytes))
    }

    pub fn len(&self) -> u64 {
        match (self.valid_ranges.first(), self.valid_ranges.last()) {
            (Some(&(start, _)), Some(&(_, end))) => end - start,
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.valid_ranges.is_empty()
    }

    pub fn write(&mut self, off: u64, data: &[u8]) -> FuseResult<()> {
        self.store.write(off, data)?;
        let end = off + data.len() as u64;

        Self::insert_range(&mut self.valid_ranges, off, end);
        Self::insert_range(&mut self.dirty_ranges, off, end);
        Ok(())
    }

    pub fn mark_dirty(&mut self, off: u64, end: u64) {
        Self::insert_range(&mut self.dirty_ranges, off, end);
    }

    fn insert_range(ranges: &mut Vec<(u64, u64)>, off: u64, end: u64) {
        if off >= end {
            return;
        }

        let mut off = off;
        let mut end = end;

        // First range whose end >= off (may overlap or touch).
        let pos = ranges.partition_point(|&(_, e)| e < off);

        // Absorb all consecutive overlapping/touching ranges from pos.
        let mut remove_end = pos;
        while remove_end < ranges.len() && ranges[remove_end].0 <= end {
            off = off.min(ranges[remove_end].0);
            end = end.max(ranges[remove_end].1);
            remove_end += 1;
        }

        ranges.splice(pos..remove_end, [(off, end)]);
    }

    pub fn dirty_ranges(&self) -> &[(u64, u64)] {
        &self.dirty_ranges
    }

    pub(crate) fn finish_flush(&mut self) {
        self.flushing = false;
    }

    pub fn restore_dirty(&mut self, ranges: &[(u64, u64)]) {
        for &(off, end) in ranges {
            self.mark_dirty(off, end);
        }
        self.flushing = false;
    }

    pub fn is_dirty(&self) -> bool {
        !self.dirty_ranges.is_empty()
    }

    pub fn readable_len(&self, off: u64) -> Option<u64> {
        let pos = self.valid_ranges.partition_point(|&(_, end)| end <= off);
        let &(start, end) = self.valid_ranges.get(pos)?;
        (start <= off && off < end).then_some(end - off)
    }

    pub fn is_full(&self, page_size: u64) -> bool {
        self.valid_ranges.as_slice() == [(0, page_size)]
    }

    pub fn read_dirty(&mut self) -> FuseResult<Vec<(u64, Bytes)>> {
        if self.dirty_ranges.is_empty() {
            return Ok(vec![]);
        }

        const CHUNK_SIZE: usize = 128 * 1024;
        let mut out = Vec::with_capacity(self.dirty_ranges.len());
        for &(off, end) in &self.dirty_ranges {
            let mut cur = off;
            while cur < end {
                let len = std::cmp::min(CHUNK_SIZE, (end - cur) as usize);
                let data = self.store.read(cur, len)?;
                out.push((self.page_id + cur, data));
                cur += len as u64;
            }
        }
        Ok(out)
    }

    pub fn read(&self, off: u64, len: usize) -> FuseResult<Bytes> {
        self.store.read(off, len)
    }

    pub fn start(&self) -> u64 {
        self.valid_ranges.first().map_or(0, |&(start, _)| start)
    }

    pub fn end(&self) -> u64 {
        self.valid_ranges.last().map_or(0, |&(_, end)| end)
    }

    pub fn is_disk(&self) -> bool {
        matches!(self.store, StoreInner::Disk(_))
    }

    pub fn path(&self) -> Option<&str> {
        match &self.store {
            StoreInner::Disk(file) => Some(file.path()),
            _ => None,
        }
    }

    pub fn clear(&mut self) -> u64 {
        let len = self.len();
        if let StoreInner::Disk(file) = &mut self.store {
            if let Err(e) = fs::remove_file(file.path()) {
                warn!("failed to remove file: {}", e);
            }
        }

        self.valid_ranges.clear();
        self.dirty_ranges.clear();
        self.flushing = false;
        len
    }

    /// Take dirty data for background flush. When the page is not full, returns
    /// `None` so the tick worker only flushes complete pages.
    pub fn take_dirty(&mut self, page_size: u64) -> FuseResult<Option<Page>> {
        self.take_dirty_inner(page_size, true)
    }

    /// Take any dirty data, including partial pages (complete / evict / flush_pages).
    pub fn take_all_dirty(&mut self, page_size: u64) -> FuseResult<Option<Page>> {
        self.take_dirty_inner(page_size, false)
    }

    fn take_dirty_inner(&mut self, page_size: u64, full_only: bool) -> FuseResult<Option<Page>> {
        if self.dirty_ranges.is_empty() || self.is_empty() || self.flushing {
            return Ok(None);
        }
        if self.len() > page_size {
            return err_box!("page is too large");
        }
        if full_only && !self.is_full(page_size) {
            return Ok(None);
        }

        let store = match &self.store {
            StoreInner::Buf(bytes) => StoreInner::Bytes(BytesMut::from(&bytes[..]).freeze()),

            StoreInner::Bytes(bytes) => StoreInner::Bytes(bytes.clone()),

            StoreInner::Disk(file) => {
                let file = LocalFile::with_read(file.path(), 0)?;
                StoreInner::Disk(file)
            }
        };
        let dirty_ranges = mem::take(&mut self.dirty_ranges);

        self.flushing = true;
        let page = Self {
            page_id: self.page_id,
            store,
            valid_ranges: self.valid_ranges.clone(),
            dirty_ranges,
            flushing: false,
            counted: false,
        };
        Ok(Some(page))
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        if self.counted {
            FuseMetrics::with(|m| m.write_back_cache_page_num.dec());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_page(page_id: u64) -> Page {
        Page::with_memory(page_id, BytesMut::new())
    }

    #[test]
    fn write_sequential_expands_len() {
        let mut page = mem_page(0);
        page.write(0, b"abc").unwrap();
        assert_eq!((page.start(), page.len()), (0, 3));
        page.write(3, b"def").unwrap();
        assert_eq!((page.start(), page.len()), (0, 6));
    }

    #[test]
    fn write_backwards_expands_start() {
        let mut page = mem_page(0);
        page.write(100, b"abc").unwrap();
        page.write(50, b"xy").unwrap();
        assert_eq!((page.start(), page.len()), (50, 53));
    }

    #[test]
    fn write_hole_expands_end() {
        let mut page = mem_page(0);
        page.write(0, b"ab").unwrap();
        page.write(100, b"cd").unwrap();
        assert_eq!((page.start(), page.len()), (0, 102));
        assert_eq!(page.readable_len(0), Some(2));
        assert_eq!(page.readable_len(2), None);
        assert_eq!(page.readable_len(50), None);
        assert_eq!(page.readable_len(100), Some(2));
        assert!(!page.is_full(102));
    }

    #[test]
    fn adjacent_writes_form_full_valid_range() {
        let mut page = mem_page(0);
        page.write(2, b"cd").unwrap();
        page.write(0, b"ab").unwrap();
        assert_eq!(page.readable_len(0), Some(4));
        assert!(page.is_full(4));
    }

    #[test]
    fn mark_dirty_keeps_disjoint_ranges() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 200);
        page.mark_dirty(300, 400);
        assert_eq!(page.dirty_ranges, vec![(100, 200), (300, 400)]);
    }

    #[test]
    fn mark_dirty_out_of_order_insert_stays_sorted() {
        let mut page = mem_page(0);
        page.mark_dirty(300, 400);
        page.mark_dirty(100, 200);
        assert_eq!(page.dirty_ranges, vec![(100, 200), (300, 400)]);
    }

    #[test]
    fn mark_dirty_merges_overlapping_ranges() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 200);
        page.mark_dirty(150, 300);
        assert_eq!(page.dirty_ranges, vec![(100, 300)]);
    }

    #[test]
    fn mark_dirty_merges_touching_ranges() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 200);
        page.mark_dirty(200, 300);
        assert_eq!(page.dirty_ranges, vec![(100, 300)]);
    }

    #[test]
    fn mark_dirty_absorbs_contained_range() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 500);
        page.mark_dirty(200, 300);
        assert_eq!(page.dirty_ranges, vec![(100, 500)]);
    }

    #[test]
    fn mark_dirty_expands_enclosing_range() {
        let mut page = mem_page(0);
        page.mark_dirty(200, 300);
        page.mark_dirty(100, 500);
        assert_eq!(page.dirty_ranges, vec![(100, 500)]);
    }

    #[test]
    fn mark_dirty_merges_multiple_ranges() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 200);
        page.mark_dirty(400, 500);
        page.mark_dirty(150, 450);
        assert_eq!(page.dirty_ranges, vec![(100, 500)]);
    }

    #[test]
    fn mark_dirty_chain_of_touching_ranges() {
        let mut page = mem_page(0);
        page.mark_dirty(100, 200);
        page.mark_dirty(200, 300);
        page.mark_dirty(300, 400);
        assert_eq!(page.dirty_ranges, vec![(100, 400)]);
    }

    #[test]
    fn dirty_state_transitions() {
        let mut page = mem_page(0);
        assert!(!page.is_dirty());
        page.mark_dirty(0, 1);
        assert!(page.is_dirty());
    }

    #[test]
    fn take_dirty_partial_page_returns_none() {
        let mut page = mem_page(0);
        page.write(0, b"partial").unwrap();
        page.mark_dirty(0, 7);
        assert!(page.take_dirty(4096).unwrap().is_none());
        assert!(page.is_dirty());
    }

    #[test]
    fn take_all_dirty_partial_page_returns_page() {
        let mut page = mem_page(0);
        page.write(0, b"partial").unwrap();
        page.mark_dirty(0, 7);
        let taken = page.take_all_dirty(4096).unwrap().unwrap();
        assert_eq!(taken.len(), 7);
        assert!(!page.is_dirty());
        assert!(page.flushing);
        assert_eq!(&taken.read(0, 7).unwrap()[..], b"partial");
    }

    #[test]
    fn take_dirty_full_page_returns_page_and_clears_dirty() {
        let mut page = mem_page(0);
        let data: Vec<u8> = (0..4096u64).map(|i| (i % 251) as u8).collect();
        page.write(0, &data).unwrap();
        page.mark_dirty(0, 4096);
        let taken = page.take_dirty(4096).unwrap().unwrap();
        assert_eq!(taken.page_id, 0);
        assert_eq!(taken.len(), 4096);
        assert!(!page.is_dirty());
        assert!(page.flushing);
        assert!(page.take_dirty(4096).unwrap().is_none());
        assert_eq!(&taken.read(0, 4).unwrap()[..], &data[..4]);
    }

    #[test]
    fn finish_flush_keeps_writes_after_take_dirty() {
        let mut page = mem_page(0);
        page.write(0, &vec![1u8; 4096]).unwrap();
        page.mark_dirty(0, 4096);
        let _taken = page.take_dirty(4096).unwrap().unwrap();

        page.mark_dirty(100, 200);
        page.finish_flush();

        assert_eq!(page.dirty_ranges, vec![(100, 200)]);
        assert!(!page.flushing);
    }

    #[test]
    fn restore_dirty_merges_snapshot_after_flush_failure() {
        let mut page = mem_page(0);
        page.write(0, &vec![1u8; 4096]).unwrap();
        page.mark_dirty(0, 4096);
        let taken = page.take_dirty(4096).unwrap().unwrap();

        page.mark_dirty(100, 200);
        page.restore_dirty(taken.dirty_ranges());

        assert_eq!(page.dirty_ranges, vec![(0, 4096)]);
        assert!(!page.flushing);
    }

    #[test]
    fn snapshot_drop_does_not_change_gauge() {
        FuseMetrics::ensure_init().unwrap();
        let mut page = mem_page(0);
        page.write(0, &vec![1u8; 4096]).unwrap();
        page.mark_dirty(0, 4096);

        let taken = page.take_dirty(4096).unwrap().unwrap();
        let before = FuseMetrics::get().write_back_cache_page_num.get();
        drop(taken);
        assert_eq!(
            FuseMetrics::get().write_back_cache_page_num.get(),
            before,
            "snapshot drop must not undercount the gauge"
        );

        let base = before - 1;
        drop(page);
        assert_eq!(FuseMetrics::get().write_back_cache_page_num.get(), base);
    }

    #[test]
    fn take_dirty_clean_page_returns_none() {
        let mut page = mem_page(0);
        page.write(0, b"abc").unwrap();
        let _ = page.take_all_dirty(4096).unwrap().unwrap();
        page.finish_flush();
        assert!(page.take_dirty(4096).unwrap().is_none());
    }

    #[test]
    fn take_dirty_oversized_page_errors() {
        let mut page = mem_page(0);
        page.write(0, &vec![0u8; 8192]).unwrap();
        page.mark_dirty(0, 8192);
        assert!(page.take_dirty(4096).is_err());
    }

    #[test]
    fn clear_resets_state_and_returns_len() {
        let mut page = mem_page(0);
        page.write(0, b"hello").unwrap();
        page.mark_dirty(0, 5);
        assert_eq!(page.clear(), 5);
        assert_eq!(page.len(), 0);
        assert!(!page.is_dirty());
        assert_eq!(page.start(), 0);
    }
}
