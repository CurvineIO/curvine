use crate::fs::FuseWriter;
use crate::FuseResult;
use bytes::{Bytes, BytesMut};
use curvine_core_error::err_box;
use curvine_io::LocalFile;
use log::warn;
use std::{fs, mem};

pub(crate) enum StoreInner {
    Memory(BytesMut),
    Disk(LocalFile),
}

impl StoreInner {
    pub fn write(&mut self, off: u64, data: &[u8]) -> FuseResult<()> {
        match self {
            StoreInner::Memory(bytes) => {
                let off = off as usize;
                let end = off + data.len();
                if end > bytes.len() {
                    bytes.resize(end, 0);
                }
                bytes[off..end].copy_from_slice(data);
                Ok(())
            }

            StoreInner::Disk(file) => {
                file.seek(off as i64)?;
                file.write_all(data)?;
                Ok(())
            }
        }
    }

    pub fn read(&mut self, off: u64, len: usize) -> FuseResult<Bytes> {
        match self {
            StoreInner::Memory(bytes) => {
                let off = off as usize;
                let end = off + len;
                if end > bytes.len() {
                    return err_box!("out of range");
                }
                Ok(Bytes::copy_from_slice(&bytes[off..end]))
            }

            StoreInner::Disk(file) => {
                let bytes = file.read_full(Some(off as i64), len)?;
                Ok(bytes.freeze())
            }
        }
    }
}

pub struct Page {
    pub(crate) page_id: u64,
    pub(crate) store: StoreInner,
    pub(crate) start: u64,
    len: u64,
    dirty_ranges: Vec<(u64, u64)>,
}

impl Page {
    pub(crate) fn new(page_id: u64, store: StoreInner) -> Self {
        Page {
            page_id,
            store,
            start: 0,
            len: 0,
            dirty_ranges: Vec::new(),
        }
    }

    pub fn with_disk(page_id: u64, file: LocalFile) -> Self {
        Self::new(page_id, StoreInner::Disk(file))
    }

    pub fn with_memory(page_id: u64, bytes: BytesMut) -> Self {
        Self::new(page_id, StoreInner::Memory(bytes))
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn write(&mut self, off: u64, data: &[u8]) -> FuseResult<()> {
        self.store.write(off, data)?;
        let end = off + data.len() as u64;
        if self.len == 0 {
            self.start = off;
            self.len = data.len() as u64;
        } else {
            let old_end = self.start + self.len;
            self.start = self.start.min(off);
            self.len = old_end.max(end) - self.start;
        }
        Ok(())
    }

    pub fn mark_dirty(&mut self, off: u64, end: u64) {
        let mut off = off;
        let mut end = end;

        // First range whose end >= off (may overlap or touch).
        let pos = self.dirty_ranges.partition_point(|&(_, e)| e < off);

        // Absorb all consecutive overlapping/touching ranges from pos.
        let mut remove_end = pos;
        while remove_end < self.dirty_ranges.len() && self.dirty_ranges[remove_end].0 <= end {
            off = off.min(self.dirty_ranges[remove_end].0);
            end = end.max(self.dirty_ranges[remove_end].1);
            remove_end += 1;
        }

        self.dirty_ranges.splice(pos..remove_end, [(off, end)]);
    }

    pub fn is_dirty(&self) -> bool {
        !self.dirty_ranges.is_empty()
    }

    pub async fn flush_to(&mut self, writer: &FuseWriter) -> FuseResult<u64> {
        if self.dirty_ranges.is_empty() {
            return Ok(0);
        }

        const CHUNK_SIZE: usize = 128 * 1024;
        let mut total = 0;
        let dirty_ranges = mem::take(&mut self.dirty_ranges);
        for (off, end) in dirty_ranges {
            let mut cur = off;
            while cur < end {
                let len = std::cmp::min(CHUNK_SIZE, (end - cur) as usize);
                let data = self.store.read(cur, len)?;
                writer
                    .write(self.page_id as i64 + cur as i64, data, None)
                    .await?;
                cur += len as u64;
            }
            total += end - off;
        }
        Ok(total)
    }

    pub fn read(&mut self, off: u64, len: usize) -> FuseResult<Bytes> {
        self.store.read(off, len)
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.start + self.len
    }

    pub fn is_disk(&self) -> bool {
        matches!(self.store, StoreInner::Disk(_))
    }

    pub fn path(&self) -> Option<&str> {
        match &self.store {
            StoreInner::Disk(file) => Some(file.path()),
            StoreInner::Memory(_) => None,
        }
    }

    pub fn clear(&mut self) -> u64 {
        let len = self.len;
        match &mut self.store {
            StoreInner::Disk(file) => {
                if let Err(e) = fs::remove_file(file.path()) {
                    warn!("failed to remove file: {}", e);
                }
            }
            StoreInner::Memory(bytes) => bytes.clear(),
        }

        self.start = 0;
        self.len = 0;
        self.dirty_ranges.clear();
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
        if self.dirty_ranges.is_empty() || self.len == 0 {
            return Ok(None);
        }
        if self.len > page_size {
            return err_box!("page is too large");
        }
        if full_only && self.len < page_size {
            return Ok(None);
        }

        let store = match &self.store {
            StoreInner::Memory(bytes) => StoreInner::Memory(bytes.clone()),
            StoreInner::Disk(file) => {
                let file = LocalFile::with_read(file.path(), 0)?;
                StoreInner::Disk(file)
            }
        };
        let dirty_ranges = mem::take(&mut self.dirty_ranges);

        let page = Self {
            page_id: self.page_id,
            start: self.start,
            store,
            len: self.len,
            dirty_ranges,
        };
        Ok(Some(page))
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
        let mut taken = page.take_all_dirty(4096).unwrap().unwrap();
        assert_eq!(taken.len(), 7);
        assert!(!page.is_dirty());
        assert_eq!(&taken.read(0, 7).unwrap()[..], b"partial");
    }

    #[test]
    fn take_dirty_full_page_returns_page_and_clears_dirty() {
        let mut page = mem_page(0);
        let data: Vec<u8> = (0..4096u64).map(|i| (i % 251) as u8).collect();
        page.write(0, &data).unwrap();
        page.mark_dirty(0, 4096);
        let mut taken = page.take_dirty(4096).unwrap().unwrap();
        assert_eq!(taken.page_id, 0);
        assert_eq!(taken.len(), 4096);
        assert!(!page.is_dirty());
        assert_eq!(&taken.read(0, 4).unwrap()[..], &data[..4]);
    }

    #[test]
    fn take_dirty_clean_page_returns_none() {
        let mut page = mem_page(0);
        page.write(0, b"abc").unwrap();
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
