//! RocksDB-backed SPDK block metadata.
use crate::meta_store::BlockMetaStore;
use crate::BlockMeta;
/// Key: block_id (8B). Value: dir_id(4B) | offset(8B) | size(8B) | len(8B) | finalized(1B) | quarantined(1B) = 30B.
/// Legacy 29B records (no quarantined byte) decode with quarantined=false.
/// O(1) per block
use byteorder::{BigEndian, ByteOrder};
use curvine_core_error::{err_box, CommonResult};
use curvine_rocksdb::{DBConf, DBEngine};
use log::{info, warn};
const CF_SPDK_BLOCKS: &str = "spdk_blocks";
const VALUE_SIZE: usize = 30;
/// Pre-quarantine record size (no quarantined byte); must stay decodable.
const LEGACY_VALUE_SIZE: usize = 29;
pub struct SpdkMetaStore {
    db: DBEngine,
}
#[derive(Debug, Clone)]
pub struct SpdkBlockRecord {
    pub block_id: i64,
    pub dir_id: u32,
    pub offset: i64,
    pub size: i64,
    pub len: i64,
    pub finalized: bool,
    /// Extent must stay reserved on restart when true.
    pub quarantined: bool,
}
impl SpdkMetaStore {
    pub fn open(dir: &str, format: bool) -> CommonResult<Self> {
        let conf = DBConf::new(dir).add_cf(CF_SPDK_BLOCKS);
        let db = DBEngine::new(conf, format)?;
        info!("SpdkMetaStore opened at {}", dir);
        Ok(Self { db })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &self,
        block_id: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        finalized: bool,
        quarantined: bool,
    ) -> CommonResult<()> {
        let key = Self::encode_key(block_id);
        let value = Self::encode_value(dir_id, offset, size, len, finalized, quarantined);
        self.db.put_cf(CF_SPDK_BLOCKS, key, value)
    }
    pub fn delete(&self, block_id: i64) -> CommonResult<()> {
        let key = Self::encode_key(block_id);
        self.db.delete_cf(CF_SPDK_BLOCKS, key)
    }
    pub fn get(&self, block_id: i64) -> CommonResult<Option<SpdkBlockRecord>> {
        let key = Self::encode_key(block_id);
        match self.db.get_cf(CF_SPDK_BLOCKS, key)? {
            None => Ok(None),
            Some(v) => {
                let rec = Self::decode_value(block_id, &v)?;
                Ok(Some(rec))
            }
        }
    }
    pub fn scan_all(&self) -> CommonResult<Vec<SpdkBlockRecord>> {
        let iter = self.db.scan(CF_SPDK_BLOCKS)?;
        let mut records = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) = match item {
                Ok(kv) => kv,
                Err(e) => return err_box!("RocksDB scan error: {}", e),
            };
            if key_bytes.len() < 8 {
                warn!(
                    "SpdkMetaStore: skipping short key ({} bytes)",
                    key_bytes.len()
                );
                continue;
            }
            let block_id = BigEndian::read_i64(&key_bytes);
            match Self::decode_value(block_id, &val_bytes) {
                Ok(rec) => records.push(rec),
                Err(e) => {
                    warn!(
                        "SpdkMetaStore: skipping corrupt record for block {}: {}",
                        block_id, e
                    );
                }
            }
        }
        Ok(records)
    }
    #[inline]
    fn encode_key(block_id: i64) -> [u8; 8] {
        let mut buf = [0u8; 8];
        BigEndian::write_i64(&mut buf, block_id);
        buf
    }
    #[inline]
    fn encode_value(
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        finalized: bool,
        quarantined: bool,
    ) -> [u8; VALUE_SIZE] {
        let mut buf = [0u8; VALUE_SIZE];
        BigEndian::write_u32(&mut buf[0..4], dir_id);
        BigEndian::write_i64(&mut buf[4..12], offset);
        BigEndian::write_i64(&mut buf[12..20], size);
        BigEndian::write_i64(&mut buf[20..28], len);
        buf[28] = if finalized { 1 } else { 0 };
        buf[29] = if quarantined { 1 } else { 0 };
        buf
    }
    #[inline]
    fn decode_value(block_id: i64, bytes: &[u8]) -> CommonResult<SpdkBlockRecord> {
        if bytes.len() < LEGACY_VALUE_SIZE {
            return err_box!(
                "SpdkMetaStore: value too short for block {} ({} < {})",
                block_id,
                bytes.len(),
                LEGACY_VALUE_SIZE
            );
        }
        // Missing in legacy 29B records -> defaults to not quarantined.
        let quarantined = bytes.get(VALUE_SIZE - 1).is_some_and(|b| *b != 0);
        Ok(SpdkBlockRecord {
            block_id,
            dir_id: BigEndian::read_u32(&bytes[0..4]),
            offset: BigEndian::read_i64(&bytes[4..12]),
            size: BigEndian::read_i64(&bytes[12..20]),
            len: BigEndian::read_i64(&bytes[20..28]),
            finalized: bytes[28] != 0,
            quarantined,
        })
    }

    /// Test-only: write a raw legacy 29B record (bypasses encode_value,
    /// which always emits 30B), for backward-compat decode tests.
    #[cfg(test)]
    pub(crate) fn put_legacy_for_test(
        &self,
        block_id: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        finalized: bool,
    ) -> CommonResult<()> {
        let mut buf = [0u8; LEGACY_VALUE_SIZE];
        BigEndian::write_u32(&mut buf[0..4], dir_id);
        BigEndian::write_i64(&mut buf[4..12], offset);
        BigEndian::write_i64(&mut buf[12..20], size);
        BigEndian::write_i64(&mut buf[20..28], len);
        buf[28] = if finalized { 1 } else { 0 };
        let key = Self::encode_key(block_id);
        self.db.put_cf(CF_SPDK_BLOCKS, key, buf)
    }
}

impl BlockMetaStore for SpdkMetaStore {
    fn put_block_meta(&self, meta: &BlockMeta) -> CommonResult<()> {
        self.put(
            meta.id(),
            meta.dir_id(),
            meta.bdev_offset,
            meta.actual_len,
            meta.len(),
            meta.is_final(),
            meta.is_quarantined(),
        )
    }

    fn remove_block_meta(&self, meta: &BlockMeta) -> CommonResult<()> {
        self.delete(meta.id())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    fn test_dir(name: &str) -> String {
        let d = format!("../testing/spdk_meta_{}", name);
        let _ = std::fs::remove_dir_all(&d);
        d
    }
    #[test]
    fn put_get_delete() {
        let store = SpdkMetaStore::open(&test_dir("pgd"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, true, false).unwrap();
        store.put(2, 1, 4096, 8192, 6000, false, false).unwrap();
        let r = store.get(1).unwrap().unwrap();
        assert_eq!(r.offset, 0);
        assert!(r.finalized);
        assert!(!r.quarantined);
        store.delete(1).unwrap();
        assert!(store.get(1).unwrap().is_none());
    }
    #[test]
    fn put_persists_quarantined() {
        let store = SpdkMetaStore::open(&test_dir("quarantined"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, false, true).unwrap();
        let r = store.get(1).unwrap().unwrap();
        assert!(!r.finalized);
        assert!(r.quarantined);
    }
    /// Regression: a legacy 29B record must still decode and show up in
    /// scan_all(), defaulting quarantined=false.
    #[test]
    fn decode_legacy_29_byte_record_defaults_quarantined_false() {
        let store = SpdkMetaStore::open(&test_dir("legacy29"), true).unwrap();
        store
            .put_legacy_for_test(42, 7, 4096, 8192, 8192, true)
            .unwrap();

        let rec = store.get(42).unwrap().expect("legacy record must decode");
        assert_eq!(rec.dir_id, 7);
        assert_eq!(rec.offset, 4096);
        assert_eq!(rec.size, 8192);
        assert_eq!(rec.len, 8192);
        assert!(rec.finalized);
        assert!(
            !rec.quarantined,
            "legacy record must default quarantined=false"
        );

        // scan_all() is the actual restart/upgrade path: it must not skip
        // the legacy record either.
        let all = store.scan_all().unwrap();
        assert_eq!(all.len(), 1, "scan_all() must not skip legacy records");
        assert!(!all[0].quarantined);
    }
    #[test]
    fn scan_all() {
        let store = SpdkMetaStore::open(&test_dir("scan"), true).unwrap();
        for i in 0..100 {
            store
                .put(i, (i % 3) as u32, i * 4096, 4096, 4096, i % 2 == 0, false)
                .unwrap();
        }
        assert_eq!(store.scan_all().unwrap().len(), 100);
    }
    #[test]
    fn dir_id_preserved() {
        let store = SpdkMetaStore::open(&test_dir("dir"), true).unwrap();
        store.put(1, 10, 0, 4096, 4096, true, false).unwrap();
        store.put(2, 20, 4096, 4096, 4096, false, false).unwrap();
        let records = store.scan_all().unwrap();
        assert_eq!(records.iter().filter(|r| r.dir_id == 10).count(), 1);
        assert_eq!(records.iter().filter(|r| r.dir_id == 20).count(), 1);
    }
    #[test]
    fn update_key() {
        let store = SpdkMetaStore::open(&test_dir("upd"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, false, false).unwrap();
        store.put(1, 1, 0, 4096, 2048, true, false).unwrap();
        assert_eq!(store.get(1).unwrap().unwrap().len, 2048);
    }
    #[test]
    fn reopen() {
        let dir = test_dir("reopen");
        {
            let s = SpdkMetaStore::open(&dir, true).unwrap();
            s.put(1, 5, 0, 4096, 4096, true, false).unwrap();
        }
        let s = SpdkMetaStore::open(&dir, false).unwrap();
        assert_eq!(s.scan_all().unwrap().len(), 1);
    }
}
