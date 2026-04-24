//! RocksDB-backed SPDK block metadata.
/// Key: block_id (8B). Value: dir_id(4B) | offset(8B) | size(8B) | len(8B) | finalized(1B) = 29B.
/// O(1) per block
use byteorder::{BigEndian, ByteOrder};
use curvine_common::rocksdb::{DBConf, DBEngine};
use log::{info, warn};
use orpc::{err_box, CommonResult};
const CF_SPDK_BLOCKS: &str = "spdk_blocks";
const VALUE_SIZE: usize = 29;
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
}
impl SpdkMetaStore {
    pub fn open(dir: &str, format: bool) -> CommonResult<Self> {
        let conf = DBConf::new(dir).add_cf(CF_SPDK_BLOCKS);
        let db = DBEngine::new(conf, format)?;
        info!("SpdkMetaStore opened at {}", dir);
        Ok(Self { db })
    }
    pub fn put(
        &self,
        block_id: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        finalized: bool,
    ) -> CommonResult<()> {
        let key = Self::encode_key(block_id);
        let value = Self::encode_value(dir_id, offset, size, len, finalized);
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
    ) -> [u8; VALUE_SIZE] {
        let mut buf = [0u8; VALUE_SIZE];
        BigEndian::write_u32(&mut buf[0..4], dir_id);
        BigEndian::write_i64(&mut buf[4..12], offset);
        BigEndian::write_i64(&mut buf[12..20], size);
        BigEndian::write_i64(&mut buf[20..28], len);
        buf[28] = if finalized { 1 } else { 0 };
        buf
    }
    #[inline]
    fn decode_value(block_id: i64, bytes: &[u8]) -> CommonResult<SpdkBlockRecord> {
        if bytes.len() < VALUE_SIZE {
            return err_box!(
                "SpdkMetaStore: value too short for block {} ({} < {})",
                block_id,
                bytes.len(),
                VALUE_SIZE
            );
        }
        Ok(SpdkBlockRecord {
            block_id,
            dir_id: BigEndian::read_u32(&bytes[0..4]),
            offset: BigEndian::read_i64(&bytes[4..12]),
            size: BigEndian::read_i64(&bytes[12..20]),
            len: BigEndian::read_i64(&bytes[20..28]),
            finalized: bytes[28] != 0,
        })
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
        store.put(1, 1, 0, 4096, 4096, true).unwrap();
        store.put(2, 1, 4096, 8192, 6000, false).unwrap();
        let r = store.get(1).unwrap().unwrap();
        assert_eq!(r.offset, 0);
        assert!(r.finalized);
        store.delete(1).unwrap();
        assert!(store.get(1).unwrap().is_none());
    }
    #[test]
    fn scan_all() {
        let store = SpdkMetaStore::open(&test_dir("scan"), true).unwrap();
        for i in 0..100 {
            store
                .put(i, (i % 3) as u32, i * 4096, 4096, 4096, i % 2 == 0)
                .unwrap();
        }
        assert_eq!(store.scan_all().unwrap().len(), 100);
    }
    #[test]
    fn dir_id_preserved() {
        let store = SpdkMetaStore::open(&test_dir("dir"), true).unwrap();
        store.put(1, 10, 0, 4096, 4096, true).unwrap();
        store.put(2, 20, 4096, 4096, 4096, false).unwrap();
        let records = store.scan_all().unwrap();
        assert_eq!(records.iter().filter(|r| r.dir_id == 10).count(), 1);
        assert_eq!(records.iter().filter(|r| r.dir_id == 20).count(), 1);
    }
    #[test]
    fn update_key() {
        let store = SpdkMetaStore::open(&test_dir("upd"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, false).unwrap();
        store.put(1, 1, 0, 4096, 2048, true).unwrap();
        assert_eq!(store.get(1).unwrap().unwrap().len, 2048);
    }
    #[test]
    fn reopen() {
        let dir = test_dir("reopen");
        {
            let s = SpdkMetaStore::open(&dir, true).unwrap();
            s.put(1, 5, 0, 4096, 4096, true).unwrap();
        }
        let s = SpdkMetaStore::open(&dir, false).unwrap();
        assert_eq!(s.scan_all().unwrap().len(), 1);
    }
}
