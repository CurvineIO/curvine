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

use byteorder::{BigEndian, ByteOrder};
use orpc::{err_box, CommonResult};
use prost::bytes::BufMut;

// A utility class that converts some types to bytes.
pub struct RocksUtils;

impl RocksUtils {
    // Returns i64 type big-endian (network) byte order
    pub fn i64_to_bytes(val: i64) -> [u8; 8] {
        let mut key = [0; 8];
        BigEndian::write_i64(&mut key, val);
        key
    }

    // Returns the byte order of u32 type big endian (network)
    pub fn u32_to_bytes(val: u32) -> [u8; 4] {
        let mut key = [0; 4];
        BigEndian::write_u32(&mut key, val);
        key
    }

    pub fn i64_from_bytes(bytes: &[u8]) -> CommonResult<i64> {
        if bytes.len() < 8 {
            err_box!("Byte array has less than 8 bytes")
        } else {
            Ok(BigEndian::read_i64(bytes))
        }
    }

    pub fn u64_to_bytes(val: u64) -> [u8; 8] {
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, val);
        key
    }

    pub fn u64_from_bytes(bytes: &[u8]) -> CommonResult<u64> {
        if bytes.len() < 8 {
            err_box!("Byte array has less than 8 bytes")
        } else {
            Ok(BigEndian::read_u64(bytes))
        }
    }

    pub fn i32_to_bytes(val: i32) -> [u8; 4] {
        let mut key = [0; 4];
        BigEndian::write_i32(&mut key, val);
        key
    }

    pub fn i32_from_bytes(bytes: &[u8]) -> CommonResult<i32> {
        if bytes.len() < 4 {
            err_box!("Byte array has less than 4 bytes")
        } else {
            Ok(BigEndian::read_i32(bytes))
        }
    }

    pub fn prefix_to_bytes<P, K>(prefix: P, key: K) -> Vec<u8>
    where
        P: AsRef<[u8]>,
        K: AsRef<[u8]>,
    {
        let prefix = prefix.as_ref();
        let key = key.as_ref();

        let mut bytes = Vec::with_capacity(prefix.len() + key.len());
        bytes.extend_from_slice(prefix);
        bytes.extend_from_slice(key);
        bytes
    }

    pub fn i64_str_to_bytes(prefix: i64, key: &str) -> Vec<u8> {
        let key_bytes = key.as_bytes();
        let mut v = Vec::with_capacity(8 + key_bytes.len());
        v.extend_from_slice(&Self::i64_to_bytes(prefix));
        v.extend_from_slice(key_bytes);
        v
    }

    pub fn i64_str_from_bytes(bytes: &[u8]) -> CommonResult<(i64, String)> {
        if bytes.len() < 8 {
            err_box!("Byte array has less than 4 bytes")
        } else {
            let id = BigEndian::read_i64(&bytes[0..8]);
            let str = String::from_utf8_lossy(&bytes[8..]).to_string();
            Ok((id, str))
        }
    }

    pub fn u32_i64_to_bytes(prefix: u32, key: i64) -> [u8; 12] {
        let mut v = [0; 12];
        BigEndian::write_u32(&mut v[..4], prefix);
        BigEndian::write_i64(&mut v[4..12], key);
        v
    }

    pub fn i64_u32_to_bytes(prefix: i64, key: u32) -> [u8; 12] {
        let mut v = [0; 12];
        BigEndian::write_i64(&mut v[..8], prefix);
        BigEndian::write_u32(&mut v[8..12], key);
        v
    }

    pub fn i64_u32_from_bytes(bytes: &[u8]) -> CommonResult<(i64, u32)> {
        if bytes.len() < 12 {
            err_box!("Byte array has less than 4 bytes")
        } else {
            let id1 = BigEndian::read_i64(&bytes[0..8]);
            let id2 = BigEndian::read_u32(&bytes[8..12]);
            Ok((id1, id2))
        }
    }

    pub fn u64_u64_to_bytes(id1: u64, id2: u64) -> [u8; 16] {
        let mut v = [0; 16];
        BigEndian::write_u64(&mut v[..8], id1);
        BigEndian::write_u64(&mut v[8..16], id2);
        v
    }

    pub fn u64_u64_from_bytes(bytes: &[u8]) -> CommonResult<(u64, u64)> {
        if bytes.len() < 16 {
            err_box!("Byte array has less than 16 bytes")
        } else {
            let id1 = BigEndian::read_u64(&bytes[0..8]);
            let id2 = BigEndian::read_u64(&bytes[8..16]);
            Ok((id1, id2))
        }
    }

    // Calculate the key of the end position prefixed with start.
    pub fn calculate_end_bytes(start: &[u8]) -> Vec<u8> {
        let mut offset = start.len();
        while offset > 0 {
            if start[offset - 1] != 0xff {
                break;
            }
            offset -= 1;
        }

        // prefix is 0xff in every position.
        if offset == 0 {
            return vec![];
        }

        let mut end: Vec<u8> = Vec::with_capacity(offset);
        end.put_slice(&start[0..offset]);

        let last_pos = end.len() - 1;
        end[last_pos] += 1;

        end
    }
}

#[cfg(test)]
mod tests {
    use crate::rocksdb::RocksUtils;

    const CHECK: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x12];

    #[test]
    fn test_i64() {
        let val = 10002;
        let bytes = RocksUtils::i64_to_bytes(val);
        let de_val = RocksUtils::i64_from_bytes(&bytes).unwrap();

        assert_eq!(CHECK, bytes);
        assert_eq!(val, de_val)
    }

    #[test]
    fn test_u64() {
        let val = 10002;
        let bytes = RocksUtils::u64_to_bytes(val);
        let de_val = RocksUtils::u64_from_bytes(&bytes).unwrap();

        assert_eq!(CHECK, bytes);
        assert_eq!(val, de_val)
    }

    #[test]
    fn test_i32() {
        let val = 10002;
        let bytes = RocksUtils::i32_to_bytes(val);
        let de_val = RocksUtils::i32_from_bytes(&bytes).unwrap();

        assert_eq!(CHECK[4..8], bytes);
        assert_eq!(val, de_val)
    }

    #[test]
    fn test_prefix() {
        let id1 = 1001;
        let id2 = 1002;
        let bytes = RocksUtils::i64_u32_to_bytes(id1, id2);
        let (c_id1, c_id2) = RocksUtils::i64_u32_from_bytes(&bytes).unwrap();
        assert_eq!(id1, c_id1);
        assert_eq!(id2, c_id2);

        let id = 1003;
        let name = "rust";
        let bytes = RocksUtils::i64_str_to_bytes(id, name);
        let (c_id, c_name) = RocksUtils::i64_str_from_bytes(&bytes).unwrap();
        assert_eq!(id, c_id);
        assert_eq!(name, c_name);
    }
}
