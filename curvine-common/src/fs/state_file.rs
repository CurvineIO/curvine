//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::utils::SerdeUtils;
use crate::FILE_BUFFER_SIZE;
use orpc::io::IOResult;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

pub struct StateWriter {
    path: String,
    inner: BufWriter<File>,
}

impl StateWriter {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::create(path.as_ref())?;
        let inner = BufWriter::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner
            .get_ref()
            .metadata()
            .map(|x| x.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn write_len(&mut self, len: u64) -> IOResult<()> {
        SerdeUtils::serialize_into(&mut self.inner, &len)?;
        Ok(())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        self.inner.write_all(buf)?;
        Ok(())
    }

    pub fn write_struct<T: Serialize>(&mut self, obj: &T) -> IOResult<()> {
        SerdeUtils::serialize_into(&mut self.inner, obj)?;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.inner.flush()?;
        Ok(())
    }
}

pub struct StateReader {
    path: String,
    inner: BufReader<File>,
}

impl StateReader {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::open(path.as_ref())?;
        let inner = BufReader::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner
            .get_ref()
            .metadata()
            .map(|x| x.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> IOResult<()> {
        self.inner.read_exact(buf)?;
        Ok(())
    }

    pub fn read_len(&mut self) -> IOResult<u64> {
        let len = SerdeUtils::deserialize_from(&mut self.inner)?;
        Ok(len)
    }

    pub fn read_struct<T: for<'de> Deserialize<'de>>(&mut self) -> IOResult<T> {
        let st = SerdeUtils::deserialize_from(&mut self.inner)?;
        Ok(st)
    }
}

#[cfg(test)]
mod tests {
    use super::{StateReader, StateWriter};
    use orpc::common::{FastHashMap, FastHashSet, Utils};
    use std::fs;

    #[test]
    fn test_write_read() {
        let test_path = Utils::test_file();
        let _ = fs::remove_file(&test_path);

        let test_len: u64 = 12345;
        let test_tuple = (100u64, "test".to_string());
        let mut test_map = FastHashMap::new();
        test_map.insert("key1".to_string(), 42i32);
        test_map.insert("key2".to_string(), 84i32);
        let mut test_set = FastHashSet::new();
        test_set.insert(1u64);
        test_set.insert(2u64);

        let mut writer = StateWriter::new(&test_path).unwrap();
        writer.write_len(test_len).unwrap();
        writer.write_struct(&test_tuple).unwrap();
        writer.write_struct(&test_map).unwrap();
        writer.write_struct(&test_set).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let mut reader = StateReader::new(&test_path).unwrap();
        assert_eq!(reader.read_len().unwrap(), test_len);
        let read_tuple: (u64, String) = reader.read_struct().unwrap();
        assert_eq!(read_tuple, test_tuple);
        let read_map: FastHashMap<String, i32> = reader.read_struct().unwrap();
        assert_eq!(read_map.len(), test_map.len());
        assert_eq!(read_map.get("key1"), test_map.get("key1"));
        assert_eq!(read_map.get("key2"), test_map.get("key2"));
        let read_set: FastHashSet<u64> = reader.read_struct().unwrap();
        assert_eq!(read_set.len(), 2);
        assert!(read_set.contains(&1));
        assert!(read_set.contains(&2));
    }
}
