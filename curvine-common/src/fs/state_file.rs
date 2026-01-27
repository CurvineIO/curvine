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

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use serde::{Deserialize, Serialize};
use orpc::io::IOResult;
use crate::utils::SerdeUtils;

pub struct StateWriter {
    path: String,
    inner: BufWriter<File>,
}

impl StateWriter {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::create(path.as_ref())?;
        let inner = BufWriter::with_capacity(128 * 1024, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner.get_ref().metadata()
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
        let inner = BufReader::with_capacity(128 * 1024, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner.get_ref().metadata()
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
        let len = SerdeUtils::deserialize_from(&self.inner)?;
        Ok(len)
    }

    pub fn read_struct<T: for<'de> Deserialize<'de>>(&mut self) -> IOResult<T> {
        let st = SerdeUtils::deserialize_from(&self.inner)?;
        Ok(st)
    }
}
