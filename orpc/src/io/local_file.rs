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

use crate::io::IOResult;
use crate::sys::{self, CacheManager, DataSlice, ReadAheadTask};
use crate::{err_box, try_err};
use bytes::BytesMut;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

pub struct LocalFile {
    inner: fs::File,
    path: String,
    is_tmpfs: bool,
    len: i64,
    pos: i64,
    buf: BytesMut,
}

impl LocalFile {
    pub fn new<T: AsRef<str>>(path: T, mut inner: fs::File) -> IOResult<Self> {
        let is_tmpfs = sys::is_tmpfs(path.as_ref())?;

        let len = inner.metadata()?.len() as i64;
        let pos = inner.stream_position()? as i64;
        let file = Self {
            inner,
            path: path.as_ref().to_string(),
            is_tmpfs,
            len,
            pos,
            buf: BytesMut::new(),
        };

        Ok(file)
    }

    // Append write.
    pub fn with_append<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = OpenOptions::new().append(true).open(path.as_ref())?;

        Self::new(path.as_ref(), file)
    }

    // Open a file to read the file.
    pub fn with_read<T: AsRef<str>>(path: T, off: u64) -> IOResult<Self> {
        let mut file = OpenOptions::new().read(true).open(path.as_ref())?;

        file.seek(SeekFrom::Start(off))?;
        Self::new(path.as_ref(), file)
    }

    // Open a file for writing.
    pub fn with_write<T: AsRef<str>>(path: T, overwrite: bool) -> IOResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(overwrite)
            .open(path.as_ref());

        let file = try_err!(file);
        Self::new(path.as_ref(), file)
    }

    pub fn from_file(path: &str, inner: fs::File) -> IOResult<Self> {
        Self::new(path, inner)
    }

    pub fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let chunk = (len as i64).min(self.len - self.pos);
        if chunk <= 0 {
            let err_msg = format!(
                "offset exceeds file length, length={}, offset={}",
                self.len, self.pos
            );
            return Err(err_msg.as_str().into());
        }

        let region = DataSlice::from_file(self, enable_send_file, Some(self.pos), chunk as i32)?;

        self.pos += chunk;
        Ok(region)
    }

    pub fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        let len: usize = match region {
            DataSlice::Empty => 0,

            DataSlice::Buffer(bytes) => {
                self.inner.write_all(bytes)?;
                bytes.len()
            }

            DataSlice::IOSlice(_) => return err_box!("Not support"),

            DataSlice::MemSlice(bytes) => {
                self.inner.write_all(bytes.as_slice())?;
                bytes.len()
            }

            DataSlice::Bytes(bytes) => {
                self.inner.write_all(bytes)?;
                bytes.len()
            }
        };

        self.pos += len as i64;
        Ok(())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        try_err!(self.inner.write_all(buf));
        self.pos += buf.len() as i64;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        try_err!(self.inner.flush());
        Ok(())
    }

    pub fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        try_err!(self.inner.read_exact(buf));
        self.pos += buf.len() as i64;
        Ok(())
    }

    pub fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let len = try_err!(self.inner.read(buf));
        self.pos += len as i64;
        Ok(len)
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn seek(&mut self, pos: i64) -> IOResult<i64> {
        if pos == self.pos {
            return Ok(pos);
        }

        let m = try_err!(self.inner.seek(SeekFrom::Start(pos as u64)));
        if m != pos as u64 {
            return err_box!("seek failed, expected {}, actual {}", pos, m);
        }

        self.pos = pos;
        Ok(self.pos)
    }

    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    pub fn is_tmpfs(&self) -> bool {
        self.is_tmpfs
    }

    pub fn read_ahead(
        &mut self,
        os_cache: &CacheManager,
        last_task: Option<ReadAheadTask>,
    ) -> Option<ReadAheadTask> {
        // Memory files do not use pre-reading.
        if self.is_tmpfs {
            None
        } else {
            os_cache.read_ahead(&self.inner, self.pos, self.len, last_task)
        }
    }

    pub fn read_full(&mut self, off: Option<i64>, len: usize) -> IOResult<BytesMut> {
        if let Some(v) = off {
            self.inner.seek(SeekFrom::Start(v as u64))?;
        }

        self.buf.reserve(len);
        unsafe { self.buf.set_len(len) }
        let mut buf = self.buf.split();

        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    // Write a string to the file and close the file immediately after the write is completed.
    pub fn write_string<P: AsRef<Path>>(path: P, value: &str, append: bool) -> IOResult<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(append)
            .truncate(!append)
            .open(path)?;

        try_err! {
            file.write_all(value.as_bytes())
        }
        Ok(())
    }

    pub fn read_string<P: AsRef<Path>>(path: P) -> IOResult<String> {
        let res = try_err!(fs::read_to_string(path));
        Ok(res)
    }

    pub fn read_toml<T: DeserializeOwned>(path: &Path) -> IOResult<Option<T>> {
        if !path.exists() {
            return Ok(None);
        }

        let str = Self::read_string(path)?;
        let r: T = toml::from_str(&str)?;
        Ok(Some(r))
    }

    pub fn write_toml<T: Serialize>(path: &Path, value: &T) -> IOResult<()> {
        let str = try_err!(toml::ser::to_string(value));
        Self::write_string(path, &str, false)?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for LocalFile {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl Display for LocalFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl Write for LocalFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
