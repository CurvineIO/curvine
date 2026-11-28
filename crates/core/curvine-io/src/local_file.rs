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

use crate::{CacheManager, DataSlice, IOError, IOResult, ReadAheadTask};
#[cfg(target_os = "linux")]
use bytes::Bytes;
use bytes::BytesMut;
use curvine_sys::{self, RawIOSlice};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::{Metadata, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
#[cfg(target_os = "linux")]
use std::{future::Future, pin::Pin};

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

macro_rules! err_box {
    ($e:expr) => {{
        let message = format!(
            "[{}] ERROR: {}({}:{})",
            curvine_sys::thread_name(),
            $e,
            file!(),
            line!()
        );
        Err(IOError::create(message))
    }};
    ($f:tt, $($arg:expr),+) => {{
        err_box!(format!($f, $($arg),+))
    }};
}

macro_rules! try_err {
    ($expr:expr) => {{
        match $expr {
            Ok(result) => result,
            Err(error) => return err_box!(error),
        }
    }};
}

type PositionalReader = Box<dyn Fn(&mut [u8], u64) -> std::io::Result<()> + Send + Sync>;
#[cfg(target_os = "linux")]
type OwnedReadFuture = Pin<Box<dyn Future<Output = std::io::Result<Bytes>> + Send>>;
#[cfg(target_os = "linux")]
type OwnedPositionalReader = Box<dyn Fn(u64, usize) -> OwnedReadFuture + Send + Sync>;

enum ReadMode {
    Sequential,
    // Explicit-offset sendfile advances the logical cursor, not the descriptor.
    #[cfg(target_os = "linux")]
    Positional,
    Callback(PositionalReader),
    #[cfg(target_os = "linux")]
    Owned(OwnedPositionalReader),
}

pub struct LocalFile {
    inner: fs::File,
    path: String,
    len: i64,
    pos: i64,
    buf: BytesMut,
    read_mode: ReadMode,
}

impl LocalFile {
    pub fn new<T: AsRef<str>>(path: T, mut inner: fs::File) -> IOResult<Self> {
        let len = inner.metadata()?.len() as i64;
        let pos = inner.stream_position()? as i64;
        let file = Self {
            inner,
            path: path.as_ref().to_string(),
            len,
            pos,
            buf: BytesMut::new(),
            read_mode: ReadMode::Sequential,
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

    // Open a file for writing (also requests read so the same handle can read back).
    pub fn with_write<T: AsRef<str>>(path: T, overwrite: bool) -> IOResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(overwrite)
            .open(path.as_ref());

        let file = try_err!(file);
        Self::new(path.as_ref(), file)
    }

    pub fn with_write_offset<T: AsRef<str>>(
        path: T,
        overwrite: bool,
        offset: i64,
    ) -> IOResult<Self> {
        let mut local_file = Self::with_write(path, overwrite)?;

        if offset > 0 {
            // If offset is specified, seek to the specified position
            local_file.seek(offset)?;
        }

        Ok(local_file)
    }

    pub fn from_file(path: &str, inner: fs::File) -> IOResult<Self> {
        Self::new(path, inner)
    }

    /// Install buffered positional reads on this same open file (not a path
    /// reopened later). Descriptor operations still use the original handle.
    pub fn set_positional_reader<F>(&mut self, init: impl FnOnce(fs::File) -> F) -> IOResult<()>
    where
        F: Fn(&mut [u8], u64) -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.read_mode = ReadMode::Callback(Box::new(init(self.inner.try_clone()?)));
        Ok(())
    }

    /// Use the logical cursor for reads on a duplicated descriptor shared by snapshots.
    #[cfg(target_os = "linux")]
    pub fn use_positional_reads(&mut self) {
        self.read_mode = ReadMode::Positional;
    }

    /// The async reader owns a pinned descriptor and buffer through completion.
    /// Synchronous reads on this file use pread at the same logical cursor.
    #[cfg(target_os = "linux")]
    pub fn set_owned_positional_reader<A, F>(
        &mut self,
        init: impl FnOnce(fs::File) -> A,
    ) -> IOResult<()>
    where
        A: Fn(u64, usize) -> F + Send + Sync + 'static,
        F: Future<Output = std::io::Result<Bytes>> + Send + 'static,
    {
        let owned = init(self.inner.try_clone()?);
        self.read_mode = ReadMode::Owned(Box::new(move |offset, len| Box::pin(owned(offset, len))));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub fn has_owned_positional_reader(&self) -> bool {
        matches!(self.read_mode, ReadMode::Owned(_))
    }

    /// Advance only the logical file cursor, and only on a complete read.
    #[cfg(target_os = "linux")]
    pub async fn read_owned_region(&mut self, len: usize) -> IOResult<Bytes> {
        let ReadMode::Owned(read) = &self.read_mode else {
            return err_box!("no owned positional reader attached");
        };
        let end = i64::try_from(len)
            .ok()
            .and_then(|len| self.pos.checked_add(len))
            .filter(|end| self.pos >= 0 && *end <= self.len)
            .ok_or_else(|| IOError::from("owned read exceeds file length"))?;
        let data = read(self.pos as u64, len).await?;
        if data.len() != len {
            return err_box!("short owned read: expected {}, got {}", len, data.len());
        }
        self.pos = end;
        Ok(data)
    }

    fn advance_after_write(&mut self, n: i64) {
        self.pos += n;
        if self.pos > self.len {
            self.len = self.pos;
        }
    }

    pub fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let chunk = (len as i64).min(self.len - self.pos);
        if chunk <= 0 {
            return err_box!(
                "offset exceeds file length, length={}, offset={}",
                self.len,
                self.pos
            );
        }

        #[cfg(not(target_os = "linux"))]
        let region = DataSlice::Buffer(self.read_full(Some(self.pos), chunk as usize)?);

        #[cfg(target_os = "linux")]
        let region = if enable_send_file {
            let region = DataSlice::IOSlice(RawIOSlice::new(
                curvine_sys::get_raw_io(self)?,
                Some(self.pos),
                chunk as usize,
            ));
            // sendfile consumes the explicit RawIOSlice offset later, so keep
            // the logical device position in sync here.
            if matches!(self.read_mode, ReadMode::Sequential) {
                self.read_mode = ReadMode::Positional;
            }
            self.pos += chunk;
            region
        } else {
            DataSlice::Buffer(self.read_full(Some(self.pos), chunk as usize)?)
        };

        // Buffered reads advance through read_full/read_all. Advancing again
        // here would skip one chunk on every subsequent read.
        Ok(region)
    }

    pub fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        match region {
            DataSlice::Empty => Ok(()),
            DataSlice::IOSlice(_) => err_box!("Not support"),
            _ => self.write_all(region.as_slice()),
        }
    }

    fn align_for_write(&mut self) -> std::io::Result<()> {
        // Explicit-offset reads don't advance the descriptor used by Write.
        if !matches!(self.read_mode, ReadMode::Sequential) {
            self.inner.seek(SeekFrom::Start(self.pos as u64))?;
        }
        Ok(())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        self.align_for_write()?;
        try_err!(self.inner.write_all(buf));
        self.advance_after_write(buf.len() as i64);
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        try_err!(self.inner.flush());
        Ok(())
    }

    /// Push written data to stable storage.
    pub fn sync_data(&mut self) -> IOResult<()> {
        try_err!(self.inner.sync_data());
        Ok(())
    }

    pub fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        match &self.read_mode {
            ReadMode::Sequential => try_err!(self.inner.read_exact(buf)),
            ReadMode::Callback(read) => read(buf, self.pos as u64)?,
            #[cfg(target_os = "linux")]
            ReadMode::Owned(_) | ReadMode::Positional => {
                self.inner.read_exact_at(buf, self.pos as u64)?
            }
        }
        self.pos += buf.len() as i64;
        Ok(())
    }

    pub fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let len = if matches!(self.read_mode, ReadMode::Sequential) {
            try_err!(self.inner.read(buf))
        } else {
            #[cfg(unix)]
            {
                self.inner.read_at(buf, self.pos as u64)?
            }
            #[cfg(not(unix))]
            {
                self.inner.seek(SeekFrom::Start(self.pos as u64))?;
                try_err!(self.inner.read(buf))
            }
        };
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

    pub fn pread(&self, buf: &mut [u8], offset: u64) -> IOResult<usize> {
        #[cfg(unix)]
        {
            let n = try_err!(self.inner.read_at(buf, offset));
            Ok(n)
        }
        #[cfg(not(unix))]
        {
            let _ = (buf, offset);
            err_box!("pread is not supported on this platform")
        }
    }

    pub fn pread_exact(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
        #[cfg(unix)]
        {
            try_err!(self.inner.read_exact_at(buf, offset));
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (buf, offset);
            err_box!("pread is not supported on this platform")
        }
    }

    pub fn pread_full(&self, offset: u64, len: usize) -> IOResult<BytesMut> {
        let mut buf = BytesMut::with_capacity(len);
        unsafe { buf.set_len(len) }
        self.pread_exact(&mut buf, offset)?;
        Ok(buf)
    }

    pub fn pwrite(&self, buf: &[u8], offset: u64) -> IOResult<usize> {
        #[cfg(unix)]
        {
            let n = try_err!(self.inner.write_at(buf, offset));
            Ok(n)
        }
        #[cfg(not(unix))]
        {
            let _ = (buf, offset);
            err_box!("pwrite is not supported on this platform")
        }
    }

    pub fn pwrite_all(&self, buf: &[u8], offset: u64) -> IOResult<()> {
        #[cfg(unix)]
        {
            try_err!(self.inner.write_all_at(buf, offset));
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (buf, offset);
            err_box!("pwrite is not supported on this platform")
        }
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

    pub fn read_ahead(
        &mut self,
        os_cache: &CacheManager,
        last_task: Option<ReadAheadTask>,
    ) -> Option<ReadAheadTask> {
        os_cache.read_ahead(&self.inner, self.pos, self.len, last_task)
    }

    pub fn read_full(&mut self, off: Option<i64>, len: usize) -> IOResult<BytesMut> {
        if let Some(v) = off {
            self.seek(v)?;
        }

        self.buf.reserve(len);
        unsafe { self.buf.set_len(len) }
        let mut buf = self.buf.split();

        self.read_all(&mut buf)?;

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

    pub fn resize(&mut self, truncate: bool, off: i64, len: i64, mode: i32) -> IOResult<()> {
        if truncate {
            curvine_sys::ftruncate(&self.inner, len)?;
        } else {
            curvine_sys::fallocate(&self.inner, off, len, mode)?;
        }

        self.len = self.inner.metadata()?.len() as i64;
        Ok(())
    }

    pub fn actual_size(&self) -> IOResult<u64> {
        let meta = self.inner.metadata()?;
        curvine_sys::file_actual_size(meta).map_err(Into::into)
    }

    pub fn metadata(&self) -> IOResult<Metadata> {
        Ok(self.inner.metadata()?)
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
        self.align_for_write()?;
        let len = self.inner.write(buf)?;
        self.advance_after_write(len as i64);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::LocalFile;
    use curvine_runtime::common::Utils;
    use std::fs::remove_file;

    fn test_path() -> String {
        let path = Utils::test_file();
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        path
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn owned_and_synchronous_reads_share_the_logical_cursor() {
        use std::os::unix::fs::FileExt;

        let path = test_path();
        std::fs::write(&path, b"abcdefghijk").unwrap();
        let mut file = LocalFile::with_read(&path, 1).unwrap();
        remove_file(&path).unwrap(); // All modes must use the pinned file.
        file.set_owned_positional_reader(|fd| {
            move |off, len| {
                let mut buf = vec![0; len];
                std::future::ready(
                    fd.read_exact_at(&mut buf, off)
                        .map(|()| bytes::Bytes::from(buf)),
                )
            }
        })
        .unwrap();

        assert_eq!(&file.read_owned_region(2).await.unwrap()[..], b"bc");
        let mut buf = [0; 2];
        assert_eq!(file.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"de");
        file.read_all(&mut buf).unwrap();
        assert_eq!(&buf, b"fg");
        file.seek(2).unwrap();
        assert_eq!(file.read_region(false, 2).unwrap().as_slice(), b"cd");
        assert_eq!(&file.read_owned_region(2).await.unwrap()[..], b"ef");
        file.seek(10).unwrap();
        assert!(file.read_all(&mut buf).is_err());
        assert_eq!(file.pos(), 10);
        assert_eq!(file.read(&mut buf).unwrap(), 1);
        assert_eq!(buf[0], b'k');
        assert_eq!(file.read(&mut buf).unwrap(), 0);
        assert_eq!(file.pos(), 11);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sendfile_then_buffered_read_without_a_ring() {
        let path = test_path();
        std::fs::write(&path, b"abcdef").unwrap();
        let mut file = LocalFile::with_read(&path, 1).unwrap();
        remove_file(&path).unwrap();
        assert!(matches!(
            file.read_region(true, 2).unwrap(),
            super::DataSlice::IOSlice(_)
        ));
        assert_eq!(file.read_region(false, 2).unwrap().as_slice(), b"de");
        let mut last = [0; 2];
        assert_eq!(file.read(&mut last).unwrap(), 1);
        assert_eq!(last[0], b'f');
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn write_after_positional_read_uses_logical_cursor() {
        let path = test_path();
        std::fs::write(&path, b"abcdef").unwrap();
        let mut file = LocalFile::with_write(&path, false).unwrap();
        remove_file(path).unwrap();
        file.use_positional_reads();
        file.read_all(&mut [0; 2]).unwrap();
        file.write_region(&super::DataSlice::from("X")).unwrap();
        file.read_all(&mut [0; 1]).unwrap();
        assert_eq!(std::io::Write::write(&mut file, b"Y").unwrap(), 1);
        assert_eq!(&file.pread_full(0, 6).unwrap()[..], b"abXdYf");
    }

    #[test]
    fn pread_pwrite_do_not_move_cursor() {
        let path = test_path();
        let file = LocalFile::with_write(&path, true).unwrap();

        file.pwrite_all(b"hello world", 0).unwrap();
        assert_eq!(file.pos(), 0);
        assert_eq!(file.len(), 0, "pwrite must not update cached len");

        let mut buf = [0u8; 5];
        assert_eq!(file.pread(&mut buf, 6).unwrap(), 5);
        assert_eq!(&buf, b"world");
        assert_eq!(file.pos(), 0);

        file.pwrite_all(b"RUST", 6).unwrap();
        assert_eq!(file.pos(), 0);

        let got = file.pread_full(0, 11).unwrap();
        assert_eq!(&got[..], b"hello RUSTd");

        remove_file(&path).unwrap();
    }

    #[test]
    fn pwrite_does_not_update_cached_len() {
        let path = test_path();
        let mut file = LocalFile::with_write(&path, true).unwrap();
        file.write_all(b"abc").unwrap();
        assert_eq!(file.pos(), 3);
        assert_eq!(file.len(), 3);

        file.pwrite_all(b"xyz", 10).unwrap();
        assert_eq!(file.pos(), 3, "pwrite must not move the cursor");
        assert_eq!(file.len(), 3, "pwrite must not update cached len");

        let got = file.pread_full(10, 3).unwrap();
        assert_eq!(&got[..], b"xyz");

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_exact_fails_at_eof() {
        let path = test_path();
        let file = LocalFile::with_write(&path, true).unwrap();
        file.pwrite_all(b"abc", 0).unwrap();

        let mut buf = [0u8; 4];
        assert!(file.pread_exact(&mut buf, 0).is_err());

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_pwrite_roundtrip_random_offsets() {
        let path = test_path();
        let file = LocalFile::with_write(&path, true).unwrap();

        file.pwrite_all(&[1u8; 4096], 0).unwrap();
        file.pwrite_all(&[2u8; 4096], 4096).unwrap();
        file.pwrite_all(&[3u8; 100], 8192).unwrap();

        let a = file.pread_full(0, 4096).unwrap();
        let b = file.pread_full(4096, 4096).unwrap();
        let c = file.pread_full(8192, 100).unwrap();
        assert!(a.iter().all(|&x| x == 1));
        assert!(b.iter().all(|&x| x == 2));
        assert!(c.iter().all(|&x| x == 3));
        assert_eq!(file.len(), 0, "pwrite must not update cached len");
        assert_eq!(file.pos(), 0);

        remove_file(&path).unwrap();
    }
}
