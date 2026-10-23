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

use crate::{AlignedBuf, CacheManager, DataSlice, IOError, IOResult, ReadAheadTask};
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

pub struct LocalFile {
    inner: fs::File,
    path: String,
    len: i64,
    pos: i64,
    buf: BytesMut,
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

    /// Open for direct (O_DIRECT) reading, bypassing the page cache.
    ///
    /// Returns `Ok(None)` when the open fails with `EINVAL`/`EOPNOTSUPP`
    /// (filesystem or kernel without O_DIRECT support) so the caller can fall
    /// back to buffered reads; other errors propagate. On non-Linux platforms
    /// O_DIRECT does not exist and this always returns `Ok(None)`.
    ///
    /// Reads on the returned handle must go through [`Self::pread_aligned`]
    /// with a 4096-aligned destination; unaligned buffered reads fail with
    /// `EINVAL` under O_DIRECT.
    pub fn with_read_direct<T: AsRef<str>>(path: T, off: u64) -> IOResult<Option<Self>> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(path.as_ref());
            let mut file = match file {
                Ok(f) => f,
                Err(e) => match e.raw_os_error() {
                    Some(code) if code == libc::EINVAL || code == libc::EOPNOTSUPP => {
                        return Ok(None)
                    }
                    _ => return err_box!(e),
                },
            };

            try_err!(file.seek(SeekFrom::Start(off)));
            Self::new(path.as_ref(), file).map(Some)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path.as_ref(), off);
            Ok(None)
        }
    }

    /// Positioned aligned read for O_DIRECT mode: reads the aligned region
    /// `[off & !0xFFF, align_up(off + len))` covering `[off, off + len)` into
    /// `buf` (`buf.len()` must be >= the aligned length) and returns the number
    /// of RAW bytes read. The raw count is relative to the aligned start, so
    /// it may exceed `len`; the caller crops
    /// `[off - aligned_start, off - aligned_start + len)`.
    ///
    /// Uses positioned reads, but updates `self.pos` to `off + len` — the
    /// *requested* logical length, even when the raw read comes up short at
    /// EOF — so sequential `pos` tracking stays coherent for the direct-read
    /// flow (which skips `read_ahead`).
    pub fn pread_aligned(&mut self, buf: &mut AlignedBuf, off: i64, len: usize) -> IOResult<usize> {
        #[cfg(unix)]
        {
            const ALIGN: i64 = crate::DIRECT_IO_ALIGN as i64;

            if off < 0 {
                return err_box!("pread_aligned: negative offset {}", off);
            }
            if len == 0 {
                return Ok(0);
            }

            let Ok(len_i64) = i64::try_from(len) else {
                return err_box!("pread_aligned: len {} exceeds i64 range", len);
            };
            let Some(end) = off
                .checked_add(len_i64)
                .and_then(|e| e.checked_add(ALIGN - 1))
            else {
                return err_box!("pread_aligned: off {} + len {} overflows i64", off, len);
            };

            let aligned_start = off & !(ALIGN - 1);
            let aligned_end = end & !(ALIGN - 1);
            let aligned_len = (aligned_end - aligned_start) as usize;
            if buf.len() < aligned_len {
                return err_box!(
                    "pread_aligned: buffer too small, need {} aligned bytes, have {}",
                    aligned_len,
                    buf.len()
                );
            }

            let dst = &mut buf.as_bytes_mut()[..aligned_len];
            let mut done = 0usize;
            while done < aligned_len {
                match self
                    .inner
                    .read_at(&mut dst[done..], aligned_start as u64 + done as u64)
                {
                    Ok(0) => break, // EOF: clip to a raw short read.
                    Ok(n) => done += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return err_box!(e),
                }
            }

            self.pos = off + len as i64;
            Ok(done)
        }

        #[cfg(not(unix))]
        {
            let _ = (buf, off, len);
            err_box!("pread_aligned is not supported on this platform")
        }
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

        self.advance_after_write(len as i64);
        Ok(())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        try_err!(self.inner.write_all(buf));
        self.advance_after_write(buf.len() as i64);
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
    use crate::{AlignedBuf, DIRECT_IO_ALIGN};
    use curvine_runtime::common::Utils;
    use std::fs::remove_file;

    fn test_path() -> String {
        let path = Utils::test_file();
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        path
    }

    /// Deterministic byte pattern: `pattern[i] = i % 251`.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn write_pattern_file(path: &str, len: usize) -> Vec<u8> {
        let data = pattern(len);
        std::fs::write(path, &data).unwrap();
        data
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

    #[test]
    fn pread_aligned_matches_buffered_read() {
        let path = test_path();
        let data = write_pattern_file(&path, 3 * 4096);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        let mut buf = AlignedBuf::alloc(2 * 4096);
        let n = file.pread_aligned(&mut buf, 0, 2 * 4096).unwrap();
        assert_eq!(n, 2 * 4096);
        assert_eq!(&buf.as_slice()[..n], &data[..2 * 4096]);
        assert_eq!(file.pos(), 2 * 4096, "pread_aligned must advance pos");

        // Second call continues sequentially when the caller tracks pos.
        let n = file.pread_aligned(&mut buf, file.pos(), 2 * 4096).unwrap();
        assert_eq!(&buf.as_slice()[..n], &data[2 * 4096..]);

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_aligned_unaligned_offset_crops_to_buffered_bytes() {
        let path = test_path();
        let data = write_pattern_file(&path, 4 * 4096);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        let (off, len) = (100i64, 5000usize);
        let mut buf = AlignedBuf::alloc(DIRECT_IO_ALIGN * 3);
        let raw = file.pread_aligned(&mut buf, off, len).unwrap();

        // Raw read spans the aligned region [0, 8192).
        assert_eq!(raw, 2 * DIRECT_IO_ALIGN);
        let delta = (off as usize) & (DIRECT_IO_ALIGN - 1);
        assert_eq!(
            &buf.as_slice()[delta..delta + len],
            &data[off as usize..off as usize + len]
        );
        assert_eq!(file.pos(), off + len as i64);

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_aligned_tail_short_read_clips_at_eof() {
        let path = test_path();
        let data = write_pattern_file(&path, 6000);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        // Request [4096, 8192) but EOF is at 6000.
        let mut buf = AlignedBuf::alloc(2 * DIRECT_IO_ALIGN);
        let raw = file.pread_aligned(&mut buf, 4096, 4096).unwrap();
        assert_eq!(raw, 6000 - 4096);
        assert_eq!(&buf.as_slice()[..raw], &data[4096..]);
        // pos still advances by the requested logical length.
        assert_eq!(file.pos(), 8192);

        // Unaligned tail: request [4500, 8596), aligned region [4096, 12288).
        let raw = file.pread_aligned(&mut buf, 4500, 4096).unwrap();
        assert_eq!(raw, 6000 - 4096);
        let delta = 4500 - 4096;
        assert_eq!(&buf.as_slice()[delta..raw], &data[4500..]);

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_aligned_rejects_too_small_buffer() {
        let path = test_path();
        write_pattern_file(&path, 8192);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        let mut buf = AlignedBuf::alloc(DIRECT_IO_ALIGN);
        // len=5000 expands to the aligned region [0, 8192) which exceeds buf.
        assert!(file.pread_aligned(&mut buf, 0, 5000).is_err());

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_aligned_rejects_overflowing_region() {
        let path = test_path();
        write_pattern_file(&path, 4096);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        let mut buf = AlignedBuf::alloc(DIRECT_IO_ALIGN);
        assert!(file.pread_aligned(&mut buf, i64::MAX - 10, 4096).is_err());
        assert!(file.pread_aligned(&mut buf, -1, 4096).is_err());

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_aligned_zero_len_is_noop() {
        let path = test_path();
        write_pattern_file(&path, 4096);

        let mut file = LocalFile::with_read(&path, 123).unwrap();
        let mut buf = AlignedBuf::alloc(DIRECT_IO_ALIGN);
        assert_eq!(file.pread_aligned(&mut buf, 512, 0).unwrap(), 0);
        assert_eq!(file.pos(), 123, "zero-length read must not move pos");

        remove_file(&path).unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn with_read_direct_falls_back_on_non_linux() {
        let path = test_path();
        write_pattern_file(&path, 4096);

        match LocalFile::with_read_direct(&path, 0) {
            Ok(None) => (),
            Ok(Some(_)) => panic!("non-Linux O_DIRECT must report unsupported"),
            Err(e) => panic!("non-Linux O_DIRECT must not error, got {e}"),
        }

        remove_file(&path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn with_read_direct_reads_match_buffered_reads() {
        let path = test_path();
        let data = write_pattern_file(&path, 4 * 4096 + 700);

        let mut direct = match LocalFile::with_read_direct(&path, 0) {
            Ok(Some(f)) => f,
            Ok(None) => {
                // `Ok(None)` is a supported capability-detection result: the
                // filesystem holding the test file (e.g. tmpfs) has no
                // O_DIRECT. The buffered fallback is covered elsewhere; skip
                // the direct-I/O assertions.
                eprintln!("skipping: filesystem does not support O_DIRECT");
                remove_file(&path).unwrap();
                return;
            }
            Err(e) => panic!("unexpected open error: {e}"),
        };
        assert_eq!(direct.pos(), 0);

        // Aligned read.
        let mut buf = AlignedBuf::alloc(2 * DIRECT_IO_ALIGN);
        let n = direct.pread_aligned(&mut buf, 0, 2 * 4096).unwrap();
        assert_eq!(n, 2 * 4096);
        assert_eq!(&buf.as_slice()[..n], &data[..2 * 4096]);

        // Unaligned read, cropped to the buffered equivalent.
        let (off, len) = (100i64, 5000usize);
        let raw = direct.pread_aligned(&mut buf, off, len).unwrap();
        let delta = (off as usize) & (DIRECT_IO_ALIGN - 1);
        assert_eq!(
            &buf.as_slice()[delta..delta + len],
            &data[off as usize..off as usize + len]
        );
        assert!(raw >= len);
        assert_eq!(direct.pos(), off + len as i64);

        // Non-aligned tail crossing EOF.
        let off = 4 * 4096i64 - 100;
        let len = 4096usize;
        let raw = direct.pread_aligned(&mut buf, off, len).unwrap();
        let delta = (off as usize) & (DIRECT_IO_ALIGN - 1);
        let aligned_start = off & !(DIRECT_IO_ALIGN as i64 - 1);
        assert_eq!(raw as i64, data.len() as i64 - aligned_start);
        assert_eq!(&buf.as_slice()[delta..raw], &data[off as usize..]);

        remove_file(&path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn with_read_direct_propagates_non_einval_errors() {
        let res = LocalFile::with_read_direct("/definitely/missing/dir/block", 0);
        assert!(res.is_err(), "missing file must error, not fall back");
    }
}
