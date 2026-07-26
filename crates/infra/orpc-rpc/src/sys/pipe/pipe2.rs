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
use crate::sys::pipe::{AsyncFd, PipeFd, PipePool, PipeReader, PipeWriter};
use crate::sys::{CInt, RawIO};
use crate::{err_box, sys};
use log::warn;
use std::io::IoSlice;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Backoff bounds for the splice-path EAGAIN retry loop (see `splice_retry`).
// /dev/fuse is permanently level-writable, so its writable edge never fires
// again after tokio's edge-triggered readiness clears on an EAGAIN from the
// non-blocking splice. Awaiting the WRITABLE readiness there hangs forever
// (issue #1215); instead we retry the raw splice behind a bounded async backoff.
const SPLICE_RETRY_MIN: Duration = Duration::from_micros(50);
const SPLICE_RETRY_MAX: Duration = Duration::from_millis(5);
// While retrying continuous EAGAIN, log a warning this often. Until the sender
// watchdog lands (#1215 PR-B), a stuck sender is otherwise invisible (it makes
// no progress and emits no error); this surfaces the failure mode in the field.
const SPLICE_RETRY_WARN_INTERVAL: Duration = Duration::from_secs(5);

pub struct Pipe2 {
    buf_size: usize,
    pipe_fd: Option<PipeFd>,
    writer: PipeWriter,
    reader: PipeReader,
    pool: Option<Arc<PipePool>>,
}

impl Pipe2 {
    pub fn new(pipe_fd: PipeFd) -> IOResult<Self> {
        let writer = PipeWriter::new(pipe_fd.write.as_borrowed())?;
        let reader = PipeReader::new(pipe_fd.read.as_borrowed())?;
        let pipe2 = Self {
            buf_size: pipe_fd.buf_size,
            pipe_fd: Some(pipe_fd),
            writer,
            reader,
            pool: None,
        };

        Ok(pipe2)
    }

    pub fn set_pool(&mut self, pool: Arc<PipePool>) {
        let _ = self.pool.replace(pool);
    }

    pub fn write_raw_fd(&self) -> RawIO {
        self.writer.raw_fd()
    }

    pub fn read_raw_fd(&self) -> RawIO {
        self.reader.raw_fd()
    }

    pub fn writer(&self) -> &PipeWriter {
        &self.writer
    }

    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub async fn readable(&self) -> IOResult<()> {
        match self.reader.async_fd() {
            None => err_box!("Unsupported operation"),
            Some(v) => v.readable().await,
        }
    }

    pub async fn writable(&self) -> IOResult<()> {
        match self.writer.async_fd() {
            None => err_box!("Unsupported operation"),
            Some(v) => v.writable().await,
        }
    }

    pub fn reader(&self) -> &PipeReader {
        &self.reader
    }

    // Read data from AsyncFd and write to the pipeline, fd_in -> pipe writer
    pub async fn write_io(
        &self,
        fd_in: &AsyncFd,
        mut off_in: Option<i64>,
        len: usize,
    ) -> IOResult<usize> {
        let fd_out = self.writer.raw_fd();
        let res = fd_in
            .async_read(|fd| sys::splice(fd.fd(), off_in.as_mut(), fd_out, None, len))
            .await?;
        Ok(res as usize)
    }

    // Write IoSlice data into the pipeline, iov -> pipe writer.
    pub async fn write_iov(&self, len: usize, iov: &[IoSlice<'_>]) -> IOResult<()> {
        if len > self.buf_size {
            return err_box!(
                "write_iov: data size {} exceeds pipe buffer size {}",
                len,
                self.buf_size
            );
        }

        let mut written = 0;
        while written < len {
            let res = if written == 0 {
                self.writer
                    .async_write(|fd| sys::vm_splice(fd.fd(), iov))
                    .await?
            } else {
                let cur_iov = Self::skip_iov_bytes(iov, written);
                self.writer
                    .async_write(|fd| sys::vm_splice(fd.fd(), &cur_iov))
                    .await?
            };
            if res == 0 {
                return err_box!("vmsplice returned 0");
            }
            written += res as usize;
        }
        Ok(())
    }

    // Read data in the pipeline, write to the io object, pipe reader -> fd out.
    // Loops to completion: a partial splice (short transfer under SPLICE_F_NONBLOCK)
    // is retried until all bytes are transferred, preventing pipe poisoning (issue #965).
    pub async fn read_io(&self, fd_out: &AsyncFd, len: usize) -> IOResult<()> {
        if len > self.buf_size {
            return err_box!(
                "read_io: request size {} exceeds pipe buffer size {}",
                len,
                self.buf_size
            );
        }
        let fd_in = self.reader.raw_fd();
        let fd_out = fd_out.raw_fd();
        let mut remaining = len;
        while remaining > 0 {
            // /dev/fuse is permanently level-writable, so on EAGAIN its writable
            // edge never fires again and awaiting AsyncFd WRITABLE readiness
            // hangs forever (#1215). Retry the raw splice behind a bounded async
            // backoff rather than waiting on the tokio readiness.
            let res =
                Self::splice_retry(|| sys::splice(fd_in, None, fd_out, None, remaining)).await?;
            if res == 0 {
                return err_box!("splice returned 0");
            }
            remaining -= res as usize;
        }
        Ok(())
    }

    // Run the non-blocking splice syscall, retrying on EAGAIN behind a bounded
    // exponential async backoff. Used on the FUSE reply path where the
    // destination fd (/dev/fuse) is permanently level-writable, so tokio's
    // edge-triggered WRITABLE readiness never fires again after an EAGAIN and
    // awaiting it hangs forever (#1215). EINTR is retried immediately; any other
    // error propagates. The backoff sleeps on the tokio timer so it yields the
    // worker instead of busy-spinning. Note: if the destination stays EAGAIN
    // forever this retries indefinitely (bounded-rate, not a hang) by design;
    // detecting a stuck sender and giving up is the watchdog's job (#1215 PR-B).
    // While it keeps hitting EAGAIN it logs a warning every
    // SPLICE_RETRY_WARN_INTERVAL so the otherwise-silent HOL-blocked sender is
    // visible in the field before the watchdog lands.
    async fn splice_retry(mut f: impl FnMut() -> IOResult<CInt>) -> IOResult<CInt> {
        let mut delay = SPLICE_RETRY_MIN;
        // Set on the first EAGAIN; measures how long this call has been stalled
        // on continuous would-block. (Ok / real errors return, so there is no
        // interleaved success to reset it within a single call.)
        let mut eagain_since: Option<Instant> = None;
        let mut next_warn = SPLICE_RETRY_WARN_INTERVAL;
        loop {
            match f() {
                Ok(res) => return Ok(res),
                Err(e) => {
                    let os = e.raw_error().raw_os_error();
                    if os == Some(libc::EINTR) {
                        continue;
                    }
                    if e.is_would_block() {
                        let stalled = match eagain_since {
                            Some(start) => start.elapsed(),
                            None => {
                                eagain_since = Some(Instant::now());
                                Duration::ZERO
                            }
                        };
                        if stalled >= next_warn {
                            warn!(
                                "splice to fuse fd stuck on EAGAIN for {:?}; still retrying \
                                 (bounded-rate). A sender may be HOL-blocked (#1215).",
                                stalled
                            );
                            next_warn += SPLICE_RETRY_WARN_INTERVAL;
                        }
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(SPLICE_RETRY_MAX);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    // Read the data of the pipeline into buf, pipe read -> buf
    pub async fn read_buf(&self, buf: &mut [u8]) -> IOResult<usize> {
        let res = self.reader.async_read(|fd| sys::read(fd.fd(), buf)).await?;
        Ok(res as usize)
    }

    pub fn deregister(&mut self) -> PipeFd {
        let _ = self.reader.deregister();
        let _ = self.writer.deregister();

        self.take_fd()
    }

    pub fn take_fd(&mut self) -> PipeFd {
        self.pipe_fd.take().unwrap()
    }

    /// Build a new iovec that skips the first `offset` bytes from `iov`.
    /// Used by `write_iov` to resume a partially completed vmsplice transfer.
    fn skip_iov_bytes<'a>(iov: &'a [IoSlice<'a>], mut offset: usize) -> Vec<IoSlice<'a>> {
        let mut result = Vec::with_capacity(iov.len());
        for slice in iov {
            if offset == 0 {
                result.push(IoSlice::new(&slice[..]));
            } else if slice.len() <= offset {
                offset -= slice.len();
            } else {
                result.push(IoSlice::new(&slice[offset..]));
                offset = 0;
            }
        }
        result
    }
}

impl Drop for Pipe2 {
    fn drop(&mut self) {
        let pool = self.pool.take();
        if let Some(pool) = pool {
            // Write and reader have been dropped and removed from the tokio poller.
            // The pipeline in the resource is returned to the resource pool, not close.
            pool.release(self)
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::Pipe2;
    use crate::io::{IOError, IOResult};
    use crate::sys::CInt;
    use std::cell::Cell;
    use std::io;

    fn eagain() -> IOError {
        IOError::new(io::Error::from_raw_os_error(libc::EAGAIN))
    }

    fn eintr() -> IOError {
        IOError::new(io::Error::from_raw_os_error(libc::EINTR))
    }

    // Regression for #1215: on a permanently level-writable fd (/dev/fuse), the
    // syscall returns EAGAIN but tokio's edge-triggered WRITABLE readiness never
    // fires again. `splice_retry` must NOT propagate WouldBlock nor hang — it
    // must retry the raw syscall until it succeeds. The OLD code awaited
    // async_write readiness here and hung forever, so this test fails against it.
    #[tokio::test]
    async fn splice_retry_retries_eagain_until_ok() {
        let calls = Cell::new(0u32);
        let f = || -> IOResult<CInt> {
            let n = calls.get();
            calls.set(n + 1);
            // First 5 attempts report EAGAIN (would-block), then succeed.
            if n < 5 {
                Err(eagain())
            } else {
                Ok(4096)
            }
        };

        let res = Pipe2::splice_retry(f)
            .await
            .expect("must complete, not hang");
        assert_eq!(res, 4096);
        assert_eq!(calls.get(), 6, "5 EAGAIN retries + 1 success");
    }

    // EINTR is retried immediately (no backoff), like the drain loop in #965.
    #[tokio::test]
    async fn splice_retry_retries_eintr() {
        let calls = Cell::new(0u32);
        let f = || -> IOResult<CInt> {
            let n = calls.get();
            calls.set(n + 1);
            if n < 3 {
                Err(eintr())
            } else {
                Ok(1)
            }
        };
        let res = Pipe2::splice_retry(f).await.unwrap();
        assert_eq!(res, 1);
        assert_eq!(calls.get(), 4);
    }

    // A genuine (non-would-block, non-EINTR) error must propagate, not be
    // swallowed by the retry loop — otherwise a real splice failure would spin
    // forever. Uses EPIPE (broken pipe), a real splice failure mode.
    #[tokio::test]
    async fn splice_retry_propagates_real_error() {
        let f =
            || -> IOResult<CInt> { Err(IOError::new(io::Error::from_raw_os_error(libc::EPIPE))) };
        let err = Pipe2::splice_retry(f).await.unwrap_err();
        assert_eq!(err.raw_error().raw_os_error(), Some(libc::EPIPE));
    }
}
