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
use crate::sys::RawIO;
use crate::{err_box, sys};
use std::io::IoSlice;
use std::sync::Arc;

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

    // Write IoSlice data into the pipeline, iov -> pipe writer
    pub async fn write_iov(&self, iov: &[IoSlice<'_>]) -> IOResult<usize> {
        let res = self
            .writer
            .async_write(|fd| sys::vm_splice(fd.fd(), iov))
            .await?;
        Ok(res as usize)
    }

    // Read data in the pipeline, write to the io object, pipe reader -> fd out
    pub async fn read_io(&self, fd_out: &AsyncFd, len: usize) -> IOResult<usize> {
        let fd_in = self.reader.raw_fd();
        let res = fd_out
            .async_write(|fd| sys::splice(fd_in, None, fd.fd(), None, len))
            .await?;
        Ok(res as usize)
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
