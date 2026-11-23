// Copyright 2026 OPPO.
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

use curvine_io::{IOResult, LocalFile};
use std::io;

/// Backend for buffered I/O in remote, file-backed worker sessions.
#[derive(Clone, Default)]
pub struct FileIoBackend {
    #[cfg(target_os = "linux")]
    uring: Option<crate::uring::UringExecutor>,
    #[cfg(all(target_os = "linux", feature = "test-util"))]
    staged_pwrite: bool,
}

impl FileIoBackend {
    pub fn start_uring() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        return Ok(Self {
            uring: Some(crate::uring::UringExecutor::start()?),
            #[cfg(feature = "test-util")]
            staged_pwrite: false,
        });
        #[cfg(not(target_os = "linux"))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires Linux",
        ))
    }

    pub fn is_uring(&self) -> bool {
        #[cfg(target_os = "linux")]
        return self.uring.is_some();
        #[cfg(not(target_os = "linux"))]
        false
    }

    #[cfg(all(target_os = "linux", feature = "test-util"))]
    pub fn staged_pwrite_for_test() -> Self {
        Self {
            uring: None,
            staged_pwrite: true,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn supports_staged_writes(&self) -> bool {
        #[cfg(feature = "test-util")]
        if self.staged_pwrite {
            return true;
        }
        self.is_uring()
    }

    /// Register once for successful workload bytes (read, write), excluding
    /// the startup probe. A local backend never calls this observer.
    pub fn observe_completed_bytes(
        &self,
        _observer: impl Fn(usize, usize) + Send + Sync + 'static,
    ) {
        #[cfg(target_os = "linux")]
        if let Some(uring) = &self.uring {
            uring.observe_completed_bytes(_observer);
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn open_staged_writer(
        &self,
        path: std::path::PathBuf,
    ) -> IOResult<crate::StagedFileWriter> {
        #[cfg(feature = "test-util")]
        if self.staged_pwrite {
            use std::os::unix::fs::FileExt;
            return crate::StagedFileWriter::new(path, |file, offset, buf| {
                file.as_ref().write_all_at(buf, offset)
            });
        }
        let uring = self.uring.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "worker io_uring is not active")
        })?;
        crate::StagedFileWriter::new(path, move |file, offset, buf| {
            uring.write_all_at(file, offset, buf)
        })
    }

    pub(crate) fn attach_reader(&self, _file: &mut LocalFile) -> IOResult<()> {
        #[cfg(target_os = "linux")]
        if let Some(uring) = &self.uring {
            let uring = uring.clone();
            _file.set_owned_positional_reader(move |file| {
                let file = std::sync::Arc::new(file);
                move |offset, len| {
                    let (file, uring) = (file.clone(), uring.clone());
                    async move { uring.read_owned_at(file, offset, len).await }
                }
            })?;
        }
        Ok(())
    }

    /// Reproduce the blocking handoff/copy only in the benchmark control arm.
    #[cfg(all(target_os = "linux", feature = "test-util"))]
    pub fn attach_blocking_reader_for_test(&self, file: &mut LocalFile) -> IOResult<()> {
        let uring = self.uring.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "blocking read control requires io_uring",
            )
        })?;
        file.set_positional_reader(move |file| {
            let file = std::sync::Arc::new(file);
            move |buf: &mut [u8], offset| uring.read_exact_at(&file, offset, buf)
        })
    }
}
