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

use crate::fs::operator::{OpenAction, Read, Write};
use crate::fs::{FuseReader, FuseWriter};
use crate::session::FuseResponse;
use crate::{err_fuse, FuseResult, FuseUtils, FUSE_SUCCESS};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::FileStatus;

/// File operation mode to track current file state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMode {
    /// File is idle, no active read or write operations
    Idle,
    /// File is currently being read
    Reading,
    /// File is currently being written
    Writing,
}

pub struct FuseFile {
    fs: UnifiedFileSystem,
    fh: u64,
    path: Path,
    flags: u32,
    writer: Option<FuseWriter>,
    reader: Option<FuseReader>,
    mode: FileMode,
}

impl FuseFile {
    pub async fn create(fs: UnifiedFileSystem, path: Path, flags: u32) -> FuseResult<Self> {
        let action = OpenAction::try_from(flags)?;
        let (writer, reader) = match action {
            OpenAction::ReadOnly => {
                let reader = Self::create_reader(&fs, &path).await?;
                (None, Some(reader))
            }

            OpenAction::WriteOnly => {
                let writer = Self::create_writer(&fs, &path, flags).await?;
                (Some(writer), None)
            }

            OpenAction::ReadWrite => {
                // It is explicitly to create a file.
                let writer = if FuseUtils::has_truncate(flags) || FuseUtils::has_create(flags) {
                    let writer = Self::create_writer(&fs, &path, flags).await?;
                    Some(writer)
                } else {
                    None
                };
                (writer, None)
            }
        };

        let file = Self {
            fs,
            fh: 0,
            path,
            flags,
            writer,
            reader,
            mode: FileMode::Idle,
        };
        Ok(file)
    }

    pub async fn for_write(fs: UnifiedFileSystem, path: Path, flags: u32) -> FuseResult<Self> {
        let writer = Self::create_writer(&fs, &path, flags).await?;
        let file = Self {
            fs,
            fh: 0,
            path,
            flags,
            writer: Some(writer),
            reader: None,
            mode: FileMode::Idle,
        };
        Ok(file)
    }

    async fn create_writer(
        fs: &UnifiedFileSystem,
        path: &Path,
        flags: u32,
    ) -> FuseResult<FuseWriter> {
        let overwrite = FuseUtils::has_truncate(flags);
        let writer = fs.create(path, overwrite).await?;
        Ok(FuseWriter::new(&fs.conf().fuse, fs.clone_runtime(), writer))
    }

    async fn create_reader(fs: &UnifiedFileSystem, path: &Path) -> FuseResult<FuseReader> {
        let reader = fs.open(path).await?;
        Ok(FuseReader::new(&fs.conf().fuse, fs.clone_runtime(), reader))
    }

    pub async fn write(&mut self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        // Check if file is currently being read
        if self.mode == FileMode::Reading {
            return err_fuse!(
                libc::EBUSY,
                "File {} is currently being read, cannot write",
                self.path
            );
        }

        let writer = match &mut self.writer {
            Some(v) => v,

            None => {
                if self.reader.is_some() {
                    return err_fuse!(
                        libc::ENOSYS,
                        "File {} is reading, and cannot be writing",
                        self.path
                    );
                }
                let writer = Self::create_writer(&self.fs, &self.path, self.flags).await?;
                self.writer.get_or_insert(writer)
            }
        };

        // Set file mode to writing
        self.mode = FileMode::Writing;

        let off = op.arg.offset;
        let pos = writer.pos() as u64;
        let len = op.data.len() as u64;
        if off != pos && off + len > pos {
            return err_fuse!(
                libc::EIO,
                "Only sequential write is supported, path={} offset={} size={}, pos={}",
                writer.path_str(),
                off,
                len,
                pos
            );
        }

        if off + len <= pos {
            // for fulfill vim :wq
            // Business layer error, but the return to the fuse kernel is successful.
            return err_fuse!(
                FUSE_SUCCESS,
                "Skip writing to file {} offset={} size={} when {} bytes has written to file",
                writer.path_str(),
                off,
                len,
                pos
            );
        }

        writer.write(op, reply).await?;
        Ok(())
    }

    pub async fn read(&mut self, op: Read<'_>, rep: FuseResponse) -> FuseResult<()> {
        // Check if file is currently being written
        if self.mode == FileMode::Writing {
            return err_fuse!(
                libc::EBUSY,
                "File {} is currently being written, cannot read",
                self.path
            );
        }

        let reader = match &mut self.reader {
            Some(v) => v,
            None => {
                if self.writer.is_some() {
                    return err_fuse!(
                        libc::ENOSYS,
                        "File {} is writing, and cannot be reading",
                        self.path
                    );
                }
                let reader = Self::create_reader(&self.fs, &self.path).await?;
                self.reader.get_or_insert(reader)
            }
        };

        // Set file mode to reading
        self.mode = FileMode::Reading;

        reader.read(op, rep).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> FuseResult<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FuseResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.complete().await?;
        }

        if let Some(mut reader) = self.reader.take() {
            reader.complete().await?;
        }

        // Reset file mode to idle when completing
        self.mode = FileMode::Idle;
        Ok(())
    }

    pub async fn close(&mut self) -> FuseResult<()> {
        self.complete().await
    }

    pub fn status(&self) -> FuseResult<&FileStatus> {
        if let Some(writer) = &self.writer {
            Ok(writer.status())
        } else {
            err_fuse!(libc::EIO, "{}", "Not found file status")
        }
    }

    pub fn set_fh(&mut self, fh: u64) {
        self.fh = fh;
    }

    /// Get current file operation mode
    pub fn mode(&self) -> FileMode {
        self.mode
    }
}
