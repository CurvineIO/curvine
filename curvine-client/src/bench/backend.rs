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
//

use super::config::{BenchMode, BenchTarget};
use crate::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use orpc::{err_box, CommonResult};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BenchBackend {
    Curvine,
    Fuse,
}

impl BenchBackend {
    pub(crate) fn from_config(mode: BenchMode, target: BenchTarget) -> CommonResult<Self> {
        match (mode, target) {
            (BenchMode::Client, BenchTarget::Curvine) => Ok(Self::Curvine),
            (BenchMode::Fuse, BenchTarget::Fuse) => Ok(Self::Fuse),
            (mode, target) => err_box!(
                "unsupported bench access: mode={:?} target={:?}",
                mode,
                target
            ),
        }
    }
}

pub(crate) struct BenchIo<'a> {
    fs: &'a UnifiedFileSystem,
    backend: BenchBackend,
    block_size: usize,
}

impl<'a> BenchIo<'a> {
    pub(crate) fn new(
        fs: &'a UnifiedFileSystem,
        mode: BenchMode,
        target: BenchTarget,
        block_size: usize,
    ) -> CommonResult<Self> {
        Ok(Self {
            fs,
            backend: BenchBackend::from_config(mode, target)?,
            block_size,
        })
    }

    pub(crate) async fn mkdir(&self, path: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                self.fs.mkdir(&path, true).await?;
            }
            BenchBackend::Fuse => fs::create_dir_all(path).await?,
        }
        Ok(())
    }

    pub(crate) async fn delete_tree(&self, path: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                self.fs.delete(&path, true).await?;
            }
            BenchBackend::Fuse => fs::remove_dir_all(path).await?,
        }
        Ok(())
    }

    pub(crate) async fn write_file(
        &self,
        path: &str,
        file_size: usize,
        write_buf: &[u8],
    ) -> CommonResult<u64> {
        let chunk_size = self.block_size.max(1).min(file_size.max(1));
        let fallback = [0u8; 1];
        let write_buf = if write_buf.is_empty() {
            &fallback[..]
        } else {
            write_buf
        };
        let data = &write_buf[..chunk_size.min(write_buf.len())];
        let mut remaining = file_size;

        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                let mut writer = self.fs.create(&path, true).await?;
                while remaining > 0 {
                    let write_len = remaining.min(data.len());
                    writer.write(&data[..write_len]).await?;
                    remaining -= write_len;
                }
                writer.complete().await?;
            }
            BenchBackend::Fuse => {
                let mut file = fs::File::create(path).await?;
                while remaining > 0 {
                    let write_len = remaining.min(data.len());
                    file.write_all(&data[..write_len]).await?;
                    remaining -= write_len;
                }
                file.flush().await?;
            }
        }

        Ok(file_size as u64)
    }

    pub(crate) async fn read_file(&self, path: &str, buf: &mut [u8]) -> CommonResult<u64> {
        let mut bytes = 0u64;

        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                let mut reader = self.fs.open(&path).await?;
                loop {
                    let n = reader.read(buf).await?;
                    if n == 0 {
                        break;
                    }
                    bytes += n as u64;
                }
                reader.complete().await?;
            }
            BenchBackend::Fuse => {
                let mut file = fs::File::open(path).await?;
                loop {
                    let n = file.read(buf).await?;
                    if n == 0 {
                        break;
                    }
                    bytes += n as u64;
                }
            }
        }

        Ok(bytes)
    }

    pub(crate) async fn open_file(&self, path: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                let mut reader = self.fs.open(&path).await?;
                reader.complete().await?;
            }
            BenchBackend::Fuse => {
                let _file = fs::File::open(path).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn stat_file(&self, path: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                self.fs.get_status(&path).await?;
            }
            BenchBackend::Fuse => {
                fs::metadata(path).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn rename_file(&self, src: &str, dst: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let src = Path::from_str(src)?;
                let dst = Path::from_str(dst)?;
                self.fs.rename(&src, &dst).await?;
            }
            BenchBackend::Fuse => fs::rename(src, dst).await?,
        }
        Ok(())
    }

    pub(crate) async fn delete_file(&self, path: &str) -> CommonResult<()> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                self.fs.delete(&path, true).await?;
            }
            BenchBackend::Fuse => match fs::metadata(path).await {
                Ok(meta) if meta.is_dir() => fs::remove_dir_all(path).await?,
                Ok(_) => fs::remove_file(path).await?,
                Err(error) => return Err(error.into()),
            },
        }
        Ok(())
    }

    pub(crate) async fn list_dir(&self, path: &str) -> CommonResult<u64> {
        match self.backend {
            BenchBackend::Curvine => {
                let path = Path::from_str(path)?;
                self.fs.list_status(&path).await?;
            }
            BenchBackend::Fuse => {
                let mut dir = fs::read_dir(path).await?;
                while dir.next_entry().await?.is_some() {}
            }
        }
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_backend_combo() {
        assert!(BenchBackend::from_config(BenchMode::Client, BenchTarget::Fuse).is_err());
        assert!(BenchBackend::from_config(BenchMode::Fuse, BenchTarget::Curvine).is_err());
    }
}
