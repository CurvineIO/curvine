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

use std::io::ErrorKind;

use crate::error::FsError;
use crate::fs::local::{LocalReader, LocalWriter};
use crate::fs::{FileSystem, FsKind, ListStream, Path};
use crate::state::{FileStatus, ListOptions, SetAttrOpts};
use crate::utils::CommonUtils;
use crate::FsResult;
use async_stream::stream;
use orpc::err_box;
use std::fs;

#[derive(Clone)]
pub struct LocalFilesystem {
    chunk_size: usize,
}

impl LocalFilesystem {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }
}

impl FileSystem<LocalWriter, LocalReader> for LocalFilesystem {
    fn fs_kind(&self) -> FsKind {
        FsKind::File
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        if create_parent {
            fs::create_dir_all(path.path())?;
            Ok(true)
        } else {
            match fs::create_dir(path.path()) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(e.into()),
            }
        }
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<LocalWriter> {
        LocalWriter::open_write(path, overwrite, self.chunk_size)
    }

    async fn append(&self, path: &Path) -> FsResult<LocalWriter> {
        LocalWriter::open_append(path, self.chunk_size)
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        Ok(fs::exists(path.path())?)
    }

    async fn open(&self, path: &Path) -> FsResult<LocalReader> {
        LocalReader::new(path, self.chunk_size)
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        fs::rename(src.path(), dst.path())?;
        Ok(true)
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        if path.path() == Path::SEPARATOR {
            return err_box!("Cannot delete root directory.");
        }

        let meta = fs::metadata(path.path())?;
        if meta.is_dir() {
            if recursive {
                fs::remove_dir_all(path.path())?;
            } else {
                fs::remove_dir(path.path())?;
            }
        } else {
            fs::remove_file(path.path())?;
        }
        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let meta = fs::metadata(path.path())?;
        Ok(CommonUtils::metadata_to_file_status(path, &meta))
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(path.path())? {
            let entry = entry?;
            let os_path = entry.path();
            let meta = entry.metadata()?;
            let child_path = Path::from_str(format!(
                "{}://{}",
                FsKind::SCHEME_FILE,
                os_path.to_string_lossy()
            ))?;
            out.push(CommonUtils::metadata_to_file_status(&child_path, &meta));
        }
        Ok(out)
    }

    async fn set_attr(&self, _: &Path, _: SetAttrOpts) -> FsResult<()> {
        Ok(())
    }

    async fn list_stream(&self, path: &Path, _options: ListOptions) -> FsResult<ListStream> {
        let path = path.clone();
        let rd = fs::read_dir(path.path())?;

        let stream = stream! {
            for entry in rd {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        yield Err(FsError::from(e));
                        break;
                    }
                };

                let meta = match entry.metadata() {
                    Ok(m) => m,
                    Err(e) => {
                        yield Err(FsError::from(e));
                        break;
                    }
                };

                let os_path = format!("{}://{}", FsKind::SCHEME_FILE, entry.path().to_string_lossy());
                let child_path = match Path::from_str(os_path) {
                    Ok(p) => p,
                    Err(e) => {
                        yield Err(FsError::from(e));
                        break;
                    }
                };

                let status = CommonUtils::metadata_to_file_status(&child_path, &meta);
                yield Ok(status);
            }
        };

        Ok(ListStream::new(stream))
    }
}
