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

use crate::file::{FsReader, FsWriter};
use crate::impl_filesystem_for_enum;
use crate::{impl_reader_for_enum, impl_writer_for_enum};
use curvine_common::conf::UfsConf;
use curvine_common::fs::Path;
use curvine_common::FsResult;
use curvine_ufs::err_ufs;
use curvine_ufs::s3::S3FileSystem;
use curvine_ufs::s3::{S3Reader, S3Writer};
use std::collections::HashMap;

pub const S3_SCHEME: &str = "s3";

pub mod macros;
mod opendal_filesystem;
use self::opendal_filesystem::{OpendalFileSystem, OpendalReader, OpendalWriter};

mod unified_filesystem;
pub use self::unified_filesystem::UnifiedFileSystem;

pub enum UnifiedWriter {
    Cv(FsWriter),
    S3(S3Writer),
    OpenDAL(OpendalWriter),
}

impl_writer_for_enum!(
    UnifiedWriter {
        Cv(FsWriter),
        S3(S3Writer),
        OpenDAL(OpendalWriter)
    }
);

pub enum UnifiedReader {
    Cv(FsReader),
    S3(S3Reader),
    OpenDAL(OpendalReader),
}

impl_reader_for_enum!(
    UnifiedReader {
        Cv(FsReader),
        S3(S3Reader),
        OpenDAL(OpendalReader)
    }
);

// @todo Too much repeated code, subsequent optimization
#[derive(Clone)]
pub enum UfsFileSystem {
    S3(S3FileSystem),
    OpenDAL(OpendalFileSystem),
}

impl UfsFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        match path.scheme() {
            Some(S3_SCHEME) => {
                let fs = S3FileSystem::new(UfsConf::with_map(conf))?;
                Ok(UfsFileSystem::S3(fs))
            }
            Some("gcs") | Some("gs") | Some("azure") | Some("azblob") | Some("abfs")
            | Some("oss") => {
                let fs = OpendalFileSystem::new(path, UfsConf::with_map(conf))?;
                Ok(UfsFileSystem::OpenDAL(fs))
            }
            _ => {
                err_ufs!("Unsupported scheme: {}", path.scheme().unwrap_or("Unknown"))
            }
        }
    }
}

impl_filesystem_for_enum!(
    UfsFileSystem {
        S3(S3FileSystem),
        OpenDAL(OpendalFileSystem)
    }
);
