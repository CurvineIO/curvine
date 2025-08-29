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
use crate::*;
use crate::{impl_reader_for_enum, impl_writer_for_enum};
use curvine_common::conf::UfsConf;
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use curvine_ufs::err_ufs;
use std::collections::HashMap;

#[cfg(feature = "s3")]
use curvine_ufs::s3::*;

#[cfg(feature = "opendal")]
use curvine_ufs::opendal::*;

// Storage schemes
pub const S3_SCHEME: &str = "s3";

pub mod macros;

mod unified_filesystem;
pub use self::unified_filesystem::UnifiedFileSystem;

mod mount_cache;
pub use self::mount_cache::*;

pub enum UnifiedWriter {
    Cv(FsWriter),

    #[cfg(feature = "s3")]
    S3(S3Writer),

    #[cfg(feature = "opendal")]
    OpenDAL(OpendalWriter),
}

impl_writer_for_enum!(UnifiedWriter);

pub enum UnifiedReader {
    Cv(FsReader),

    #[cfg(feature = "s3")]
    S3(S3Reader),

    #[cfg(feature = "opendal")]
    OpenDAL(OpendalReader),
}

impl_reader_for_enum!(UnifiedReader);

#[derive(Clone)]
pub enum UfsFileSystem {
    #[cfg(feature = "s3")]
    S3(S3FileSystem),

    #[cfg(feature = "opendal")]
    OpenDAL(OpendalFileSystem),
}

impl UfsFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        match path.scheme() {
            #[cfg(feature = "s3")]
            Some(S3_SCHEME) => {
                let fs = S3FileSystem::new(UfsConf::with_map(conf))?;
                Ok(UfsFileSystem::S3(fs))
            }

            #[cfg(feature = "opendal")]
            Some(scheme)
                if ["s3", "oss", "cos", "gcs", "azure", "azblob", "hdfs"].contains(&scheme) =>
            {
                let fs = OpendalFileSystem::new(path, UfsConf::with_map(conf))?;
                Ok(UfsFileSystem::OpenDAL(fs))
            }

            Some(scheme) => err_ufs!("Unsupported scheme: {}", scheme),

            None => err_ufs!("Missing scheme"),
        }
    }

    pub fn with_mount(mnt: &MountInfo) -> FsResult<Self> {
        let path = Path::from_str(&mnt.ufs_path)?;
        Self::new(&path, mnt.properties.clone())
    }
}
impl_filesystem_for_enum!(UfsFileSystem);
