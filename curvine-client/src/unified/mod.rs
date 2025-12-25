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
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use orpc::err_box;
use std::collections::HashMap;

#[cfg(feature = "opendal")]
use curvine_ufs::opendal::*;

#[cfg(feature = "oss-hdfs")]
use curvine_ufs::oss_hdfs::*;

// Storage schemes
pub const S3_SCHEME: &str = "s3";

pub mod macros;

mod unified_filesystem;
pub use self::unified_filesystem::UnifiedFileSystem;

mod mount_cache;
pub use self::mount_cache::*;

mod cache_sync_writer;
pub use self::cache_sync_writer::CacheSyncWriter;

mod cache_sync_reader;
pub use self::cache_sync_reader::CacheSyncReader;

#[allow(clippy::large_enum_variant)]
pub enum UnifiedWriter {
    Cv(FsWriter),

    CacheSync(CacheSyncWriter),

    #[cfg(feature = "opendal")]
    Opendal(OpendalWriter),

    #[cfg(feature = "oss-hdfs")]
    OssHdfs(OssHdfsWriter),
}

impl_writer_for_enum! {
    enum UnifiedWriter {
        Cv(FsWriter),

        CacheSync(CacheSyncWriter),

        #[cfg(feature = "opendal")]
        Opendal(OpendalWriter),

        #[cfg(feature = "oss-hdfs")]
        OssHdfs(OssHdfsWriter),
    }
}

#[allow(clippy::large_enum_variant)]
pub enum UnifiedReader {
    Cv(FsReader),

    CacheSync(CacheSyncReader),

    #[cfg(feature = "opendal")]
    Opendal(OpendalReader),

    #[cfg(feature = "oss-hdfs")]
    OssHdfs(OssHdfsReader),
}

impl_reader_for_enum! {
    enum UnifiedReader {
        Cv(FsReader),

        CacheSync(CacheSyncReader),

        #[cfg(feature = "opendal")]
        Opendal(OpendalReader),

        #[cfg(feature = "oss-hdfs")]
        OssHdfs(OssHdfsReader),
    }
}

#[derive(Clone)]
pub enum UfsFileSystem {
    #[cfg(feature = "opendal")]
    Opendal(OpendalFileSystem),

    #[cfg(feature = "oss-hdfs")]
    OssHdfs(OssHdfsFileSystem),
}

impl_filesystem_for_enum! {
    enum UfsFileSystem {
        #[cfg(feature = "opendal")]
        Opendal(OpendalFileSystem),

        #[cfg(feature = "oss-hdfs")]
        OssHdfs(OssHdfsFileSystem),
    }
}

impl UfsFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        match path.scheme() {
            // Jindo OSS backend (async-only)
            #[cfg(feature = "oss-hdfs")]
            Some("oss") => {
                let fs = OssHdfsFileSystem::new(path, conf)?;
                Ok(UfsFileSystem::OssHdfs(fs))
            }

            #[cfg(feature = "opendal")]
            Some(scheme)
                if [
                    "s3", "oss", "cos", "gcs", "azure", "azblob", "hdfs", "webhdfs",
                ]
                .contains(&scheme) =>
            {
                // JVM initialization for HDFS is handled in OpendalFileSystem::new
                let fs = OpendalFileSystem::new(path, conf)?;
                Ok(UfsFileSystem::Opendal(fs))
            }

            Some(scheme) => err_box!("unsupported scheme: {}", scheme),

            None => err_box!("missing scheme"),
        }
    }

    pub fn with_mount(mnt: &MountInfo) -> FsResult<Self> {
        let path = Path::from_str(&mnt.ufs_path)?;
        Self::new(&path, mnt.properties.clone())
    }
}
