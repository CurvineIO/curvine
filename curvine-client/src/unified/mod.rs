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
use curvine_common::state::{MountInfo, Provider};
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
pub mod pdpc;

pub use self::pdpc::{PdpcController, RouteIndexSnapshot};

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
    pub fn new(
        path: &Path,
        conf: HashMap<String, String>,
        provider: Option<Provider>,
    ) -> FsResult<Self> {
        let provider = provider.unwrap_or(Provider::Auto);

        match (provider, path.scheme()) {
            // Explicit provider selection
            (Provider::OssHdfs, Some("oss")) => {
                #[cfg(feature = "oss-hdfs")]
                {
                    let fs = OssHdfsFileSystem::new(path, conf)?;
                    Ok(UfsFileSystem::OssHdfs(fs))
                }
                #[cfg(not(feature = "oss-hdfs"))]
                {
                    err_box!("oss-hdfs provider is not enabled")
                }
            }

            (Provider::Opendal, Some(scheme))
                if [
                    "s3", "oss", "cos", "gcs", "azure", "azblob", "hdfs", "webhdfs",
                ]
                .contains(&scheme) =>
            {
                #[cfg(feature = "opendal")]
                {
                    // JVM initialization for HDFS is handled in OpendalFileSystem::new
                    let fs = OpendalFileSystem::new(path, conf)?;
                    Ok(UfsFileSystem::Opendal(fs))
                }
                #[cfg(not(feature = "opendal"))]
                {
                    err_box!("opendal provider is not enabled")
                }
            }

            // Auto-detect (backward compatible)
            (Provider::Auto, Some("oss")) => {
                // Check for provider in config
                match conf.get("provider").map(|s| s.as_str()) {
                    Some("oss-hdfs") => {
                        #[cfg(feature = "oss-hdfs")]
                        {
                            let fs = OssHdfsFileSystem::new(path, conf)?;
                            Ok(UfsFileSystem::OssHdfs(fs))
                        }
                        #[cfg(not(feature = "oss-hdfs"))]
                        {
                            err_box!("oss-hdfs provider is not enabled")
                        }
                    }
                    Some("opendal") => {
                        #[cfg(feature = "opendal")]
                        {
                            let fs = OpendalFileSystem::new(path, conf)?;
                            Ok(UfsFileSystem::Opendal(fs))
                        }
                        #[cfg(not(feature = "opendal"))]
                        {
                            err_box!("opendal provider is not enabled")
                        }
                    }
                    Some(other) => err_box!("invalid provider in config: {}", other),
                    None => {
                        // Current default: oss-hdfs takes precedence
                        #[cfg(feature = "oss-hdfs")]
                        {
                            let fs = OssHdfsFileSystem::new(path, conf)?;
                            Ok(UfsFileSystem::OssHdfs(fs))
                        }
                        #[cfg(all(feature = "opendal", not(feature = "oss-hdfs")))]
                        {
                            let fs = OpendalFileSystem::new(path, conf)?;
                            Ok(UfsFileSystem::Opendal(fs))
                        }
                        #[cfg(not(any(feature = "oss-hdfs", feature = "opendal")))]
                        {
                            err_box!("no OSS provider is enabled")
                        }
                    }
                }
            }

            // Other schemes with auto provider
            #[cfg(feature = "opendal")]
            (Provider::Auto, Some(scheme))
                if ["s3", "cos", "gcs", "azure", "azblob", "hdfs", "webhdfs"].contains(&scheme) =>
            {
                let fs = OpendalFileSystem::new(path, conf)?;
                Ok(UfsFileSystem::Opendal(fs))
            }

            (Provider::Auto, Some(scheme)) => err_box!("unsupported scheme: {}", scheme),

            (Provider::Auto, None) => err_box!("missing scheme"),

            (provider, Some(scheme)) => {
                err_box!(
                    "provider {:?} is not compatible with scheme {}",
                    provider,
                    scheme
                )
            }
            (_provider, None) => err_box!("missing scheme"),
        }
    }

    pub fn with_mount(mnt: &MountInfo) -> FsResult<Self> {
        let path = Path::from_str(&mnt.ufs_path)?;
        Self::new(&path, mnt.properties.clone(), mnt.provider)
    }
}
