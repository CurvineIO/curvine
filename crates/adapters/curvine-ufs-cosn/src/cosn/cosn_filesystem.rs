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

use crate::cosn::{cosn_error, cstring, CosnBuilder, CosnFile, CosnFs, CosnReader, CosnWriter, SCHEME};
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, FsKind, Path};
use curvine_model::{DeleteResult, FileStatus, FileType, SetAttrOpts};
use curvine_ufs_api::{err_ufs, CosnConf};
use hdfs_sys::*;
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_int;
use std::sync::Arc;

const DEFAULT_BLOCK_SIZE: i64 = 4 * 1024 * 1024;
const DEFAULT_REPLICAS: i32 = 1;

/// COSN filesystem reached through libhdfs + the vendor `hadoop-cos` jar.
#[derive(Clone)]
pub struct CosnFileSystem {
    inner: Arc<CosnFileSystemInner>,
}

struct CosnFileSystemInner {
    fs: Arc<CosnFs>,
    /// Authority on Curvine/Hive URIs. May be the short bucket when `appid` is set.
    uri_bucket: String,
    /// Authority passed to libhdfs (`bucket` or `bucket-appid`).
    namenode_bucket: String,
}

/// Rewrites a Curvine/Hive COSN path to the authority libhdfs connected with.
///
/// Mount and Hive LOCATION can stay `cosn://bucket/...` (short name). HeadBucket
/// and subsequent libhdfs calls need `cosn://bucket-appid/...`.
pub(crate) fn libhdfs_uri(path: &Path, uri_bucket: &str, namenode_bucket: &str) -> String {
    if path.scheme() != Some(SCHEME) {
        return path.path().to_owned();
    }
    if uri_bucket == namenode_bucket {
        return path.full_path().to_owned();
    }
    match path.authority() {
        Some(auth) if auth == uri_bucket || auth == namenode_bucket => {
            format!("{}://{}{}", SCHEME, namenode_bucket, path.path())
        }
        _ => path.full_path().to_owned(),
    }
}

/// Builds a Curvine path for a list entry. `mName` may be a full URI or an
/// absolute path under the bucket. Hadoop may echo the namenode authority
/// (`bucket-appid`); rewrite back to `uri_bucket` so mount lookup still matches.
pub(crate) fn entry_path_for_bucket(
    uri_bucket: &str,
    namenode_bucket: &str,
    m_name: &str,
) -> FsResult<Path> {
    let path = if m_name.contains("://") {
        Path::from_str(m_name.trim_end_matches('/')).map_err(FsError::from)?
    } else {
        let trimmed = m_name.trim_start_matches('/').trim_end_matches('/');
        Path::from_str(format!("{}://{}/{}", SCHEME, uri_bucket, trimmed)).map_err(FsError::from)?
    };

    if uri_bucket != namenode_bucket && path.authority() == Some(namenode_bucket) {
        return Path::from_str(format!("{}://{}{}", SCHEME, uri_bucket, path.path()))
            .map_err(FsError::from);
    }
    Ok(path)
}

pub(crate) fn file_status_from_info(path: &Path, info: &hdfsFileInfo) -> FileStatus {
    let is_dir = info.mKind == tObjectKind_kObjectKindDirectory;
    // libhdfs reports mtime/atime in seconds; Curvine FileStatus uses ms.
    let mtime = (info.mLastMod as i64).saturating_mul(1000);
    FileStatus {
        path: path.full_path().to_owned(),
        name: path.name().to_owned(),
        is_dir,
        is_complete: true,
        len: if is_dir { 0 } else { info.mSize },
        replicas: if info.mReplication > 0 {
            info.mReplication as i32
        } else {
            DEFAULT_REPLICAS
        },
        block_size: if info.mBlockSize > 0 {
            info.mBlockSize
        } else {
            DEFAULT_BLOCK_SIZE
        },
        file_type: if is_dir {
            FileType::Dir
        } else {
            FileType::File
        },
        mtime,
        mode: info.mPermissions as u32,
        ..Default::default()
    }
}

fn c_str_lossy(ptr: *const std::os::raw::c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

impl CosnFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        let scheme = path
            .scheme()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "Missing scheme"))?;
        if scheme != SCHEME {
            return Err(FsError::invalid_path(
                path.full_path(),
                format!("Expected scheme '{}', got '{}'", SCHEME, scheme),
            ));
        }

        let uri_bucket = path
            .authority()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "URI missing bucket name"))?
            .to_string();

        let cosn_conf = CosnConf::with_map(conf).map_err(|e| {
            FsError::common(format!("Invalid COSN configuration: {}", e))
        })?;

        // libhdfs treats the "name node" string as a filesystem URI when it
        // contains `://`. COS HeadBucket needs `cosn://bucket-appid`; the mount
        // URI may still be the short Hive name `cosn://bucket`.
        let namenode_bucket = cosn_conf.namenode_bucket(&uri_bucket);
        let name_node = format!("{}://{}", SCHEME, namenode_bucket);
        let mut builder = CosnBuilder::new(name_node)?;
        builder.force_new_instance();

        if let Some(user) = &cosn_conf.user {
            builder.user(user)?;
        } else if let Ok(user) = std::env::var("HADOOP_USER_NAME").or_else(|_| std::env::var("USER"))
        {
            builder.user(&user)?;
        }

        let conf_pairs: Vec<(String, String)> = cosn_conf.hadoop_conf_pairs();
        for (key, value) in conf_pairs {
            builder.conf_set(&key, &value)?;
        }

        let fs = builder.connect()?;
        Ok(Self {
            inner: Arc::new(CosnFileSystemInner {
                fs: Arc::new(fs),
                uri_bucket,
                namenode_bucket,
            }),
        })
    }

    fn path_cstring(&self, path: &Path) -> FsResult<std::ffi::CString> {
        // Prefer a `cosn://...` URI whose authority matches the connected
        // namenode (`bucket-appid` when appid is set). CosFileSystem is
        // URI-aware and rejects bare relative paths the same way JindoSDK
        // does for OSS.
        let s = libhdfs_uri(path, &self.inner.uri_bucket, &self.inner.namenode_bucket);
        cstring(&s)
    }

    async fn with_fs_blocking<T, F>(&self, f: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<CosnFs>) -> FsResult<T> + Send + 'static,
    {
        let fs = Arc::clone(&self.inner.fs);
        tokio::task::spawn_blocking(move || f(fs))
            .await
            .map_err(|e| FsError::common(format!("COSN blocking operation failed: {}", e)))?
    }
}

impl FileSystem<CosnWriter, CosnReader> for CosnFileSystem {
    fn fs_kind(&self) -> FsKind {
        FsKind::Cosn
    }

    async fn mkdir(&self, path: &Path, _create_parent: bool) -> FsResult<bool> {
        // libhdfs `hdfsCreateDirectory` creates parents recursively.
        let path_c = self.path_cstring(path)?;
        let path_owned = path.full_path().to_owned();
        self.with_fs_blocking(move |fs| {
            let rc = unsafe { hdfsCreateDirectory(fs.as_raw(), path_c.as_ptr()) };
            if rc != 0 {
                return Err(cosn_error("create directory", &path_owned));
            }
            Ok(true)
        })
        .await
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<CosnWriter> {
        let exists = self.exists(path).await?;
        if exists && !overwrite {
            return Err(FsError::file_exists(path.full_path()));
        }

        let path_c = self.path_cstring(path)?;
        let path_owned = path.clone();
        let flags = if overwrite {
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC
        } else {
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL
        };

        let file = self
            .with_fs_blocking(move |fs| {
                let raw = fs.open_file(path_c.as_c_str(), flags)?;
                Ok(CosnFile::new(fs, raw))
            })
            .await?;

        let status = FileStatus {
            path: path_owned.full_path().to_owned(),
            name: path_owned.name().to_owned(),
            is_dir: false,
            is_complete: false,
            replicas: DEFAULT_REPLICAS,
            block_size: DEFAULT_BLOCK_SIZE,
            file_type: FileType::File,
            mode: 0o644,
            ..Default::default()
        };

        Ok(CosnWriter::new(file, path_owned, status))
    }

    async fn append(&self, path: &Path) -> FsResult<CosnWriter> {
        err_ufs!(
            "Append is not supported for COSN path {} (bucket append disabled)",
            path.full_path()
        )
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let path_c = self.path_cstring(path)?;
        let path_owned = path.full_path().to_owned();
        self.with_fs_blocking(move |fs| {
            let rc = unsafe { hdfsExists(fs.as_raw(), path_c.as_ptr()) };
            if rc == 0 {
                Ok(true)
            } else {
                let err = cosn_error("stat", &path_owned);
                if matches!(err, FsError::FileNotFound(_)) {
                    return Ok(false);
                }
                let msg = err.to_string().to_ascii_lowercase();
                if msg.contains("not found") || msg.contains("filenotfound") {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        })
        .await
    }

    async fn open(&self, path: &Path) -> FsResult<CosnReader> {
        let status = self.get_status(path).await?;
        if status.is_dir {
            return err_box!("Cannot open directory as a file: {}", path.full_path());
        }

        let path_c = self.path_cstring(path)?;
        let path_owned = path.clone();
        let status_clone = status.clone();
        let file = self
            .with_fs_blocking(move |fs| {
                let raw = fs.open_file(path_c.as_c_str(), libc::O_RDONLY)?;
                Ok(CosnFile::new(fs, raw))
            })
            .await?;

        Ok(CosnReader::new(file, path_owned, status_clone))
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_c = self.path_cstring(src)?;
        let dst_c = self.path_cstring(dst)?;
        let src_owned = src.full_path().to_owned();
        self.with_fs_blocking(move |fs| {
            let rc = unsafe { hdfsRename(fs.as_raw(), src_c.as_ptr(), dst_c.as_ptr()) };
            if rc != 0 {
                return Err(cosn_error("rename", &src_owned));
            }
            Ok(true)
        })
        .await
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<DeleteResult> {
        let path_c = self.path_cstring(path)?;
        let path_owned = path.full_path().to_owned();
        let recursive_flag: c_int = if recursive { 1 } else { 0 };
        self.with_fs_blocking(move |fs| {
            let rc = unsafe { hdfsDelete(fs.as_raw(), path_c.as_ptr(), recursive_flag) };
            if rc != 0 {
                return Err(cosn_error("delete", &path_owned));
            }
            Ok(DeleteResult::default())
        })
        .await
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let path_c = self.path_cstring(path)?;
        let path_owned = path.clone();
        self.with_fs_blocking(move |fs| {
            let info = unsafe { hdfsGetPathInfo(fs.as_raw(), path_c.as_ptr()) };
            if info.is_null() {
                return Err(cosn_error("get status", path_owned.full_path()));
            }
            let status = unsafe {
                let status = file_status_from_info(&path_owned, &*info);
                hdfsFreeFileInfo(info, 1);
                status
            };
            Ok(status)
        })
        .await
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let path_c = self.path_cstring(path)?;
        let path_owned = path.clone();
        let uri_bucket = self.inner.uri_bucket.clone();
        let namenode_bucket = self.inner.namenode_bucket.clone();
        self.with_fs_blocking(move |fs| {
            let mut num_entries: c_int = 0;
            let infos = unsafe { hdfsListDirectory(fs.as_raw(), path_c.as_ptr(), &mut num_entries) };
            if infos.is_null() {
                let err = cosn_error("list directory", path_owned.full_path());
                if matches!(err, FsError::FileNotFound(_)) {
                    return Err(err);
                }
                if num_entries == 0 {
                    return Ok(Vec::new());
                }
                return Err(err);
            }

            let mut statuses = Vec::with_capacity(num_entries as usize);
            for i in 0..num_entries {
                let info = unsafe { &*infos.offset(i as isize) };
                let name = c_str_lossy(info.mName);
                let entry_path = entry_path_for_bucket(&uri_bucket, &namenode_bucket, &name)?;
                if entry_path.path() == path_owned.path() {
                    continue;
                }
                statuses.push(file_status_from_info(&entry_path, info));
            }

            unsafe { hdfsFreeFileInfo(infos, num_entries) };
            Ok(statuses)
        })
        .await
    }

    async fn set_attr(&self, _path: &Path, _opts: SetAttrOpts) -> FsResult<()> {
        err_ufs!("SetAttr is not supported by the COSN filesystem")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn file_status_from_info_converts_dir() {
        let path = Path::from_str("cosn://bucket-125/dir").unwrap();
        let info = hdfsFileInfo {
            mKind: tObjectKind_kObjectKindDirectory,
            mName: ptr::null_mut(),
            mLastMod: 1_700_000_000,
            mSize: 99,
            mReplication: 0,
            mBlockSize: 0,
            mOwner: ptr::null_mut(),
            mGroup: ptr::null_mut(),
            mPermissions: 0o755,
            mLastAccess: 0,
        };
        let status = file_status_from_info(&path, &info);
        assert!(status.is_dir);
        assert_eq!(status.len, 0);
        assert_eq!(status.mtime, 1_700_000_000_000);
        assert_eq!(status.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(status.file_type, FileType::Dir);
    }

    #[test]
    fn file_status_from_info_converts_file() {
        let path = Path::from_str("cosn://bucket-125/a.txt").unwrap();
        let info = hdfsFileInfo {
            mKind: tObjectKind_kObjectKindFile,
            mName: ptr::null_mut(),
            mLastMod: 100,
            mSize: 42,
            mReplication: 3,
            mBlockSize: 8 * 1024 * 1024,
            mOwner: ptr::null_mut(),
            mGroup: ptr::null_mut(),
            mPermissions: 0o644,
            mLastAccess: 0,
        };
        let status = file_status_from_info(&path, &info);
        assert!(!status.is_dir);
        assert_eq!(status.len, 42);
        assert_eq!(status.replicas, 3);
        assert_eq!(status.block_size, 8 * 1024 * 1024);
        assert_eq!(status.mtime, 100_000);
    }

    #[test]
    fn entry_path_accepts_absolute_and_uri() {
        let from_abs = entry_path_for_bucket("bucket-125", "bucket-125", "/dir/file").unwrap();
        assert_eq!(from_abs.full_path(), "cosn://bucket-125/dir/file");

        let from_uri =
            entry_path_for_bucket("bucket-125", "bucket-125", "cosn://bucket-125/dir/file/").unwrap();
        assert_eq!(from_uri.full_path(), "cosn://bucket-125/dir/file");
    }

    #[test]
    fn entry_path_rewrites_namenode_authority_back_to_short_bucket() {
        let from_nn = entry_path_for_bucket(
            "bigdatahll2",
            "bigdatahll2-1252161381",
            "cosn://bigdatahll2-1252161381/warehouse/file",
        )
        .unwrap();
        assert_eq!(from_nn.full_path(), "cosn://bigdatahll2/warehouse/file");

        let from_abs = entry_path_for_bucket(
            "bigdatahll2",
            "bigdatahll2-1252161381",
            "/warehouse/file",
        )
        .unwrap();
        assert_eq!(from_abs.full_path(), "cosn://bigdatahll2/warehouse/file");
    }

    #[test]
    fn libhdfs_uri_expands_short_bucket_to_namenode() {
        let short = Path::from_str("cosn://bigdatahll2/warehouse/file").unwrap();
        assert_eq!(
            libhdfs_uri(&short, "bigdatahll2", "bigdatahll2-1252161381"),
            "cosn://bigdatahll2-1252161381/warehouse/file"
        );

        let already = Path::from_str("cosn://bigdatahll2-1252161381/warehouse/file").unwrap();
        assert_eq!(
            libhdfs_uri(&already, "bigdatahll2", "bigdatahll2-1252161381"),
            "cosn://bigdatahll2-1252161381/warehouse/file"
        );

        let same = Path::from_str("cosn://bucket-125/a").unwrap();
        assert_eq!(
            libhdfs_uri(&same, "bucket-125", "bucket-125"),
            "cosn://bucket-125/a"
        );
    }

    #[test]
    fn new_rejects_wrong_scheme_before_connect() {
        let path = Path::from_str("s3://bucket/").unwrap();
        let Err(err) = CosnFileSystem::new(&path, HashMap::new()) else {
            panic!("expected scheme validation error");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("Expected scheme") || msg.to_ascii_lowercase().contains("cosn"),
            "{}",
            msg
        );
    }

    #[test]
    fn new_rejects_missing_credentials_before_connect() {
        let path = Path::from_str("cosn://bucket-125/").unwrap();
        let Err(err) = CosnFileSystem::new(&path, HashMap::new()) else {
            panic!("expected missing credential error");
        };
        assert!(
            err.to_string().to_ascii_lowercase().contains("cosn")
                || err.to_string().to_ascii_lowercase().contains("secret"),
            "{}",
            err
        );
    }
}