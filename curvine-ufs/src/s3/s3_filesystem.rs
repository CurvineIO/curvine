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

use crate::fs::aws_utils;
use crate::s3::{ObjectStatus, S3Reader, S3Writer, FOLDER_SUFFIX, SCHEME};
use crate::{err_ufs, UfsUtils};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::Client;
use curvine_common::conf::UfsConf;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{FileStatus, SetAttrOpts};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use std::sync::Arc;

/// S3 file system implementation
#[derive(Clone)]
pub struct S3FileSystem {
    client: Client,
    conf: Arc<UfsConf>,
}

impl S3FileSystem {
    pub fn new(conf: UfsConf) -> FsResult<Self> {
        let client = match aws_utils::create_s3_client(conf.get_config()) {
            Ok(v) => v,
            Err(e) => return err_ufs!("Failed to create S3 client: {}", e),
        };
        let client = (*client).clone();
        Ok(S3FileSystem {
            client,
            conf: Arc::new(conf),
        })
    }

    async fn get_object_status(&self, bucket: &str, key: &str) -> FsResult<Option<ObjectStatus>> {
        let res = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match res {
            Ok(v) => Ok(Some(ObjectStatus::from_head_object(key, &v))),
            Err(e) => match e.into_service_error() {
                HeadObjectError::NotFound(_) => Ok(None),
                err => err_ufs!(err),
            },
        }
    }

    async fn list_object_status(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> FsResult<Option<ObjectStatus>> {
        let res = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .delimiter(FOLDER_SUFFIX)
            .max_keys(1)
            .send()
            .await;
        match res {
            Ok(v) => {
                if let Some(prefixs) = &v.common_prefixes {
                    for prefix_val in prefixs {
                        if let Some(prefix_str) = &prefix_val.prefix {
                            if prefix_str.starts_with(prefix) {
                                return Ok(Some(ObjectStatus::new(
                                    prefix,
                                    0,
                                    LocalTime::mills() as i64,
                                    true,
                                )));
                            }
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => err_ufs!(e),
        }
    }

    // Get file and directory information; there are 4 cases:
    // 1. Root dir is returned directly
    // 2. First determine whether the file exists, and return if it exists.
    // 3. If it does not exist, determine whether the directory exists, and return if it exists.
    // 4. If neither exists, return None
    async fn get_file_status(&self, path: &Path) -> FsResult<Option<FileStatus>> {
        let status = if path.is_root() {
            Some(ObjectStatus::create_root())
        } else {
            let (bucket, key) = UfsUtils::get_bucket_key(path)?;
            let status = self.get_object_status(bucket, key).await?;

            match status {
                None => {
                    let status1 = self
                        .get_object_status(bucket, &UfsUtils::dir_key(key))
                        .await?;
                    match status1 {
                        None => self.list_object_status(bucket, key).await?,
                        Some(v) => Some(v),
                    }
                }

                Some(v) => Some(v),
            }
        };

        match status {
            None => Ok(None),
            Some(v) => {
                let status = FileStatus {
                    path: path.full_path().to_owned(),
                    name: path.name().to_owned(),
                    is_dir: v.is_dir,
                    mtime: v.mtime,
                    is_complete: true,
                    len: v.len,
                    replicas: 1,
                    block_size: 4 * 1024 * 1024,
                    ..Default::default()
                };
                Ok(Some(status))
            }
        }
    }

    async fn mkdir0(&self, bucket: &str, key: &str) -> FsResult<bool> {
        let status = self.get_object_status(bucket, key).await?;
        match status {
            Some(v) if v.is_file() => {
                return err_ufs!(
                    "Cannot create directory /{} because it is already a file",
                    key
                );
            }
            Some(_) => return Ok(true),
            None => (),
        }

        let res = self
            .client
            .put_object()
            .bucket(bucket)
            .key(UfsUtils::dir_key(key))
            .content_length(0)
            .body(bytes::Bytes::new().into())
            .send()
            .await;

        match res {
            Err(e) => err_ufs!(e),
            Ok(_) => Ok(true),
        }
    }

    pub async fn is_dir(&self, path: &Path) -> FsResult<bool> {
        // root dir
        if path.is_root() {
            return Ok(true);
        }

        let (bucket, key) = UfsUtils::get_bucket_key(path)?;
        let status = self.get_object_status(bucket, key).await?;
        match status {
            Some(v) if v.is_dir() => Ok(true),
            _ => Ok(false),
        }
    }

    pub fn obj_to_status(bucket: &str, obj: &Object) -> FsResult<FileStatus> {
        let sub_key = obj.key().unwrap_or("");
        let path = Path::from_str(format!("{}{}{}{}", SCHEME, bucket, FOLDER_SUFFIX, sub_key))?;
        let is_dir = sub_key.ends_with(FOLDER_SUFFIX);

        let mtime = obj
            .last_modified
            .map(|x| x.to_millis().unwrap_or(0))
            .unwrap_or(0);
        let len = obj.size.unwrap_or(0);

        let status = FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir,
            mtime,
            atime: mtime,
            is_complete: true,
            len,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            ..Default::default()
        };

        Ok(status)
    }

    pub fn prefix_to_status(bucket: &str, prefix: &str) -> FsResult<FileStatus> {
        let path = Path::from_str(format!("{}{}{}{}", SCHEME, bucket, FOLDER_SUFFIX, prefix))?;
        let status = FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: true,
            is_complete: true,
            ..Default::default()
        };

        Ok(status)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> FsResult<()> {
        let res = self
            .client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        match res {
            Err(e) => err_ufs!(e),
            Ok(_) => Ok(()),
        }
    }
}

impl FileSystem<S3Writer, S3Reader, UfsConf> for S3FileSystem {
    fn conf(&self) -> &UfsConf {
        &self.conf
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let (bucket, key) = UfsUtils::get_bucket_key(path)?;

        if create_parent {
            let components: Vec<&str> = key.split(FOLDER_SUFFIX).collect();
            let mut current_path = String::new();
            for component in components {
                if component.is_empty() {
                    continue;
                }

                if !current_path.is_empty() {
                    current_path.push_str(FOLDER_SUFFIX);
                }
                current_path.push_str(component);

                self.mkdir0(bucket, &current_path).await?;
            }
            Ok(true)
        } else {
            // Check if the parent directory exists
            if let Some(parent) = path.parent()? {
                if !self.is_dir(&parent).await? {
                    return err_ufs!("Parent directory /{} does not exist", parent);
                }
            }
            self.mkdir0(bucket, key).await?;
            Ok(true)
        }
    }

    async fn create(&self, path: &Path, _overwrite: bool) -> FsResult<S3Writer> {
        if let Some(parent) = path.parent()? {
            self.mkdir(&parent, true).await?;
        }
        let writer = S3Writer::new(self.client.clone(), path, self.conf())?;
        Ok(writer)
    }

    async fn append(&self, _path: &Path) -> FsResult<S3Writer> {
        err_ufs!("Not supported")
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        Ok(self.get_file_status(path).await?.is_some())
    }

    async fn open(&self, path: &Path) -> FsResult<S3Reader> {
        let reader = S3Reader::new(self.client.clone(), path).await?;
        Ok(reader)
    }

    async fn rename(&self, _src: &Path, _dst: &Path) -> FsResult<bool> {
        err_ufs!("Not supported")
    }

    async fn delete(&self, path: &Path, _recursive: bool) -> FsResult<()> {
        if path.is_root() {
            return err_ufs!("Cannot delete root directory");
        }

        let (bucket, key) = UfsUtils::get_bucket_key(path)?;
        let status = self.get_object_status(bucket, key).await?;

        match status {
            None => err_ufs!("Path not exists: {}", path),
            Some(v) if v.is_file() => {
                // delete file
                self.delete_object(bucket, key).await
            }
            Some(_) => {
                // delete dir
                // @todo Support recursive directory deletion
                let list = self.list_status(path).await?;
                if list.is_empty() {
                    self.delete_object(bucket, &UfsUtils::dir_key(key)).await
                } else {
                    err_ufs!("Directory not empty")
                }
            }
        }
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let res = self.get_file_status(path).await?;
        match res {
            None => err_ufs!("Path {} not found", path.full_path()),
            Some(v) => Ok(v),
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let (bucket, key) = UfsUtils::get_bucket_key(path)?;
        let dir_key = UfsUtils::dir_key(key);
        let mut entries = Vec::new();
        let req = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter(FOLDER_SUFFIX)
            .prefix(&dir_key);

        let resp = match req.send().await {
            Err(e) => return err_ufs!(e),
            Ok(v) => v,
        };

        // case 1: Process files and directories ending with FOLDER_SUFFIX
        // file: ufs/a -> a
        // dir: ufs/dir/1 -> dir/
        if let Some(objects) = &resp.contents {
            for obj in objects {
                let sub_key = obj.key().unwrap_or("").to_owned();
                if dir_key == sub_key {
                    continue;
                }
                let status = Self::obj_to_status(bucket, obj)?;
                entries.push(status);
            }
        }

        // case 2: Common prefix matching, all are considered directories.
        if let Some(prefixes) = &resp.common_prefixes {
            for prefix in prefixes {
                if let Some(prefix_str) = &prefix.prefix {
                    if prefix_str.starts_with(&dir_key) {
                        let status = Self::prefix_to_status(bucket, prefix_str)?;
                        entries.push(status);
                    }
                }
            }
        }

        Ok(entries)
    }

    async fn set_attr(&self, _path: &Path, _opts: SetAttrOpts) -> FsResult<()> {
        err_ufs!("SetAttr operation is not supported by S3 file system")
    }
}
