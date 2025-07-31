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
use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use crate::fs::filesystem::FileSystem;
use crate::fs::s3::{S3AsyncChunkReader, S3AsyncChunkWriter};
use crate::fs::ufs_context::UFSContext;
use crate::s3::ObjectStatus;
use crate::{err_ufs, UfsUtils};
use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::Client;
use curvine_common::error::FsError;
use curvine_common::fs::CurvineURI;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use std::io::Error;
use std::sync::Arc;

/// S3 file system implementation
#[derive(Clone)]
pub struct S3FileSystem {
    client: Arc<Client>, //S3Client
    context: Arc<UFSContext>,
}

impl S3FileSystem {
    pub fn new(context: Arc<UFSContext>) -> FsResult<Self> {
        let client = aws_utils::create_s3_client(context.s3a_config()).map_err(|e| {
            // Convert S3 client creation error to file system error
            FsError::common(format!("Failed to create S3 client: {}", e))
        })?;

        Ok(S3FileSystem { client, context })
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

    // Get file and directory information; there are 4 cases:
    // 1. Root dir is returned directly
    // 2. First determine whether the file exists, and return if it exists.
    // 3. If it does not exist, determine whether the directory exists, and return if it exists.
    // 4. If neither exists, return None
    async fn get_file_status(&self, path: &CurvineURI) -> FsResult<Option<FileStatus>> {
        let status = if path.is_root() {
            Some(ObjectStatus::create_root())
        } else {
            let (bucket, key) = UfsUtils::get_bucket_key(path)?;
            let status = self.get_object_status(bucket, key).await?;

            match status {
                None => {
                    self.get_object_status(bucket, &UfsUtils::dir_key(key))
                        .await?
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
}

#[async_trait]
impl FileSystem for S3FileSystem {
    async fn open(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkReader>> {
        let reader = S3AsyncChunkReader::new(uri, self.context.s3a_config()).await?;
        Ok(Box::new(reader))
    }

    async fn is_directory(&self, uri: &CurvineURI) -> FsResult<bool> {
        let bucket = uri
            .authority()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Invalid S3 path format"))?
            .to_string();

        // Scenario 1: Explicit directory judgment (ending with)
        if key.ends_with('/') {
            return Ok(true);
        }
        // Scenario 2: Check for the presence of a file with the same name (prevent the path from being both a file and a directory prefix)
        let exists_as_file = self.exists(uri).await?;
        if exists_as_file {
            return Ok(false);
        }
        // Scenario 3: Implicit directory judgment (with sub-objects or public prefixes)
        let resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(format!("{}/", key))
            .max_keys(1)
            .send()
            .await
            .map_err(|e| {
                // Convert an AWS SDK error to a file system error
                let error_msg = format!("The S3 list operation failed: {}", e);
                Error::other(error_msg)
            })?;

        Ok(!resp.contents().is_empty() || !resp.common_prefixes().is_empty())
    }

    async fn list_directory(&self, uri: &CurvineURI, recursive: bool) -> FsResult<Vec<String>> {
        let bucket = uri
            .authority()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Invalid S3 path format"))?;

        println!("Listing objects in bucket: {}, prefix: {}", bucket, key,);

        let mut entries = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(key);

            // The delimiter is set only in non-recursive mode
            if !recursive {
                req = req.delimiter("/");
            }

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.map_err(|e| Error::other(e.to_string()))?;

            if let Some(ref objects) = resp.contents {
                for obj in objects {
                    if let Some(key) = &obj.key {
                        entries.push(format!("s3://{}/{}", bucket, key));
                    }
                }
            }

            if let Some(ref prefixes) = resp.common_prefixes {
                for prefix in prefixes {
                    if let Some(prefix_str) = &prefix.prefix {
                        entries.push(format!("s3://{}/{}", bucket, prefix_str));
                    }
                }
            }

            match resp.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(entries)
    }

    async fn exists(&self, uri: &CurvineURI) -> FsResult<bool> {
        let bucket = uri
            .authority()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Invalid S3 path format"))?;

        let result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        Ok(result.is_ok())
    }

    async fn create_directory(&self, uri: &CurvineURI) -> FsResult<()> {
        let bucket = uri
            .authority()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Invalid S3 path format"))?
            .to_string();

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(bytes::Bytes::new().into())
            .send()
            .await
            .map_err(|e| Error::other(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, uri: &CurvineURI, recursive: bool) -> FsResult<()> {
        if uri.scheme() != Some("s3") {
            return Err(FsError::common("URI scheme must be 's3'"));
        }

        let bucket = uri
            .authority()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Invalid S3 path format"))?
            .to_string();

        if recursive && (key.ends_with('/') || self.is_directory(uri).await?) {
            let mut continuation_token = None;

            loop {
                let mut req = self.client.list_objects_v2().bucket(bucket).prefix(&key);

                if let Some(token) = continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req.send().await.map_err(|e| Error::other(e.to_string()))?;

                if let Some(ref objects) = resp.contents {
                    for obj in objects {
                        if let Some(key) = &obj.key {
                            self.client
                                .delete_object()
                                .bucket(bucket)
                                .key(key)
                                .send()
                                .await
                                .map_err(|e| Error::other(e.to_string()))?;
                        }
                    }
                }

                match resp.next_continuation_token() {
                    Some(token) => continuation_token = Some(token.to_string()),
                    None => break,
                }
            }
        } else {
            self.client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| Error::other(e.to_string()))?;
        }

        Ok(())
    }

    async fn rename(&self, src_uri: &CurvineURI, dst_uri: &CurvineURI) -> FsResult<()> {
        if src_uri.scheme() != Some("s3") || dst_uri.scheme() != Some("s3") {
            return Err(FsError::invalid_path(
                src_uri.full_path(),
                "URI scheme must be 's3'",
            ));
        }

        // Extract the source and destination paths
        let src_path = src_uri.path().trim_start_matches('/');
        let dst_path = dst_uri.path().trim_start_matches('/');

        // Parse buckets and keys of the source and destination paths
        let src_parts: Vec<&str> = src_path.splitn(2, '/').collect();
        let dst_parts: Vec<&str> = dst_path.splitn(2, '/').collect();

        // Verify the path format
        if src_parts.len() < 2 || dst_parts.len() < 2 {
            return Err(FsError::invalid_path(src_path, "Invalid S3 URI format"));
        }

        // Securely obtain buckets and keys
        let src_bucket = src_uri.authority().ok_or_else(|| {
            FsError::invalid_path(
                src_uri.full_path(),
                "The source S3 URI is missing a bucket name",
            )
        })?;
        let src_key = src_parts[1];
        let dst_bucket = dst_uri.authority().ok_or_else(|| {
            FsError::invalid_path(
                dst_uri.full_path(),
                "The destination S3 URI is missing a bucket name",
            )
        })?;
        let dst_key = dst_parts[1];

        self.client
            .copy_object()
            .copy_source(format!("{}/{}", src_bucket, src_key))
            .bucket(dst_bucket)
            .key(dst_key)
            .send()
            .await
            .map_err(|e| Error::other(e.to_string()))?;

        self.client
            .delete_object()
            .bucket(src_bucket)
            .key(src_key)
            .send()
            .await
            .map_err(|e| Error::other(e.to_string()))?;

        Ok(())
    }

    async fn create(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkWriter>> {
        let writer = S3AsyncChunkWriter::new(uri.path(), self.context.s3a_config())?;
        Ok(Box::new(writer))
    }

    async fn mount(&self, _src: &CurvineURI, _dst: &CurvineURI) -> FsResult<()> {
        Err(FsError::common("not implement for s3"))
    }

    async fn get_file_status(&self, path: &CurvineURI) -> FsResult<Option<FileStatus>> {
        self.get_file_status(path).await
    }
}
