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

use crate::{err_ufs, S3Conf, UfsUtils};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::{Bytes, BytesMut};
use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileStatus, FileType};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::ByteUnit;
use orpc::err_box;
use orpc::sys::DataSlice;
use tokio::sync::Mutex;

/// S3 asynchronous writer
///
/// Responsible for writing data blocks to S3 storage, supporting streaming writing and segmented upload
/// Use AWS SDK to interact with S3 services to achieve efficient data transmission
#[derive(Debug)]
pub struct S3Writer {
    client: Client,
    path: Path,
    bucket: String,
    key: String,
    status: FileStatus,
    pos: i64,
    buffer: BytesMut,
    buffer_size: usize,
    cancelled: bool,
    upload_id: Option<String>,
    parts: Mutex<Vec<CompletedPart>>,
    part_number: Mutex<i32>,
    upload_inited: Mutex<bool>,
}

impl S3Writer {
    /// Create a new S3 writer
    ///
    /// # Parameters
    /// -`uri`: S3 file URI, format s3://bucket/key
    /// -`config`: Connection configuration, including authentication information, etc.
    ///
    /// # return
    /// Return IoResult<Self>, containing the initialized writer or error
    pub fn new(client: Client, path: &Path, config: &S3Conf) -> FsResult<Self> {
        let (bucket, key) = UfsUtils::get_bucket_key(path)?;

        // Get the buffer size from the configuration, default is 5MB
        let buffer_size = config
            .get("s3.buffer_size")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(|| {
                // If parsing fails, record warnings and use default values
                if config.contains_key("s3.buffer_size") {
                    warn!("Unable to parse s3.buffer_size configuration value, use the default value of 5MB");
                }
                5 * 1024 * 1024
            });

        info!(
            "Created S3Writer for {}, buffer size: {}",
            path,
            ByteUnit::byte_to_string(buffer_size as u64)
        );

        // Prepare a minimal file status for FUSE attribute derivation before upload completes
        let status = FileStatus {
            id: 0,
            path: path.full_path().to_string(),
            name: path.name().to_string(),
            is_dir: false,
            mtime: 0,
            atime: 0,
            children_num: 0,
            is_complete: true,
            len: 0,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: FileType::File,
            x_attr: std::collections::HashMap::new(),
            storage_policy: Default::default(),
            mode: 0o644,
            owner: String::new(),
            group: String::new(),
            nlink: 1,
            target: None,
        };

        Ok(Self {
            client,
            path: path.clone(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            status,
            pos: 0,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            cancelled: false,
            upload_id: None,
            parts: Mutex::new(Vec::new()),
            part_number: Mutex::new(1),
            upload_inited: Mutex::new(false),
        })
    }

    async fn init_upload(&mut self) -> FsResult<()> {
        if self.upload_id.is_none() {
            debug!(
                "Initializing multipart upload for s3://{}/{}",
                self.bucket, self.key
            );

            let resp = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await;

            let resp = match resp {
                Ok(v) => v,
                Err(e) => {
                    return err_ufs!(
                        "S3 create_multipart_upload failed for bucket={}, key={}, error_code={}, details: {:?}",
                        &self.bucket,
                        &self.key,
                        e.code().unwrap_or("Unknown"),
                        DisplayErrorContext(&e)
                    );
                }
            };

            self.upload_id = resp.upload_id.clone();

            debug!("Multipart upload initialized with ID: {:?}", self.upload_id);
        }

        // Added initialization mark
        let mut inited = self.upload_inited.lock().await;
        *inited = true;

        Ok(())
    }

    async fn upload_part(&self, data: Bytes) -> FsResult<CompletedPart> {
        let upload_id = match self.upload_id.as_ref() {
            Some(id) => id,
            None => return err_ufs!("Upload ID not initialized"),
        };

        // Get and add segment numbers
        let mut part_number = self.part_number.lock().await;
        let current_part_number = *part_number;
        *part_number += 1;

        debug!(
            "Uploading part {} for s3://{}/{}, size: {} bytes",
            current_part_number,
            self.bucket,
            self.key,
            data.len()
        );

        let byte_stream = ByteStream::from(data);

        let resp = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .part_number(current_part_number)
            .body(byte_stream)
            .send()
            .await;

        let resp = match resp {
            Ok(v) => v,
            Err(e) => {
                return err_ufs!(
                    "S3 upload_part failed for bucket={}, key={}, part={}, error_code={}, details: {:?}",
                    &self.bucket,
                    &self.key,
                    current_part_number,
                    e.code().unwrap_or("Unknown"),
                    DisplayErrorContext(&e)
                );
            }
        };

        let e_tag = match resp.e_tag {
            Some(v) => v,
            None => {
                return err_ufs!(
                    "S3 upload_part missing ETag for bucket={}, key={}, part={}",
                    &self.bucket,
                    &self.key,
                    current_part_number
                );
            }
        };

        Ok(CompletedPart::builder()
            .part_number(current_part_number)
            .e_tag(e_tag)
            .build())
    }

    async fn complete_upload(&mut self) -> FsResult<()> {
        let upload_id = match self.upload_id.as_ref() {
            Some(v) => v,
            None => {
                return err_ufs!(
                    "S3 upload_part failed: Upload ID not initialized for bucket={}, key={}",
                    &self.bucket,
                    &self.key
                );
            }
        };

        // Get and check the segmented list
        let parts = {
            let parts_guard = self.parts.lock().await;
            if parts_guard.is_empty() {
                return err_box!("No parts uploaded");
            }
            // Clone and sort by segment number
            let mut sorted_parts = parts_guard.clone();
            sorted_parts.sort_by_key(|p| p.part_number());
            sorted_parts
        };

        debug!("Completing multipart upload with {} parts", parts.len());

        // The built segmented upload object
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        // Send a completed segment upload request
        let resp = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await;

        if let Err(e) = resp {
            return err_ufs!(
                "S3 complete_multipart_upload failed for bucket={}, key={}, upload_id={}, error_code={}, details: {:?}",
                &self.bucket,
                &self.key,
                upload_id,
                e.code().unwrap_or("Unknown"),
                DisplayErrorContext(&e)
            );
        }

        info!(
            "Multipart upload completed for s3://{}/{}",
            self.bucket, self.key
        );
        Ok(())
    }

    async fn put_object(&self, data: Bytes) -> FsResult<PutObjectOutput> {
        debug!(
            "Uploading object s3://{}/{}, size: {} bytes",
            self.bucket,
            self.key,
            data.len()
        );

        let byte_stream = ByteStream::from(data);

        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(byte_stream)
            .send()
            .await;

        let resp = match resp {
            Ok(v) => v,
            Err(e) => {
                return err_ufs!(
                    "S3 put_object failed for bucket={}, key={}, error_code={}, details: {:?}",
                    &self.bucket,
                    &self.key,
                    e.code().unwrap_or("Unknown"),
                    DisplayErrorContext(&e)
                );
            }
        };

        info!("Object uploaded to s3://{}/{}", self.bucket, self.key);

        Ok(resp)
    }
}

impl Writer for S3Writer {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    fn chunk_size(&self) -> usize {
        self.buffer_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        if self.cancelled {
            return err_ufs!(
                "S3 write_chunk cancelled for bucket={}, key={}, pos={}",
                &self.bucket,
                &self.key,
                self.pos
            );
        }

        let data_len = chunk.len();
        if self.upload_id.is_none() {
            self.init_upload().await?;
        }

        // Extract the current buffer data
        let bytes_to_upload = match chunk {
            DataSlice::Bytes(v) => v,
            _ => panic!(),
        };

        // Upload segments
        let part = self.upload_part(bytes_to_upload).await?;

        // Record uploaded segments
        let mut parts = self.parts.lock().await;
        parts.push(part);

        Ok(data_len as i64)
    }

    /// Refresh buffered data
    async fn flush(&mut self) -> FsResult<()> {
        if self.cancelled {
            return err_ufs!(
                "S3 flush cancelled for bucket={}, key={}, pos={}, buffer_len={}",
                &self.bucket,
                &self.key,
                self.pos,
                self.buffer.len()
            );
        }

        // Get and clear the buffer
        let bytes_to_upload = self.buffer.split().freeze();
        if bytes_to_upload.is_empty() {
            // Nothing in buffer to upload during flush
            return Ok(());
        }

        // If the segment upload has been initialized, continue to upload the segment
        if self.upload_id.is_some() {
            // Upload segments
            let part = self.upload_part(bytes_to_upload).await?;

            // Record uploaded segments
            let mut parts = self.parts.lock().await;
            parts.push(part);
            drop(parts); // Release the lock

            // Complete segmented upload
            self.complete_upload().await?;
        } else {
            // Upload small files directly
            self.put_object(bytes_to_upload).await?;
        }

        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        // First, flush any pending buffered data
        self.flush().await?;

        // If multipart upload was initiated, ensure we finalize it even when buffer is empty
        if self.upload_id.is_some() {
            // If there are buffered bytes (should be none after flush), upload as last part
            let leftover = self.buffer.split().freeze();
            if !leftover.is_empty() {
                let part = self.upload_part(leftover).await?;
                let mut parts = self.parts.lock().await;
                parts.push(part);
                drop(parts);
            }

            // Complete multipart upload if we have any parts
            self.complete_upload().await?;
            return Ok(());
        }

        // Small object path: if no multipart and we still have data (rare due to flush), put directly
        let leftover = self.buffer.split().freeze();
        if !leftover.is_empty() {
            let _ = self.put_object(leftover).await?;
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        if self.upload_id.is_some() && !self.cancelled {
            // To be safe, if there are still unfinished uploads when discarding, try to abort the upload.
            warn!("S3AsyncChunkWriter dropped without proper closing, trying to abort upload");

            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let upload_id = self.upload_id.clone();

            if let Some(upload_id) = upload_id {
                let res = client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await;

                if let Err(e) = res {
                    return err_ufs!(
                        "S3 abort_multipart_upload failed for bucket={}, key={}, upload_id={}, error_code={}, details: {:?}",
                        &bucket,
                        &key,
                        &upload_id,
                        e.code().unwrap_or("Unknown"),
                        DisplayErrorContext(&e)
                    );
                }
            }
        }

        Ok(())
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.pos != pos {
            err_box!("not support random write")
        } else {
            Ok(())
        }
    }
}

impl Drop for S3Writer {
    fn drop(&mut self) {
        info!("Close S3Writer for {}", self.path);
    }
}
