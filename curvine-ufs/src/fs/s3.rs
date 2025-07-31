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

#![allow(unused)]

use crate::fs::aws_utils;
use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use async_trait::async_trait;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::{Bytes, BytesMut};
use curvine_common::fs::CurvineURI;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result as IoResult};
use std::sync::Arc;
use tokio::sync::Mutex;

/// S3 asynchronous block reader
///
/// Responsible for reading data blocks from S3 storage, supporting streaming reading and random access
/// Use AWS SDK to interact with S3 services to achieve efficient data transmission
pub struct S3AsyncChunkReader {
    /// S3 Client
    client: Arc<Client>,
    /// Bucket name
    bucket: String,
    /// Object key name
    key: String,
    /// Total file size
    content_length: u64,
    mtime: i64,
    /// Current read location
    current_offset: u64,
    /// Data flow
    byte_stream: Option<ByteStream>,
    /// Canceled
    cancelled: bool,
}
impl S3AsyncChunkReader {
    ///Create a new S3 reader
    ///
    ///# Parameters
    ///-`uri`: S3 file URI, format s3://bucket/key
    ///-`config`: Connection configuration, including authentication information, etc.
    ///
    ///# return
    ///Return IoResult<Self>, containing the initialized reader or error
    pub async fn new(uri: &CurvineURI, config: &HashMap<String, String>) -> IoResult<Self> {
        let bucket = uri
            .authority()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "S3 URI missing bucket name"))?;
        let key = uri
            .path()
            .strip_prefix('/')
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Invalid S3 path format"))?;

        debug!(
            "Create S3AsyncChunkReader in bucket: {}, prefix: {}",
            bucket, key
        );

        // Create an S3 client with aws_utils
        let client = aws_utils::create_s3_client(config)
            .map_err(|e| Error::other(format!("Failed to create S3 client: {}", e)))?;
        use aws_sdk_s3::error::ProvideErrorMetadata;
        // Get object metadata
        let head_result = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let error_msg = format!(
                    "S3 error: {}, details: {}",
                    e.code().unwrap_or("Unknown"),
                    e
                );
                Error::other(error_msg)
            })?;

        let content_length = head_result
            .content_length
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Missing content length"))?
            as u64;

        let mtime = head_result
            .last_modified
            .map(|x| x.to_millis().unwrap_or(0))
            .unwrap_or(0);

        debug!(
            "Created S3 reader for s3://{}/{}, content length: {}",
            bucket, key, content_length
        );

        Ok(Self {
            client,
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_length,
            mtime,
            current_offset: 0,
            byte_stream: None,
            cancelled: false,
        })
    }

    /// Initialize the data flow
    async fn init_stream(&mut self) -> IoResult<()> {
        if self.byte_stream.is_none() {
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await
                .map_err(|e| Error::other(format!("Failed to get object: {}", e)))?;

            self.byte_stream = Some(resp.body);
        }
        Ok(())
    }

    async fn list_directory(&self, recursive: bool) -> IoResult<Vec<String>> {
        let mut entries = Vec::new();
        let mut continuation_token = None;

        // Construct the basic path
        let prefix = if self.key.ends_with('/') {
            self.key.clone()
        } else {
            format!("{}/", self.key)
        };

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);

            // Set the separator only in non-recursive mode
            if !recursive {
                req = req.delimiter("/");
            }

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| Error::other(format!("Failed to list objects: {}", e)))?;

            // Handle ordinary objects
            if let Some(ref objects) = resp.contents {
                for obj in objects {
                    if let Some(key) = &obj.key {
                        if key != &prefix {
                            entries.push(format!("s3://{}/{}", self.bucket, key));
                        }
                    }
                }
            }

            // Handling common prefixes (subdirectories) in non-recursive mode
            if !recursive {
                if let Some(ref prefixes) = resp.common_prefixes {
                    for prefix_obj in prefixes {
                        if let Some(prefix_str) = &prefix_obj.prefix {
                            entries.push(format!("s3://{}/{}", self.bucket, prefix_str));
                        }
                    }
                }
            }

            // Check if there are more results
            match resp.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(entries)
    }
}

#[async_trait]
impl AsyncChunkReader for S3AsyncChunkReader {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize> {
        // Initialize the stream (if not initialized)
        if self.byte_stream.is_none() {
            self.init_stream().await?;
        }

        if let Some(stream) = &mut self.byte_stream {
            // Use the next() method to get the next block of data
            if let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        let bytes_read = chunk.len();
                        buf.extend_from_slice(&chunk);
                        self.current_offset += bytes_read as u64;
                        Ok(bytes_read)
                    }
                    Err(e) => Err(Error::other(format!("Failed to read chunk: {}", e))),
                }
            } else {
                // The stream has ended
                self.byte_stream = None;
                Ok(0)
            }
        } else {
            Err(Error::other("Stream not initialized"))
        }
    }

    fn content_length(&self) -> u64 {
        self.content_length
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes> {
        // Check if the offset is out of file scope
        if offset >= self.content_length {
            return Ok(Bytes::new());
        }

        // If the requested offset is different from the current offset, the stream needs to be reinitialized
        if offset != self.current_offset {
            // Close existing streams
            self.byte_stream = None;

            // Set a new offset
            self.current_offset = offset;

            // Create a new scope request
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .range(format!("bytes={}-", offset))
                .send()
                .await
                .map_err(|e| Error::other(format!("Failed to get object range: {}", e)))?;

            self.byte_stream = Some(resp.body);
        }

        // Calculate the actual readable length
        let remaining = self.content_length - offset;
        let read_length = std::cmp::min(length, remaining);

        // Prepare the buffer
        let mut buffer = BytesMut::with_capacity(read_length as usize);

        // Read data of a specified length
        let mut total_read = 0;
        while total_read < read_length as usize {
            let bytes_to_read = read_length as usize - total_read;
            let mut temp_buf = BytesMut::with_capacity(bytes_to_read);

            match self.read_chunk(&mut temp_buf).await {
                Ok(0) => break, // Read completed
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[..n]);
                    total_read += n;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(buffer.freeze())
    }
}

///S3 asynchronous block writer
///
///Responsible for writing data blocks to S3 storage, supporting streaming writing and segmented upload
///Use AWS SDK to interact with S3 services to achieve efficient data transmission
pub struct S3AsyncChunkWriter {
    /// S3 Client
    client: Arc<Client>,
    /// Bucket name
    bucket: String,
    /// Object key name
    key: String,
    /// Number of bytes written
    written_bytes: u64,
    /// Buffer
    buffer: Mutex<BytesMut>,
    /// Buffer size, it will automatically refresh to S3 if it exceeds this size.
    buffer_size: usize,
    /// Canceled
    cancelled: bool,
    /// Segment upload ID
    upload_id: Option<String>,
    /// Uploaded segment list
    parts: Mutex<Vec<CompletedPart>>,
    /// Current segment number
    part_number: Mutex<i32>,
    /// Added fields to mark whether initialization has been completed
    upload_inited: Mutex<bool>,
}

impl S3AsyncChunkWriter {
    ///Create a new S3 writer
    ///
    ///# Parameters
    ///-`uri`: S3 file URI, format s3://bucket/key
    ///-`config`: Connection configuration, including authentication information, etc.
    ///
    ///# return
    ///Return IoResult<Self>, containing the initialized writer or error
    pub fn new(uri: &str, config: &HashMap<String, String>) -> IoResult<Self> {
        // Parse URI
        let curvine_uri = CurvineURI::new(uri).unwrap();

        // Verify scheme
        if curvine_uri.scheme() != Some("s3") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "URI scheme must be 's3'",
            ));
        }

        // Extract bucket name and key name
        let path = curvine_uri.path().trim_start_matches('/');
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() < 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid S3 URI format, expected s3://bucket/key",
            ));
        }

        let bucket = parts[0].to_string();
        let key = parts[1].to_string();

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

        // Create an S3 client with aws_utils
        let client = aws_utils::create_s3_client(config)
            .map_err(|e| Error::other(format!("Failed to create S3 client: {}", e)))?;

        debug!(
            "Created S3 writer for s3://{}/{}, buffer size: {} bytes",
            bucket, key, buffer_size
        );

        Ok(Self {
            client,
            bucket,
            key,
            written_bytes: 0,
            buffer: Mutex::new(BytesMut::with_capacity(buffer_size)),
            buffer_size,
            cancelled: false,
            upload_id: None,
            parts: Mutex::new(Vec::new()),
            part_number: Mutex::new(1),
            upload_inited: Mutex::new(false),
        })
    }

    async fn init_upload(&mut self) -> IoResult<()> {
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
                .await
                .map_err(|e| {
                    error!("Failed to initialize multipart upload: {}", e);
                    Error::other(format!("Failed to initialize multipart upload: {}", e))
                })?;

            self.upload_id = resp.upload_id.clone();

            debug!("Multipart upload initialized with ID: {:?}", self.upload_id);
        }
        // Added initialization mark
        let mut inited = self.upload_inited.lock().await;
        *inited = true;
        Ok(())
    }

    async fn upload_part(&self, data: Bytes) -> IoResult<CompletedPart> {
        let upload_id = self
            .upload_id
            .as_ref()
            .ok_or_else(|| Error::other("Upload ID not initialized"))?;

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
            .await
            .map_err(|e| {
                error!("Failed to upload part {}: {}", current_part_number, e);
                Error::other(format!("Failed to upload part: {}", e))
            })?;

        let e_tag = resp
            .e_tag
            .ok_or_else(|| Error::other("ETag not returned from upload part"))?;

        Ok(CompletedPart::builder()
            .part_number(current_part_number)
            .e_tag(e_tag)
            .build())
    }

    async fn complete_upload(&self) -> IoResult<()> {
        let upload_id = self
            .upload_id
            .as_ref()
            .ok_or_else(|| Error::other("Upload ID not initialized"))?;

        // Get and check the segmented list
        let parts = {
            let parts_guard = self.parts.lock().await;
            if parts_guard.is_empty() {
                return Err(Error::other("No parts uploaded"));
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
            .await
            .map_err(|e| {
                error!("Failed to complete multipart upload: {}", e);
                Error::other(format!("Failed to complete multipart upload: {}", e))
            })?;

        info!(
            "Multipart upload completed for s3://{}/{}",
            self.bucket, self.key
        );
        Ok(())
    }

    async fn abort_upload(&self) -> IoResult<()> {
        if let Some(upload_id) = &self.upload_id {
            debug!(
                "Aborting multipart upload for s3://{}/{}",
                self.bucket, self.key
            );

            self.client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(upload_id)
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to abort multipart upload: {}", e);
                    Error::other(format!("Failed to abort multipart upload: {}", e))
                })?;

            info!(
                "Multipart upload aborted for s3://{}/{}",
                self.bucket, self.key
            );
        }

        Ok(())
    }

    async fn put_object(&self, data: Bytes) -> IoResult<PutObjectOutput> {
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
            .await
            .map_err(|e| {
                error!("Failed to upload object: {}", e);
                Error::other(format!("Failed to upload object: {}", e))
            })?;

        info!("Object uploaded to s3://{}/{}", self.bucket, self.key);

        Ok(resp)
    }

    /// Close the writer
    async fn close(&mut self) -> IoResult<()> {
        if !self.cancelled {
            self.flush().await?;
        } else if self.upload_id.is_some() {
            // If there is a cancellation and there is an unfinished upload, abort the upload
            self.abort_upload().await?;
        }

        Ok(())
    }

    /// Cancel write
    fn cancel(&mut self) {
        self.cancelled = true;

        // Note: The actual cancel operation is completed in the close method
        debug!("S3 write cancelled for s3://{}/{}", self.bucket, self.key);
    }

    /// Get the number of bytes written
    fn written_bytes(&self) -> u64 {
        self.written_bytes
    }
}

#[async_trait]
impl AsyncChunkWriter for S3AsyncChunkWriter {
    /// Write data blocks
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        if self.cancelled {
            return Err(Error::new(ErrorKind::Interrupted, "Write cancelled"));
        }

        let data_len = data.len();

        // Add data to the buffer
        let mut buffer = self.buffer.lock().await;
        buffer.extend_from_slice(&data);

        // If the buffer exceeds the threshold, refresh the data
        if buffer.len() >= self.buffer_size {
            if self.upload_id.is_none() {
                // Initialize segment upload
                drop(buffer); // Release the lock
                self.init_upload().await?;
                buffer = self.buffer.lock().await; // Reacquire the lock
            }

            // Extract the current buffer data
            let bytes_to_upload = buffer.split().freeze();
            drop(buffer); // Release the lock

            // Upload segments
            let part = self.upload_part(bytes_to_upload).await?;

            // Record uploaded segments
            let mut parts = self.parts.lock().await;
            parts.push(part);
        }

        self.written_bytes += data_len as u64;
        Ok(())
    }

    /// Refresh buffered data
    async fn flush(&mut self) -> IoResult<()> {
        if self.cancelled {
            return Err(Error::new(ErrorKind::Interrupted, "Flush cancelled"));
        }

        // Get and clear the buffer
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let bytes_to_upload = buffer.split().freeze();
        drop(buffer); // Release the lock

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
}

impl Drop for S3AsyncChunkWriter {
    fn drop(&mut self) {
        if self.upload_id.is_some() && !self.cancelled {
            // To be safe, if there are still unfinished uploads when discarding, try to abort the upload.
            warn!("S3AsyncChunkWriter dropped without proper closing, trying to abort upload");

            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let upload_id = self.upload_id.clone();

            if let Some(upload_id) = upload_id {
                tokio::spawn(async move {
                    match client
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .send()
                        .await
                    {
                        Ok(_) => debug!("Successfully aborted multipart upload in drop handler"),
                        Err(e) => error!("Failed to abort multipart upload in drop handler: {}", e),
                    }
                });
            }
        }
    }
}
