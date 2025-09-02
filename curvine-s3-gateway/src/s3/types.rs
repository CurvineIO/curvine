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

//! S3 types and helper functions for upload operations

use crate::utils::io::PollRead;
use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
use curvine_common::fs::{FileSystem, Path, Writer};
use curvine_common::FsResult;
use orpc::runtime::AsyncRuntime;
use std::sync::Arc;
use tracing;

#[derive(Debug, Default, Clone)]
pub struct UploadStats {
    pub total_written: u64,
    pub chunks_processed: u32,
    pub first_chunk_logged: bool,
}

impl UploadStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_written(&mut self, bytes: u64) {
        self.total_written += bytes;
        self.chunks_processed += 1;
    }

    pub fn mark_first_chunk_logged(&mut self) {
        self.first_chunk_logged = true;
    }

    pub fn should_log_first_chunk(&self) -> bool {
        !self.first_chunk_logged
    }
}

/// S3 PUT operation context containing all necessary resources
pub struct PutContext {
    pub fs: UnifiedFileSystem,
    pub rt: Arc<AsyncRuntime>,
    pub bucket: String,
    pub object: String,
    pub path: FsResult<Path>,
}

impl PutContext {
    /// Create new PUT operation context
    pub fn new(
        fs: UnifiedFileSystem,
        rt: Arc<AsyncRuntime>,
        bucket: String,
        object: String,
        path: FsResult<Path>,
    ) -> Self {
        Self {
            fs,
            rt,
            bucket,
            object,
            path,
        }
    }

    /// Log start of PUT operation
    pub fn log_start(&self) {
        tracing::info!("PUT object s3://{}/{}", self.bucket, self.object);
    }

    /// Log completion of PUT operation
    pub fn log_completion(&self, stats: &UploadStats) {
        tracing::info!(
            "PUT object s3://{}/{} completed, total bytes: {}",
            self.bucket,
            self.object,
            stats.total_written
        );
    }

    /// Convert path with error handling
    pub fn get_validated_path(&self) -> Result<Path, String> {
        self.path.as_ref().map(|p| p.clone()).map_err(|e| {
            tracing::error!(
                "Failed to convert S3 path s3://{}/{}: {}",
                self.bucket,
                self.object,
                e
            );
            e.to_string()
        })
    }

    /// Create file writer with error handling
    pub async fn create_writer(&self) -> Result<UnifiedWriter, String> {
        let path = self.get_validated_path()?;
        self.fs.create(&path, true).await.map_err(|e| {
            tracing::error!("Failed to create file at path {}: {}", path, e);
            e.to_string()
        })
    }
}

/// Chunk processor for handling data chunks during upload
pub struct ChunkProcessor;

impl ChunkProcessor {
    /// Process a data chunk with logging and validation
    pub fn process_chunk(chunk: &[u8], stats: &mut UploadStats) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }

        let n = chunk.len();
        log::debug!("POLLREAD-CHUNK: {n} bytes");

        // Log hex dump for debugging
        if n > 0 {
            log::debug!(
                "POLLREAD-HEX: {}",
                chunk
                    .iter()
                    .take(30)
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }

        // Log first chunk details for debugging
        if stats.should_log_first_chunk() {
            log::debug!("PUT DEBUG - First chunk: {n} bytes");
            log::debug!(
                "PUT DEBUG - First chunk hex: {}",
                chunk[..n.min(64)]
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            stats.mark_first_chunk_logged();
        }

        stats.add_written(n as u64);
        tracing::debug!(
            "Written chunk: {} bytes, total: {} bytes",
            n,
            stats.total_written
        );

        Ok(())
    }
}

/// Stream reader for handling input data stream
pub struct StreamReader;

impl StreamReader {
    /// Read next chunk from input stream
    pub async fn read_next_chunk(
        body: &mut (dyn PollRead + Unpin + Send),
    ) -> Result<Option<Vec<u8>>, String> {
        let chunk_result = body.poll_read().await.map_err(|e| {
            tracing::error!("Failed to read from input stream: {}", e);
            e.to_string()
        })?;

        match chunk_result {
            Some(data) if !data.is_empty() => Ok(Some(data)),
            Some(_) | None => Ok(None), // Empty chunk or end of stream
        }
    }
}

/// Writer helper for handling file writing operations
pub struct WriterHelper;

impl WriterHelper {
    /// Write chunk to file with error handling
    pub async fn write_chunk(writer: &mut UnifiedWriter, chunk: &[u8]) -> Result<(), String> {
        writer.write(chunk).await.map_err(|e| {
            tracing::error!("Failed to write chunk to file: {}", e);
            e.to_string()
        })
    }

    /// Complete file writing with error handling
    pub async fn complete_write(writer: &mut UnifiedWriter) -> Result<(), String> {
        writer.complete().await.map_err(|e| {
            tracing::error!("Failed to complete file write: {}", e);
            e.to_string()
        })
    }
}

/// Main PUT operation orchestrator
pub struct PutOperation;

impl PutOperation {
    /// Execute complete PUT operation
    pub async fn execute(
        context: PutContext,
        body: &mut (dyn PollRead + Unpin + Send),
    ) -> Result<(), String> {
        context.log_start();

        let mut writer = context.create_writer().await?;
        let mut stats = UploadStats::new();

        // Main upload loop
        loop {
            let chunk = match StreamReader::read_next_chunk(body).await? {
                Some(data) => data,
                None => break, // End of stream
            };

            ChunkProcessor::process_chunk(&chunk, &mut stats)?;
            WriterHelper::write_chunk(&mut writer, &chunk).await?;
        }

        WriterHelper::complete_write(&mut writer).await?;
        context.log_completion(&stats);

        Ok(())
    }
}

/// HTTP Range header representation (similar to s3s implementation)
///
/// Supports standard HTTP range requests as defined in RFC 9110
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    Int { first: u64, last: Option<u64> },
    Suffix { length: u64 },
}

impl Range {
    pub fn parse(header: &str) -> Result<Self, String> {
        let header = header.trim();

        let range_spec = header
            .strip_prefix("bytes=")
            .ok_or("Range header must start with 'bytes='")?;

        if let Some(suffix_str) = range_spec.strip_prefix('-') {
            let length = suffix_str
                .parse::<u64>()
                .map_err(|_| format!("Invalid suffix length: {suffix_str}"))?;
            if length == 0 {
                return orpc::err_box!("Suffix length cannot be zero");
            }
            return Ok(Range::Suffix { length });
        }

        let parts: Vec<&str> = range_spec.splitn(2, '-').collect();
        if parts.len() != 2 {
            return orpc::err_box!("Invalid range format: {}", range_spec);
        }

        let first = parts[0]
            .parse::<u64>()
            .map_err(|_| format!("Invalid start position: {}", parts[0]))?;

        if parts[1].is_empty() {
            Ok(Range::Int { first, last: None })
        } else {
            let last = parts[1]
                .parse::<u64>()
                .map_err(|_| format!("Invalid end position: {}", parts[1]))?;
            if first > last {
                return Err(format!(
                    "Start position {first} cannot be greater than end position {last}"
                ));
            }
            Ok(Range::Int {
                first,
                last: Some(last),
            })
        }
    }

    pub fn to_byte_range(&self, file_size: u64) -> Option<(u64, u64)> {
        match *self {
            Range::Int { first, last } => {
                if first >= file_size {
                    return None;
                }

                let end_pos = match last {
                    Some(last) => last.min(file_size - 1),
                    None => file_size - 1,
                };

                if first > end_pos {
                    return None;
                }

                let bytes_to_read = end_pos - first + 1;
                Some((first, bytes_to_read))
            }
            Range::Suffix { length } => {
                if length == 0 {
                    return None;
                }

                let actual_length = length.min(file_size);
                let start_pos = file_size - actual_length;
                Some((start_pos, actual_length))
            }
        }
    }

    pub fn to_content_range(&self, file_size: u64) -> Option<String> {
        if let Some((start, length)) = self.to_byte_range(file_size) {
            let end = start + length - 1;
            Some(format!("bytes {start}-{end}/{file_size}"))
        } else {
            None
        }
    }
}
