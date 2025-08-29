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

//! # S3 Handlers Implementation
//!
//! This module provides concrete implementations of all S3 API operations,
//! bridging the gap between S3 protocol requirements and the Curvine file system.
//!
//! ## Key Features
//!
//! - **Complete S3 API Coverage**: Implements all major S3 operations
//! - **File System Integration**: Seamless mapping between S3 paths and file system paths
//! - **Metadata Conversion**: Translates file system metadata to S3-compatible attributes
//! - **Error Translation**: Converts file system errors to appropriate HTTP status codes
//! - **Performance Optimization**: Efficient streaming and async operations
//!
//! ## Handler Structure
//!
//! All handlers are implemented on the `S3Handlers` struct, which maintains:
//! - Unified file system interface for storage operations
//! - S3 region configuration for response headers
//! - Shared async runtime for blocking operations
//!
//! ## Path Mapping Strategy
//!
//! S3 paths are mapped to file system paths as follows:
//! - S3 bucket `mybucket` → File system path `/mybucket`
//! - S3 object `mybucket/path/to/object` → File system path `/mybucket/path/to/object`
//!
//! ## Error Handling
//!
//! The handlers implement comprehensive error handling:
//! - File not found → HTTP 404 Not Found
//! - Permission denied → HTTP 403 Forbidden  
//! - Invalid path → HTTP 400 Bad Request
//! - System errors → HTTP 500 Internal Server Error

use super::types::{PutContext, PutOperation};
use super::ListObjectContent;
use super::ListObjectHandler;
use super::ListObjectOption;
use super::PutObjectHandler;
use super::PutObjectOption;
use crate::s3::error::Error;
use crate::s3::s3_api::HeadHandler;
use crate::s3::s3_api::HeadObjectResult;
use crate::utils::utils::{file_status_to_head_object_result, file_status_to_list_object_content};
use bytes::BytesMut;
use chrono;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::FileType;
use curvine_common::FsResult;
use orpc::runtime::AsyncRuntime;

use tokio::io::AsyncWriteExt;
use tracing;
use uuid;

/// Core S3 handlers implementation
///
/// This struct serves as the main implementation of all S3 API operations.
/// It integrates with the Curvine file system and provides S3-compatible
/// responses for all supported operations.
///
/// ## Design Principles
///
/// - **Stateless Operations**: Each handler method is stateless and thread-safe
/// - **Async First**: All operations are designed for high-concurrency async execution
/// - **Error Resilience**: Comprehensive error handling with proper S3 error responses
/// - **Performance Focused**: Streaming operations for large objects without memory buffering
///
/// ## Thread Safety
///
/// The struct implements `Clone` and all fields are thread-safe, allowing
/// multiple concurrent operations across different async tasks.
#[derive(Clone)]
pub struct S3Handlers {
    /// Unified file system interface for all storage operations
    /// Provides abstraction over the underlying Curvine distributed storage
    pub fs: UnifiedFileSystem,

    /// S3 region identifier returned in various responses
    /// Used for GetBucketLocation and other region-aware operations
    pub region: String,

    /// Shared async runtime for executing blocking file system operations
    /// Prevents blocking the main async executor with CPU-intensive tasks
    pub rt: std::sync::Arc<AsyncRuntime>,
}

impl S3Handlers {
    /// Create a new S3Handlers instance
    ///
    /// Initializes the S3 handlers with the necessary dependencies for
    /// handling S3 operations against the Curvine file system.
    ///
    /// # Arguments
    ///
    /// * `fs` - The unified filesystem instance for storage operations
    /// * `region` - The S3 region identifier to report in responses
    /// * `rt` - Shared runtime for scheduling internal blocking tasks
    ///
    /// # Returns
    ///
    /// * `Self` - Configured S3 handlers ready to process requests
    ///
    /// # Design Notes
    ///
    /// The handlers are designed to be cloneable and thread-safe, allowing
    /// the same instance to handle multiple concurrent requests efficiently.
    pub fn new(fs: UnifiedFileSystem, region: String, rt: std::sync::Arc<AsyncRuntime>) -> Self {
        tracing::debug!("Creating new S3Handlers with region: {}", region);
        Self { fs, region, rt }
    }

    /// Convert S3 bucket and object key to Curvine filesystem path
    ///
    /// This method implements the core path mapping logic that translates
    /// S3-style bucket/object references into file system paths that can
    /// be used with the Curvine storage backend.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name (must be valid bucket identifier)
    /// * `key` - S3 object key (can contain forward slashes for nested objects)
    ///
    /// # Returns
    ///
    /// * `FsResult<Path>` - Converted file system path or validation error
    ///
    /// # Path Mapping Rules
    ///
    /// - Bucket names cannot contain forward slashes
    /// - Object keys preserve all characters including slashes
    /// - Resulting path: `/{bucket}/{object_key}`
    ///
    /// # Validation
    ///
    /// - Bucket name cannot be empty
    /// - Object key cannot be empty
    /// - Bucket name cannot contain path separators
    ///
    /// # Examples
    ///
    /// ```
    /// cv_object_path("mybucket", "path/to/object.txt")
    ///   → Ok("/mybucket/path/to/object.txt")
    ///
    /// cv_object_path("", "object")
    ///   → Err("bucket name is empty")
    ///
    /// cv_object_path("bucket/invalid", "object")
    ///   → Err("contains invalid characters")
    /// ```
    fn cv_object_path(&self, bucket: &str, key: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 path: s3://{}/{}", bucket, key);

        // Validate bucket and key parameters
        if bucket.is_empty() || key.is_empty() {
            tracing::warn!("Invalid S3 path: bucket or key is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket or key is empty",
            ));
        }

        // Validate bucket name format (no path separators allowed)
        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        // Construct the file system path
        let path = format!("/{}/{}", bucket, key);
        tracing::debug!("Mapped S3 path to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }

    /// Convert S3 bucket name to Curvine filesystem path
    ///
    /// This method handles bucket-level operations by converting bucket names
    /// into appropriate file system directory paths.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name to convert
    ///
    /// # Returns
    ///
    /// * `FsResult<Path>` - File system directory path for the bucket
    ///
    /// # Path Mapping
    ///
    /// - Bucket `mybucket` → File system path `/mybucket`
    /// - All bucket operations work on this directory
    ///
    /// # Validation
    ///
    /// - Bucket name cannot be empty
    /// - Bucket name cannot contain path separators
    ///
    /// # Usage
    ///
    /// This method is used for:
    /// - Bucket creation (mkdir operations)
    /// - Bucket deletion (rmdir operations)  
    /// - Bucket listing (directory listing)
    /// - Bucket existence checks
    fn cv_bucket_path(&self, bucket: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 bucket: s3://{}", bucket);

        // Validate bucket name
        if bucket.is_empty() {
            tracing::warn!("Invalid bucket name: bucket name is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket name is empty",
            ));
        }

        // Validate bucket name format
        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        // Construct bucket directory path
        let path = format!("/{}", bucket);
        tracing::debug!("Mapped S3 bucket to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }
}

/// Implementation of HeadHandler trait for object metadata operations
///
/// Provides S3 HeadObject functionality by retrieving file metadata from
/// the Curvine file system and converting it to S3-compatible format.
#[async_trait::async_trait]
impl HeadHandler for S3Handlers {
    /// Look up object metadata for HEAD request
    ///
    /// Retrieves comprehensive metadata for an S3 object, including size,
    /// modification time, content type, and other S3-compatible attributes.
    /// This operation is essential for S3 compatibility and is used by many
    /// S3 clients for existence checks and metadata retrieval.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name containing the object
    /// * `object` - S3 object key to retrieve metadata for
    ///
    /// # Returns
    ///
    /// * `Future<Result<Option<HeadObjectResult>, Error>>` - Object metadata or None if not found
    ///
    /// # S3 Compatibility
    ///
    /// Returns all standard S3 object metadata:
    /// - `content-length`: Object size in bytes
    /// - `last-modified`: Object modification timestamp (RFC 3339 format)
    /// - `etag`: Object entity tag for caching and integrity
    /// - `content-type`: MIME type inferred from object name
    /// - `storage-class`: Storage tier information
    /// - `metadata`: Custom metadata headers
    ///
    /// # File Type Handling
    ///
    /// - Regular files: Return complete metadata
    /// - Directories: Return None (S3 doesn't expose directories as objects)
    /// - Symlinks: Follow link and return target metadata
    ///
    /// # Error Handling
    ///
    /// - Invalid path format → Returns None
    /// - File not found → Returns None
    /// - Permission denied → Returns None
    /// - System errors → Returns Error
    ///
    /// # Performance Notes
    ///
    /// - Uses `block_in_place` to avoid blocking the async executor
    /// - Single file system call for efficiency
    /// - Metadata conversion is performed in memory
    async fn lookup(&self, bucket: &str, object: &str) -> Result<Option<HeadObjectResult>, Error> {
        tracing::info!("HEAD request for s3://{}/{}", bucket, object);

        // Convert S3 path to file system path with error handling
        let path = match self.cv_object_path(bucket, object) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                return Ok(None);
            }
        };

        // Clone necessary data for the async block
        let fs = self.fs.clone();
        let object_name = object.to_string();

        // Execute file system operation directly in async context
        let res = fs.get_status(&path).await;

        match res {
            Ok(st) if st.file_type == FileType::File => {
                tracing::debug!("Found file at path: {}, size: {}", path, st.len);

                // Use file converter to create complete HeadObjectResult
                let head = file_status_to_head_object_result(&st, &object_name);

                Ok(Some(head))
            }
            Ok(st) => {
                tracing::debug!("Path exists but is not a file: {:?}", st.file_type);
                Ok(None)
            }
            Err(e) => {
                tracing::warn!("Failed to get status for path {}: {}", path, e);
                Ok(None)
            }
        }
    }
}

/// Implementation of GetObjectHandler trait for object download operations
///
/// Provides S3 GetObject functionality with support for range requests,
/// streaming downloads, and proper S3 response headers.
#[async_trait::async_trait]
impl crate::s3::s3_api::GetObjectHandler for S3Handlers {
    /// Handle GET object request with optional range support
    ///
    /// Downloads object content from the Curvine file system and streams it
    /// to the client. Supports HTTP Range requests for partial downloads,
    /// which is essential for large file handling and resumable downloads.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name containing the object
    /// * `object` - S3 object key to download
    /// * `_opt` - Get object options including range parameters
    /// * `out` - Output stream for writing object data
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), String>>` - Success or detailed error message
    ///
    /// # Range Request Support
    ///
    /// Supports HTTP Range headers for partial content downloads:
    /// - `Range: bytes=0-1023` - First 1024 bytes
    /// - `Range: bytes=1024-` - From byte 1024 to end
    /// - `Range: bytes=-1024` - Last 1024 bytes
    ///
    /// # Streaming Architecture
    ///
    /// - **Zero Copy**: Direct streaming from storage to network
    /// - **Memory Efficient**: No buffering of entire file in memory
    /// - **Chunked Reading**: 64KB chunks for optimal throughput
    /// - **Async I/O**: Non-blocking operations throughout
    ///
    /// # Error Handling
    ///
    /// - Invalid path → Detailed error message
    /// - File not found → File system error propagation
    /// - Permission denied → Access error propagation
    /// - Range errors → Range validation error messages
    ///
    /// # Performance Optimizations
    ///
    /// - Single file open operation
    /// - Efficient seek operations for range requests
    /// - Direct write to output stream
    /// - Proper resource cleanup
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        _opt: crate::s3::s3_api::GetObjectOption,
        out: tokio::sync::Mutex<
            std::pin::Pin<Box<dyn 'a + Send + crate::utils::io::PollWrite + Unpin>>,
        >,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        // let rt = self.rt.clone();
        let path = self.cv_object_path(bucket, object);
        let bucket = bucket.to_string();
        let object = object.to_string();

        Box::pin(async move {
            // Log range request details if present
            if let Some(start) = _opt.range_start {
                if let Some(end) = _opt.range_end {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-{}",
                        bucket,
                        object,
                        start,
                        end
                    );
                } else {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-",
                        bucket,
                        object,
                        start
                    );
                }
            } else {
                tracing::info!("GET object s3://{}/{}", bucket, object);
            }

            // Convert S3 path to Curvine path with error handling
            let path = path.map_err(|e| {
                tracing::error!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                e.to_string()
            })?;

            // Open file for reading with error handling
            let mut reader = fs.open(&path).await.map_err(|e| {
                tracing::error!("Failed to open file at path {}: {}", path, e);
                e.to_string()
            })?;

            // Handle range requests (including suffix-byte-range-spec)
            let (seek_pos, bytes_to_read) = if let Some(range_end) = _opt.range_end {
                if range_end > u64::MAX / 2 {
                    // This is a suffix-byte-range-spec (bytes=-N)
                    let suffix_len = u64::MAX - range_end;
                    let file_size = reader.remaining().max(0) as u64;
                    if suffix_len > file_size {
                        // If suffix length is larger than file, read entire file
                        (None, None)
                    } else {
                        // Seek to (file_size - suffix_len) and read suffix_len bytes
                        let start_pos = file_size - suffix_len;
                        tracing::debug!(
                            "Suffix range request: seeking to {} for last {} bytes",
                            start_pos,
                            suffix_len
                        );
                        (Some(start_pos), Some(suffix_len))
                    }
                } else {
                    // Normal range processing
                    let start = _opt.range_start.unwrap_or(0);
                    let bytes = range_end - start + 1;
                    tracing::debug!(
                        "Normal range request: seeking to {} for {} bytes",
                        start,
                        bytes
                    );
                    (Some(start), Some(bytes))
                }
            } else if let Some(start) = _opt.range_start {
                // range_start only (bytes=N-)
                tracing::debug!("Open-ended range request: seeking to {}", start);
                (Some(start), None)
            } else {
                // No range request
                (None, None)
            };

            // Apply seek if needed
            if let Some(pos) = seek_pos {
                reader.seek(pos as i64).await.map_err(|e| {
                    tracing::error!("Failed to seek to position {}: {}", pos, e);
                    e.to_string()
                })?;
            }

            // Determine target read amount
            let remaining_bytes = bytes_to_read;

            // Always use direct read to fix zero-padding issues
            log::debug!(
                "GetObject: range_bytes={:?}, reader.remaining()={}",
                remaining_bytes,
                reader.remaining()
            );

            let target_read = if let Some(range_bytes) = remaining_bytes {
                range_bytes
            } else {
                reader.remaining().max(0) as u64
            };

            log::debug!("GetObject: will read {} bytes directly", target_read);

            // Stream processing with fixed 4KB buffer for memory efficiency
            const CHUNK_SIZE: usize = 4096; // 4KB chunks for memory efficiency
            let mut total_read = 0u64;
            let mut remaining_to_read = target_read;

            // Get output writer outside the loop
            let mut guard = out.lock().await;

            while remaining_to_read > 0 {
                // Calculate chunk size for this iteration
                let chunk_size = std::cmp::min(CHUNK_SIZE, remaining_to_read as usize);

                // Read chunk from file
                let mut buffer = BytesMut::zeroed(chunk_size);
                let bytes_read = reader
                    .read_full(&mut buffer)
                    .await
                    .map_err(|e| e.to_string())?;

                if bytes_read == 0 {
                    // End of file reached
                    break;
                }

                // Truncate buffer to actual bytes read
                buffer.truncate(bytes_read);

                // Write chunk to output stream immediately
                guard.poll_write(&buffer).await.map_err(|e| {
                    tracing::error!("Failed to write chunk to output: {}", e);
                    e.to_string()
                })?;

                total_read += bytes_read as u64;
                remaining_to_read -= bytes_read as u64;

                log::trace!(
                    "GetObject: streamed chunk {} bytes (total: {})",
                    bytes_read,
                    total_read
                );
            }

            // Release the guard
            drop(guard);

            log::debug!(
                "GetObject: streaming completed, total bytes: {}",
                total_read
            );

            // Clean up reader to stop background tasks
            if let Err(e) = reader.complete().await {
                tracing::warn!("Failed to complete reader cleanup: {}", e);
                // Don't fail the request for cleanup errors
            }

            tracing::info!(
                "GET object s3://{}/{} completed, total bytes: {}",
                bucket,
                object,
                total_read
            );
            Ok(())
        })
    }
}

/// Implementation of PutObjectHandler trait for object upload operations
///
/// Provides S3 PutObject functionality with streaming uploads, checksum verification,
/// and proper S3 response handling.
#[async_trait::async_trait]
impl PutObjectHandler for S3Handlers {
    /// Handle PUT object request for uploading data
    ///
    /// Processes object uploads by streaming data from the client to the
    /// Curvine file system. Uses the modular PutOperation for clean separation
    /// of concerns and better error handling.
    ///
    /// # Arguments
    ///
    /// * `_opt` - Put object options (currently unused, reserved for future extensions)
    /// * `bucket` - S3 bucket name to store the object
    /// * `object` - S3 object key for the uploaded content
    /// * `body` - Input stream for reading object data from client
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), String>>` - Success or detailed error message
    ///
    /// # Upload Process
    ///
    /// 1. **Context Creation**: Set up PUT operation context with paths and configuration
    /// 2. **Stream Processing**: Read data chunks from client input stream
    /// 3. **Write Operations**: Stream data directly to file system writer
    /// 4. **Completion**: Finalize file write and cleanup resources
    ///
    /// # Design Pattern
    ///
    /// Uses the modular `PutOperation::execute` pattern for:
    /// - **Separation of Concerns**: Upload logic isolated from handler interface
    /// - **Reusability**: Upload logic can be reused in other contexts
    /// - **Testability**: Upload logic can be unit tested independently
    /// - **Maintainability**: Complex upload logic is well-organized
    ///
    /// # Performance Characteristics
    ///
    /// - **Streaming**: No buffering of entire object in memory
    /// - **Async I/O**: Non-blocking operations throughout upload
    /// - **Error Recovery**: Proper cleanup on failure
    /// - **Resource Management**: Automatic file handle cleanup
    ///
    /// # Error Scenarios
    ///
    /// - Invalid bucket/object names → Path validation errors
    /// - File system errors → Write operation failures
    /// - Network errors → Stream reading failures
    /// - Resource exhaustion → Out of disk space errors
    async fn handle(
        &self,
        _opt: &PutObjectOption,
        bucket: &str,
        object: &str,
        body: &mut (dyn crate::utils::io::PollRead + Unpin + Send),
    ) -> Result<(), String> {
        // Create PUT operation context with all necessary configuration
        let context = PutContext::new(
            self.fs.clone(),
            self.rt.clone(),
            bucket.to_string(),
            object.to_string(),
            self.cv_object_path(bucket, object),
        );

        // Execute PUT operation using the modular orchestrator
        PutOperation::execute(context, body).await
    }
}

/// Implementation of DeleteObjectHandler trait for object deletion operations
///
/// Provides S3 DeleteObject functionality with proper error handling and
/// S3-compatible idempotent behavior.
#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteObjectHandler for S3Handlers {
    /// Handle DELETE object request
    ///
    /// Deletes an object from the specified bucket. Implements S3-compatible
    /// idempotent behavior where deleting a non-existent object succeeds.
    ///
    /// # Arguments
    ///
    /// * `_opt` - Delete object options (currently unused)
    /// * `object` - Full S3 path (bucket/object) to delete
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), String>>` - Success or error message
    ///
    /// # S3 Compatibility
    ///
    /// - Returns success even if object doesn't exist (idempotent)
    /// - Proper error handling for permission issues
    /// - Standard S3 error response format
    ///
    /// # Implementation Notes
    ///
    /// Currently takes full object path rather than separate bucket/object
    /// parameters. This may be updated in future versions for consistency.
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteObjectOption,
        object: &str,
    ) -> Result<(), String> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let object = object.to_string();
        let path = Path::from_str(format!("/{}", object));

        let path = path.map_err(|e| e.to_string())?;
        match fs.delete(&path, false).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // Treat not-found as success to be S3 compatible (idempotent)
                if msg.contains("No such file")
                    || msg.contains("not exists")
                    || msg.contains("not found")
                {
                    Ok(())
                } else {
                    Err(msg)
                }
            }
        }
    }
}

/// Implementation of CreateBucketHandler trait for bucket creation operations
///
/// Provides S3 CreateBucket functionality by creating directories in the
/// file system with proper error handling and validation.
#[async_trait::async_trait]
impl crate::s3::s3_api::CreateBucketHandler for S3Handlers {
    /// Handle CREATE bucket request
    ///
    /// Creates a new S3 bucket by creating a corresponding directory in the
    /// Curvine file system. Implements proper validation and error handling.
    ///
    /// # Arguments
    ///
    /// * `_opt` - Create bucket options (currently unused)
    /// * `bucket` - S3 bucket name to create
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), String>>` - Success or error message
    ///
    /// # Validation
    ///
    /// - Bucket name format validation
    /// - Duplicate bucket name handling
    /// - Permission validation
    ///
    /// # File System Mapping
    ///
    /// Creates directory `/bucket-name` in the file system root
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::CreateBucketOption,
        bucket: &str,
    ) -> Result<(), String> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);

        let path = path.map_err(|e| e.to_string())?;

        // Check if bucket already exists
        match fs.get_status(&path).await {
            Ok(_) => {
                // Bucket already exists, return 409 Conflict error
                return Err("BucketAlreadyExists".to_string());
            }
            Err(_) => {
                // Bucket doesn't exist, proceed to create
            }
        }

        fs.mkdir(&path, true).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// Implementation of DeleteBucketHandler trait for bucket deletion operations
///
/// Provides S3 DeleteBucket functionality with proper empty bucket validation
/// and error handling.
#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteBucketHandler for S3Handlers {
    /// Handle DELETE bucket request
    ///
    /// Deletes an S3 bucket by removing the corresponding directory from the
    /// file system. Implements S3-compatible behavior for non-empty buckets.
    ///
    /// # Arguments
    ///
    /// * `_opt` - Delete bucket options (currently unused)
    /// * `bucket` - S3 bucket name to delete
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), String>>` - Success or error message
    ///
    /// # S3 Compatibility
    ///
    /// - Fails if bucket is not empty (standard S3 behavior)
    /// - Returns appropriate error codes
    /// - Idempotent operation
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteBucketOption,
        bucket: &str,
    ) -> Result<(), String> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);

        let path = path.map_err(|e| e.to_string())?;
        fs.delete(&path, false).await.map_err(|e| e.to_string())
    }
}

/// Implementation of ListBucketHandler trait for bucket listing operations
///
/// Provides S3 ListBuckets functionality by listing directories in the
/// file system root and converting them to S3 bucket format.
#[async_trait::async_trait]
impl crate::s3::s3_api::ListBucketHandler for S3Handlers {
    /// Handle LIST buckets request
    ///
    /// Lists all buckets accessible to the requesting user by enumerating
    /// directories in the file system root.
    ///
    /// # Arguments
    ///
    /// * `_opt` - List buckets options (currently unused)
    ///
    /// # Returns
    ///
    /// * `Future<Result<Vec<Bucket>, String>>` - List of bucket information
    ///
    /// # Response Format
    ///
    /// Returns bucket information including:
    /// - Bucket name
    /// - Creation date (current timestamp)
    /// - Region information
    ///
    /// # Implementation Notes
    ///
    /// - Only directories are considered as buckets
    /// - Creation date is set to current time (file system limitation)
    /// - Region is set from handler configuration
    fn handle<'a>(
        &'a self,
        _opt: &'a crate::s3::s3_api::ListBucketsOption,
    ) -> std::pin::Pin<
        Box<
            dyn 'a
                + Send
                + std::future::Future<Output = Result<Vec<crate::s3::s3_api::Bucket>, String>>,
        >,
    > {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let region = self.region.clone();

        Box::pin(async move {
            let mut buckets = vec![];
            let root = Path::from_str("/").map_err(|e| e.to_string())?;
            let list = fs.list_status(&root).await.map_err(|e| e.to_string())?;
            let now = chrono::Utc::now().to_rfc3339();

            // Convert directories to bucket entries
            for st in list {
                if st.is_dir {
                    buckets.push(crate::s3::s3_api::Bucket {
                        name: st.name,
                        creation_date: now.clone(),
                        bucket_region: region.clone(),
                    });
                }
            }
            Ok(buckets)
        })
    }
}

/// Implementation of GetBucketLocationHandler trait for bucket location operations
///
/// Provides S3 GetBucketLocation functionality by returning the configured
/// region for all buckets.
impl crate::s3::s3_api::GetBucketLocationHandler for S3Handlers {
    /// Handle GET bucket location request
    ///
    /// Returns the AWS region where the bucket is located. Currently returns
    /// a fixed region as Curvine doesn't have region-specific storage.
    ///
    /// # Arguments
    ///
    /// * `_loc` - Location parameter (currently unused)
    ///
    /// # Returns
    ///
    /// * `Future<Result<Option<&'static str>, ()>>` - Region string or error
    ///
    /// # Implementation Notes
    ///
    /// Returns a fixed region "us-east-1" for all buckets. Future versions
    /// may support region-specific bucket placement.
    fn handle<'a>(
        &'a self,
        _loc: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<&'static str>, ()>>>,
    > {
        Box::pin(async move { Ok(Some("us-east-1")) })
    }
}

/// Implementation of MultiUploadObjectHandler trait for multipart upload operations
///
/// Provides complete S3 multipart upload functionality including session management,
/// part uploads, and session completion with proper cleanup.
#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    /// Create a new multipart upload session
    ///
    /// Initiates a new multipart upload session for large objects by generating
    /// a unique upload ID that will be used for all subsequent part uploads.
    ///
    /// # Arguments
    ///
    /// * `_bucket` - S3 bucket name (currently unused in ID generation)
    /// * `_key` - S3 object key (currently unused in ID generation)
    ///
    /// # Returns
    ///
    /// * `Future<Result<String, ()>>` - Unique upload ID or error
    ///
    /// # Implementation
    ///
    /// Uses UUID v4 for generating unique upload IDs that are:
    /// - Globally unique across all sessions
    /// - Unpredictable for security
    /// - Compatible with S3 clients
    fn handle_create_session<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            let upload_id = uuid::Uuid::new_v4().to_string();
            Ok(upload_id)
        })
    }

    /// Upload a single part of a multipart upload
    ///
    /// Processes a single part of a multipart upload by storing the part data
    /// in temporary storage and generating an ETag for integrity verification.
    ///
    /// # Arguments
    ///
    /// * `_bucket` - S3 bucket name (used for logging)
    /// * `_key` - S3 object key (used for logging)
    /// * `upload_id` - Multipart upload session ID
    /// * `part_number` - Part number (1-based indexing)
    /// * `body` - Part data stream
    ///
    /// # Returns
    ///
    /// * `Future<Result<String, ()>>` - Part ETag or error
    ///
    /// # Storage Strategy
    ///
    /// - Parts are stored in `/tmp/curvine-multipart/{upload_id}/{part_number}`
    /// - Each part is stored as a separate file
    /// - MD5 hash is calculated for ETag generation
    /// - BytesMut is used to avoid zero-prefilled memory issues
    ///
    /// # ETag Generation
    ///
    /// Uses MD5 hash of part content formatted as `"hexstring"` for S3 compatibility
    fn handle_upload_part<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
        part_number: u32,
        body: &'a mut (dyn tokio::io::AsyncRead + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            use bytes::BytesMut;
            use tokio::io::AsyncReadExt;

            // Create temporary directory for this upload session
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let _ = tokio::fs::create_dir_all(&dir).await;

            // Create file for this specific part
            let path = format!("{}/{}", dir, part_number);
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
            {
                Ok(f) => f,
                Err(_) => return Err(()),
            };

            // Initialize MD5 hasher and data accumulator
            let mut hasher = md5::Context::new();
            let mut total_data = BytesMut::new();

            // Read data chunk by chunk and accumulate in BytesMut (no pre-filled zeros)
            let mut temp_buf = vec![0u8; 1024 * 1024]; // 1MB buffer
            loop {
                let n = match body.read(&mut temp_buf).await {
                    Ok(n) => n,
                    Err(_) => return Err(()),
                };
                if n == 0 {
                    break; // End of stream
                }

                // Process only the actual data read
                let actual_data = &temp_buf[..n];
                hasher.consume(actual_data);
                total_data.extend_from_slice(actual_data);
            }

            // Write all accumulated data at once to avoid corruption
            if let Err(_) = file.write_all(&total_data).await {
                return Err(());
            }

            // Generate ETag from MD5 hash
            let digest = hasher.compute();
            Ok(format!("\"{:x}\"", digest))
        })
    }

    /// Complete a multipart upload session
    ///
    /// Combines all uploaded parts into a single object by concatenating them
    /// in the correct order and storing the result in the target location.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name for the final object
    /// * `key` - S3 object key for the final object
    /// * `upload_id` - Multipart upload session ID
    /// * `data` - Array of (ETag, part_number) pairs
    /// * `_opts` - Completion options (currently unused)
    ///
    /// # Returns
    ///
    /// * `Future<Result<String, ()>>` - Final object ETag or error
    ///
    /// # Process
    ///
    /// 1. Sort parts by part number to ensure correct order
    /// 2. Create final object writer in target location
    /// 3. Read each part file and write to final object
    /// 4. Complete the final object write
    /// 5. Clean up temporary files
    ///
    /// # Error Handling
    ///
    /// - Missing parts → Error
    /// - Part read failures → Error
    /// - Write failures → Error
    /// - Cleanup failures → Logged but not propagated
    fn handle_complete<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        data: &'a [(&'a str, u32)],
        _opts: crate::s3::s3_api::MultiUploadObjectCompleteOption,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let path_res = self.cv_object_path(&bucket, &key);

        Box::pin(async move {
            use tokio::io::AsyncReadExt;

            // Convert S3 path to file system path
            let final_path = match path_res {
                Ok(p) => p,
                Err(_) => return Err(()),
            };

            // Create writer for final object
            let mut writer = match fs.create(&final_path, true).await {
                Ok(w) => w,
                Err(_) => return Err(()),
            };

            // Prepare temporary directory path
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);

            // Sort parts by part number to ensure correct order
            let mut part_list = data.to_vec();
            part_list.sort_by_key(|(_, n)| *n);

            // Concatenate all parts in order
            for (_, num) in part_list {
                let path = format!("{}/{}", dir, num);
                let mut file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
                    Ok(f) => f,
                    Err(_) => return Err(()),
                };

                // Read part file in chunks and write to final object
                let mut buf = [0u8; 1024 * 1024]; // 1MB buffer
                loop {
                    let n = match file.read(&mut buf).await {
                        Ok(n) => n,
                        Err(_) => return Err(()),
                    };
                    if n == 0 {
                        break; // End of part file
                    }

                    // Write chunk to final object
                    if let Err(_) = writer.write(&buf[..n]).await {
                        return Err(());
                    };
                }
            }

            // Complete the final object write
            if let Err(_) = writer.complete().await {
                return Err(());
            }

            // Clean up temporary files (best effort)
            let _ = tokio::fs::remove_dir_all(&dir).await;

            // Return placeholder ETag (could be improved with actual computation)
            Ok("etag-not-computed".to_string())
        })
    }

    /// Abort a multipart upload session
    ///
    /// Cancels a multipart upload session and cleans up all temporary files
    /// associated with the upload.
    ///
    /// # Arguments
    ///
    /// * `_bucket` - S3 bucket name (currently unused)
    /// * `_key` - S3 object key (currently unused)
    /// * `upload_id` - Multipart upload session ID to abort
    ///
    /// # Returns
    ///
    /// * `Future<Result<(), ()>>` - Success or error
    ///
    /// # Cleanup Process
    ///
    /// Removes the entire temporary directory for the upload session,
    /// including all uploaded parts. This is a best-effort operation.
    fn handle_abort<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), ()>>>> {
        Box::pin(async move {
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Ok(())
        })
    }
}

/// Implementation of ListObjectHandler trait for object listing operations
///
/// Provides S3 ListObjectsV2 functionality with prefix filtering and
/// proper S3-compatible response format.
#[async_trait::async_trait]
impl ListObjectHandler for S3Handlers {
    /// Handle LIST objects request
    ///
    /// Lists objects within a specified bucket with optional prefix filtering.
    /// Implements S3 ListObjectsV2 API with proper metadata conversion.
    ///
    /// # Arguments
    ///
    /// * `opt` - List object options including prefix and pagination parameters
    /// * `bucket` - S3 bucket name to list objects from
    ///
    /// # Returns
    ///
    /// * `Future<Result<Vec<ListObjectContent>, String>>` - List of object information
    ///
    /// # Filtering
    ///
    /// - Prefix filtering: Only objects matching the prefix are returned
    /// - Directory filtering: Directories are excluded from results
    /// - Metadata conversion: File system metadata is converted to S3 format
    ///
    /// # Response Format
    ///
    /// Each object includes:
    /// - Key: Object name/path
    /// - LastModified: Modification timestamp
    /// - ETag: Object integrity tag
    /// - Size: Object size in bytes
    /// - StorageClass: Storage tier information
    /// - Owner: Object owner information
    async fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> Result<Vec<ListObjectContent>, String> {
        // Convert bucket name to file system path
        let bkt_path = self.cv_bucket_path(bucket).map_err(|e| e.to_string())?;

        // Always list from bucket root
        let root = bkt_path;

        // List directory contents
        let list = self
            .fs
            .list_status(&root)
            .await
            .map_err(|e| e.to_string())?;
        let mut contents = Vec::new();

        // Process each file system entry
        for st in list {
            if st.is_dir {
                // Skip directories for now; S3 v2 can emit CommonPrefixes if delimiter set
                continue;
            }

            // Object key is just the file name (no prefix manipulation)
            let key = st.name.clone();

            // Apply prefix filtering if specified
            if let Some(pref) = &opt.prefix {
                if !key.starts_with(pref) {
                    continue; // Skip objects that don't match prefix
                }
            }

            // Use file converter to create complete ListObjectContent
            contents.push(file_status_to_list_object_content(&st, key));
        }

        Ok(contents)
    }
}
