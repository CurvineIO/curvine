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

use bytes::BytesMut;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use std::future::Future;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
const MIN_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct TempStorageConfig {
    pub temp_dir: String,
    pub memory_buffer_threshold: usize,
    pub max_memory_buffer: usize,
}

impl TempStorageConfig {
    pub fn new(temp_dir: String, memory_buffer_threshold: usize, max_memory_buffer: usize) -> Self {
        Self {
            temp_dir,
            memory_buffer_threshold,
            max_memory_buffer,
        }
    }

    #[inline]
    pub fn buffer_size(&self) -> usize {
        if self.max_memory_buffer > 0 {
            self.max_memory_buffer.max(MIN_BUFFER_SIZE)
        } else {
            DEFAULT_BUFFER_SIZE
        }
    }
}

#[derive(Clone)]
pub struct LocalTempStorage {
    config: TempStorageConfig,
}

impl LocalTempStorage {
    pub fn new(config: TempStorageConfig) -> Self {
        Self { config }
    }

    #[inline]
    fn part_path(&self, upload_id: &str, part_number: u32) -> String {
        format!("{}/{}/{}", self.config.temp_dir, upload_id, part_number)
    }

    #[inline]
    fn dir_path(&self, upload_id: &str) -> String {
        format!("{}/{}", self.config.temp_dir, upload_id)
    }

    pub fn config(&self) -> &TempStorageConfig {
        &self.config
    }

    /// Create directory for upload session
    pub async fn create_dir(&self, upload_id: &str) -> Result<(), String> {
        let dir = self.dir_path(upload_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("Failed to create temp dir {}: {}", dir, e))
    }

    /// Remove entire upload session directory (cleanup after complete/abort)
    pub async fn cleanup(&self, upload_id: &str) -> Result<(), String> {
        let dir = self.dir_path(upload_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("Failed to cleanup temp dir {}: {}", dir, e)),
        }
    }

    /// Stream write part data, returns ETag (MD5 hash)
    pub async fn write_part_stream(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut crate::utils::io::AsyncReadEnum,
    ) -> Result<String, String> {
        use tokio::io::AsyncWriteExt;

        let path = self.part_path(upload_id, part_number);
        let dir = self.dir_path(upload_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("Failed to create temp dir {}: {}", dir, e))?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| format!("Failed to create part file {}: {}", path, e))?;

        let mut hasher = md5::Context::new();
        let buffer_size = self.config.buffer_size();
        let threshold = self.config.memory_buffer_threshold;
        let mut memory_buf = BytesMut::with_capacity(threshold.min(buffer_size));
        let mut temp_buf = BytesMut::with_capacity(buffer_size);
        temp_buf.resize(buffer_size, 0);

        loop {
            let n = body
                .read(&mut temp_buf)
                .await
                .map_err(|e| format!("Failed to read from body: {}", e))?;

            if n == 0 {
                break;
            }

            let chunk = &temp_buf[..n];
            hasher.consume(chunk);

            if memory_buf.len() + n <= threshold {
                memory_buf.extend_from_slice(chunk);
            } else {
                if !memory_buf.is_empty() {
                    file.write_all(&memory_buf)
                        .await
                        .map_err(|e| format!("Failed to write to part file: {}", e))?;
                    memory_buf.clear();
                }
                file.write_all(chunk)
                    .await
                    .map_err(|e| format!("Failed to write to part file: {}", e))?;
            }
        }

        if !memory_buf.is_empty() {
            file.write_all(&memory_buf)
                .await
                .map_err(|e| format!("Failed to write to part file: {}", e))?;
        }

        file.flush()
            .await
            .map_err(|e| format!("Failed to flush part file: {}", e))?;

        let digest = hasher.compute();
        Ok(format!("\"{:x}\"", digest))
    }

    /// Stream read part data to Writer
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        writer: &mut W,
    ) -> Result<(), String> {
        use tokio::io::AsyncReadExt;

        let path = self.part_path(upload_id, part_number);
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| format!("Failed to open part file {}: {}", path, e))?;

        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);

        loop {
            let n = file
                .read(&mut buf)
                .await
                .map_err(|e| format!("Failed to read from part file: {}", e))?;
            if n == 0 {
                break;
            }

            writer
                .write(&buf[..n])
                .await
                .map_err(|e| format!("Failed to write to destination: {}", e))?;
        }

        Ok(())
    }
}

// ============================================================================
// Curvine Temp Storage Implementation
// ============================================================================

/// Curvine distributed filesystem based temporary storage (distributed mode)
#[derive(Clone)]
pub struct CurvineTempStorage {
    fs: UnifiedFileSystem,
    config: TempStorageConfig,
}

impl CurvineTempStorage {
    pub fn new(fs: UnifiedFileSystem, config: TempStorageConfig) -> Self {
        Self { fs, config }
    }

    #[inline]
    fn part_path(&self, upload_id: &str, part_number: u32) -> Result<Path, String> {
        Path::from_str(format!(
            "{}/{}/{}",
            self.config.temp_dir, upload_id, part_number
        ))
        .map_err(|e| format!("Invalid part path: {}", e))
    }

    #[inline]
    fn dir_path(&self, upload_id: &str) -> Result<Path, String> {
        Path::from_str(format!("{}/{}", self.config.temp_dir, upload_id))
            .map_err(|e| format!("Invalid dir path: {}", e))
    }

    pub fn config(&self) -> &TempStorageConfig {
        &self.config
    }

    /// Create directory for upload session
    pub async fn create_dir(&self, upload_id: &str) -> Result<(), String> {
        let path = self.dir_path(upload_id)?;
        self.fs
            .mkdir(&path, true)
            .await
            .map_err(|e| format!("Failed to create temp dir in Curvine: {}", e))?;
        Ok(())
    }

    /// Remove entire upload session directory (cleanup after complete/abort)
    pub async fn cleanup(&self, upload_id: &str) -> Result<(), String> {
        let path = self.dir_path(upload_id)?;
        match self.fs.delete(&path, true).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found")
                    || msg.contains("No such file")
                    || msg.contains("not exists")
                {
                    Ok(())
                } else {
                    Err(format!("Failed to cleanup temp dir in Curvine: {}", e))
                }
            }
        }
    }

    /// Stream write part data, returns ETag (MD5 hash)
    pub async fn write_part_stream(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut crate::utils::io::AsyncReadEnum,
    ) -> Result<String, String> {
        let dir_path = self.dir_path(upload_id)?;
        let _ = self.fs.mkdir(&dir_path, true).await;

        let path = self.part_path(upload_id, part_number)?;
        let mut writer = self
            .fs
            .create(&path, true)
            .await
            .map_err(|e| format!("Failed to create part file in Curvine: {}", e))?;

        let mut hasher = md5::Context::new();
        let buffer_size = self.config.buffer_size();
        let threshold = self.config.memory_buffer_threshold;
        let mut memory_buf = BytesMut::with_capacity(threshold.min(buffer_size));
        let mut temp_buf = BytesMut::with_capacity(buffer_size);
        temp_buf.resize(buffer_size, 0);

        loop {
            let n = body
                .read(&mut temp_buf)
                .await
                .map_err(|e| format!("Failed to read from body: {}", e))?;

            if n == 0 {
                break;
            }

            let chunk = &temp_buf[..n];
            hasher.consume(chunk);

            if memory_buf.len() + n <= threshold {
                memory_buf.extend_from_slice(chunk);
            } else {
                if !memory_buf.is_empty() {
                    writer
                        .write(&memory_buf)
                        .await
                        .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
                    memory_buf.clear();
                }
                writer
                    .write(chunk)
                    .await
                    .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
            }
        }

        if !memory_buf.is_empty() {
            writer
                .write(&memory_buf)
                .await
                .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
        }

        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete part write in Curvine: {}", e))?;

        let digest = hasher.compute();
        Ok(format!("\"{:x}\"", digest))
    }

    /// Stream read part data to Writer
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        writer: &mut W,
    ) -> Result<(), String> {
        let path = self.part_path(upload_id, part_number)?;
        let mut reader = self
            .fs
            .open(&path)
            .await
            .map_err(|e| format!("Failed to open part file in Curvine: {}", e))?;

        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);

        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|e| format!("Failed to read from part file: {}", e))?;
            if n == 0 {
                break;
            }

            writer
                .write(&buf[..n])
                .await
                .map_err(|e| format!("Failed to write to destination: {}", e))?;
        }

        reader
            .complete()
            .await
            .map_err(|e| format!("Failed to complete reader in Curvine: {}", e))?;

        Ok(())
    }
}

// ============================================================================
// Temp Storage Enum (Static Dispatch)
// ============================================================================

/// Temp storage implementation enum for static dispatch
///
/// This enum provides zero-cost abstraction over different storage backends.
/// The compiler will generate optimized code for each variant without
/// dynamic dispatch overhead.
#[derive(Clone)]
pub enum TempStorageEnum {
    /// Local filesystem storage (standalone mode)
    Local(LocalTempStorage),
    /// Curvine distributed filesystem storage (distributed mode)
    Curvine(CurvineTempStorage),
}

impl TempStorageEnum {
    // ========================================================================
    // Factory Method
    // ========================================================================

    /// Factory method: create storage based on configuration
    ///
    /// # Arguments
    /// * `enable_distributed` - If true, use Curvine DFS; if false, use local FS
    ///   Must match `worker.enable_s3_gateway` configuration.
    /// * `local_temp_dir` - Local filesystem temp directory path
    /// * `curvine_temp_prefix` - Curvine DFS temp directory prefix (e.g., "/.s3temp")
    /// * `memory_buffer_threshold` - Memory buffer threshold for small parts
    /// * `max_memory_buffer` - Maximum memory buffer size
    /// * `fs` - UnifiedFileSystem instance (required when enable_distributed=true)
    ///
    /// # Panics
    /// Panics if `enable_distributed=true` but `fs` is `None`.
    pub fn create(
        enable_distributed: bool,
        local_temp_dir: String,
        curvine_temp_prefix: String,
        memory_buffer_threshold: usize,
        max_memory_buffer: usize,
        fs: Option<UnifiedFileSystem>,
    ) -> Self {
        if enable_distributed {
            let config = TempStorageConfig::new(
                curvine_temp_prefix,
                memory_buffer_threshold,
                max_memory_buffer,
            );
            TempStorageEnum::Curvine(CurvineTempStorage::new(
                fs.expect(
                    "UnifiedFileSystem required for distributed mode (enable_s3_gateway=true)",
                ),
                config,
            ))
        } else {
            let config =
                TempStorageConfig::new(local_temp_dir, memory_buffer_threshold, max_memory_buffer);
            TempStorageEnum::Local(LocalTempStorage::new(config))
        }
    }

    // ========================================================================
    // Delegated Methods (Static Dispatch)
    // ========================================================================

    /// Get storage configuration
    pub fn config(&self) -> &TempStorageConfig {
        match self {
            TempStorageEnum::Local(s) => s.config(),
            TempStorageEnum::Curvine(s) => s.config(),
        }
    }

    /// Create directory for upload session
    pub fn create_dir(&self, upload_id: &str) -> impl Future<Output = Result<(), String>> + Send {
        let this = self.clone();
        let upload_id = upload_id.to_string();
        async move {
            match this {
                TempStorageEnum::Local(s) => s.create_dir(&upload_id).await,
                TempStorageEnum::Curvine(s) => s.create_dir(&upload_id).await,
            }
        }
    }

    /// Remove entire upload session directory (cleanup after complete/abort)
    pub fn cleanup(&self, upload_id: &str) -> impl Future<Output = Result<(), String>> + Send {
        let this = self.clone();
        let upload_id = upload_id.to_string();
        async move {
            match this {
                TempStorageEnum::Local(s) => s.cleanup(&upload_id).await,
                TempStorageEnum::Curvine(s) => s.cleanup(&upload_id).await,
            }
        }
    }

    /// Check if using distributed storage
    pub fn is_distributed(&self) -> bool {
        matches!(self, TempStorageEnum::Curvine(_))
    }

    /// Get storage type name for logging
    pub fn storage_type(&self) -> &'static str {
        match self {
            TempStorageEnum::Local(_) => "local",
            TempStorageEnum::Curvine(_) => "curvine",
        }
    }

    /// Stream write part data, returns ETag (MD5 hash)
    pub async fn write_part_stream(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut crate::utils::io::AsyncReadEnum,
    ) -> Result<String, String> {
        match self {
            TempStorageEnum::Local(s) => s.write_part_stream(upload_id, part_number, body).await,
            TempStorageEnum::Curvine(s) => s.write_part_stream(upload_id, part_number, body).await,
        }
    }

    /// Stream read part data to Writer
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        writer: &mut W,
    ) -> Result<(), String> {
        match self {
            TempStorageEnum::Local(s) => s.read_part_stream(upload_id, part_number, writer).await,
            TempStorageEnum::Curvine(s) => s.read_part_stream(upload_id, part_number, writer).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_storage_config() {
        let config = TempStorageConfig::new("/tmp/test".to_string(), 1024 * 1024, 16 * 1024 * 1024);
        assert_eq!(config.temp_dir, "/tmp/test");
        assert_eq!(config.memory_buffer_threshold, 1024 * 1024);
        assert_eq!(config.max_memory_buffer, 16 * 1024 * 1024);
    }

    #[test]
    fn test_local_temp_storage_paths() {
        let config = TempStorageConfig::new("/tmp/curvine".to_string(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        assert_eq!(
            storage.part_path("upload-123", 1),
            "/tmp/curvine/upload-123/1"
        );
        assert_eq!(storage.dir_path("upload-123"), "/tmp/curvine/upload-123");
    }

    #[test]
    fn test_factory_creates_local_storage() {
        let storage = TempStorageEnum::create(
            false, // enable_distributed = false (enable_s3_gateway=false)
            "/tmp/local".to_string(),
            "/.s3temp".to_string(),
            1024,
            4096,
            None,
        );

        assert!(!storage.is_distributed());
        assert_eq!(storage.storage_type(), "local");
        assert_eq!(storage.config().temp_dir, "/tmp/local");
        assert_eq!(storage.config().memory_buffer_threshold, 1024);
        assert_eq!(storage.config().max_memory_buffer, 4096);
    }

    #[test]
    fn test_factory_creates_curvine_storage() {
        let result = std::panic::catch_unwind(|| {
            TempStorageEnum::create(
                true, // enable_distributed = true (enable_s3_gateway=true)
                "/tmp/local".to_string(),
                "/.s3temp".to_string(),
                1024,
                4096,
                None, // fs is None, should panic
            );
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_config_buffer_size() {
        let config1 = TempStorageConfig::new("/tmp".to_string(), 1024, 0);
        assert_eq!(config1.buffer_size(), DEFAULT_BUFFER_SIZE);

        let config2 = TempStorageConfig::new("/tmp".to_string(), 1024, 16 * 1024 * 1024);
        assert_eq!(config2.buffer_size(), 16 * 1024 * 1024);

        let config3 = TempStorageConfig::new("/tmp".to_string(), 1024, 32 * 1024);
        assert_eq!(config3.buffer_size(), 32 * 1024);

        let config4 = TempStorageConfig::new("/tmp".to_string(), 1024, 1024);
        assert_eq!(config4.buffer_size(), MIN_BUFFER_SIZE);
    }

    #[test]
    fn test_storage_type_discrimination() {
        let local_storage = TempStorageEnum::create(
            false,
            "/tmp/local".to_string(),
            "/.s3temp".to_string(),
            1024,
            4096,
            None,
        );

        assert_eq!(local_storage.storage_type(), "local");
        assert!(!local_storage.is_distributed());

        match local_storage {
            TempStorageEnum::Local(_) => {}
            TempStorageEnum::Curvine(_) => panic!("Should be Local storage"),
        }
    }

    #[tokio::test]
    async fn test_local_storage_create_dir() {
        use std::env;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-123";
        let result = storage.create_dir(upload_id).await;
        assert!(result.is_ok());

        let expected_dir = std::path::PathBuf::from(&temp_path).join(upload_id);
        assert!(expected_dir.exists());
        assert!(expected_dir.is_dir());
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    #[tokio::test]
    async fn test_local_storage_cleanup() {
        use std::env;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-456";
        storage.create_dir(upload_id).await.unwrap();

        let expected_dir = std::path::PathBuf::from(&temp_path).join(upload_id);
        assert!(expected_dir.exists());

        let result = storage.cleanup(upload_id).await;
        assert!(result.is_ok());
        assert!(!expected_dir.exists());
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    #[tokio::test]
    async fn test_local_storage_write_read_stream() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-stream";
        storage.create_dir(upload_id).await.unwrap();

        let small_data = b"Hello, World!";
        let mut body = crate::utils::io::AsyncReadEnum::BufCursor(tokio::io::BufReader::new(
            Cursor::new(small_data.to_vec()),
        ));

        let etag = storage.write_part_stream(upload_id, 1, &mut body).await;
        assert!(etag.is_ok());
        let etag_value = etag.unwrap();
        assert!(etag_value.starts_with('"'));
        assert!(etag_value.ends_with('"'));

        let part_path = storage.part_path(upload_id, 1);
        let file_data = tokio::fs::read(&part_path).await.unwrap();
        assert_eq!(file_data, small_data);

        let large_data: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        let mut body2 = crate::utils::io::AsyncReadEnum::BufCursor(tokio::io::BufReader::new(
            Cursor::new(large_data.clone()),
        ));

        let etag2 = storage.write_part_stream(upload_id, 2, &mut body2).await;
        assert!(etag2.is_ok());

        let part_path2 = storage.part_path(upload_id, 2);
        let file_data2 = tokio::fs::read(&part_path2).await.unwrap();
        assert_eq!(file_data2, large_data);

        storage.cleanup(upload_id).await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }
}
