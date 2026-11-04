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

//! # Temporary File Manager
//!
//! High-performance temporary file management for S3 operations.
//! Provides optimized strategies for different file sizes and operation types.

use tokio::fs::File;
use tokio::io::AsyncRead;

#[derive(Clone)]
pub struct TempFileManager {
    pub temp_dir: String,
    pub memory_buffer_threshold: usize,
    pub max_memory_buffer: usize,
    pub use_memory_optimization: bool,
}

pub enum TempStorage {
    Memory(Vec<u8>),
    File(File, String),
}

impl TempFileManager {
    pub fn new(
        put_temp_dir: String,
        put_memory_buffer_threshold: usize,
        put_max_memory_buffer: usize,
    ) -> Self {
        Self {
            temp_dir: put_temp_dir,
            memory_buffer_threshold: put_memory_buffer_threshold,
            max_memory_buffer: put_max_memory_buffer,
            use_memory_optimization: true,
        }
    }

    pub async fn create_temp_storage(
        &self,
        content_length: Option<usize>,
        prefix: &str,
    ) -> Result<TempStorage, std::io::Error> {
        use std::os::unix::fs::PermissionsExt;

        // Owner-only: this directory holds in-flight object bodies, and the
        // default is under /tmp.
        tokio::fs::create_dir_all(&self.temp_dir).await?;
        tokio::fs::set_permissions(&self.temp_dir, std::fs::Permissions::from_mode(0o700)).await?;

        if let Some(size) = content_length {
            if self.use_memory_optimization
                && size <= self.memory_buffer_threshold
                && size <= self.max_memory_buffer
            {
                tracing::debug!("Using memory buffer for {} bytes", size);
                return Ok(TempStorage::Memory(Vec::with_capacity(size)));
            }
        }

        let file_name = format!(
            "{}/{}-{}",
            self.temp_dir,
            prefix,
            &uuid::Uuid::new_v4().to_string()[..8]
        );

        tracing::debug!("Creating temp file: {}", file_name);
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            // Owner-only: a part body is user data.
            .mode(0o600)
            .open(&file_name)
            .await?;

        Ok(TempStorage::File(file, file_name))
    }

    pub fn get_stats(&self) -> TempFileStats {
        TempFileStats {
            memory_threshold: self.memory_buffer_threshold,
            max_memory_buffer: self.max_memory_buffer,
            temp_dir: self.temp_dir.clone(),
            memory_optimization_enabled: self.use_memory_optimization,
        }
    }
}

#[derive(Debug)]
pub struct TempFileStats {
    pub memory_threshold: usize,
    pub max_memory_buffer: usize,
    pub temp_dir: String,
    pub memory_optimization_enabled: bool,
}

impl TempStorage {
    pub async fn write_all(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        match self {
            TempStorage::Memory(buffer) => {
                buffer.extend_from_slice(data);
                Ok(())
            }
            TempStorage::File(file, _) => {
                use tokio::io::AsyncWriteExt;
                file.write_all(data).await
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TempStorage::Memory(buffer) => buffer.len(),
            TempStorage::File(_, _) => 0, // Would need to stat the file
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn into_reader(
        mut self,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, std::io::Error> {
        match &mut self {
            TempStorage::Memory(buffer) => {
                let data = std::mem::take(buffer);
                Ok(Box::new(std::io::Cursor::new(data)))
            }
            TempStorage::File(file, _) => {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(0)).await?;
                Err(std::io::Error::other("File-based reader not implemented"))
            }
        }
    }
}

impl Drop for TempStorage {
    fn drop(&mut self) {
        if let TempStorage::File(_, path) = self {
            let path = path.clone();
            tokio::spawn(async move {
                let _ = tokio::fs::remove_file(&path).await;
                tracing::debug!("Cleaned up temp file: {}", path);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// In-flight object bodies must not be readable by other local users, and
    /// the staging directory must not be world-listable.
    #[tokio::test]
    async fn temp_files_are_owner_only() {
        let temp_path = std::env::temp_dir().join(format!("curvine-tmp-{}", uuid::Uuid::new_v4()));
        let manager = TempFileManager::new(temp_path.to_str().unwrap().to_string(), 1024, 1024);

        // A body above the memory threshold is written to a file.
        let storage = manager
            .create_temp_storage(Some(4096), "obj")
            .await
            .unwrap();
        let file_path = match &storage {
            TempStorage::File(_, path) => path.clone(),
            TempStorage::Memory(_) => panic!("a 4 KiB body must spill to a file"),
        };
        let file_mode = std::fs::metadata(&file_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "staged body mode {file_mode:o}");

        let dir_mode = std::fs::metadata(&temp_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "staging dir mode {dir_mode:o}");

        let _ = std::fs::remove_dir_all(&temp_path);
    }
}
