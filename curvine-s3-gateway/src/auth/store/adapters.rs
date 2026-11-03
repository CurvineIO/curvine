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

use async_trait::async_trait;
use curvine_client::unified::UnifiedFileSystem;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::traits::FileSystemAdapter;

#[derive(Clone)]
pub struct CurvineFileSystemAdapter {
    fs: UnifiedFileSystem,
}

impl CurvineFileSystemAdapter {
    pub fn new(fs: UnifiedFileSystem) -> Self {
        Self { fs }
    }
}

#[async_trait]
impl FileSystemAdapter for CurvineFileSystemAdapter {
    type PathType = Path;

    fn path_to_string(&self, path: &Self::PathType) -> String {
        path.to_string()
    }

    async fn read_file(&self, path: &Self::PathType) -> Result<Vec<u8>, String> {
        let mut reader = self
            .fs
            .open(path)
            .await
            .map_err(|e| format!("Failed to open file: {}", e))?;

        let mut content = Vec::new();
        let mut buffer = vec![0u8; 4096];

        loop {
            let bytes_read = reader
                .read_full(&mut buffer)
                .await
                .map_err(|e| format!("Failed to read file: {}", e))?;

            if bytes_read == 0 {
                break;
            }

            content.extend_from_slice(&buffer[..bytes_read]);
        }

        reader
            .complete()
            .await
            .map_err(|e| format!("Failed to complete file read: {}", e))?;

        Ok(content)
    }

    async fn write_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String> {
        let mut writer = self
            .fs
            .create(path, true)
            .await
            .map_err(|e| format!("Failed to create file: {}", e))?;

        writer
            .write(content)
            .await
            .map_err(|e| format!("Failed to write file: {}", e))?;

        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete file write: {}", e))?;

        Ok(())
    }

    async fn append_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String> {
        let mut writer = self
            .fs
            .append(path)
            .await
            .map_err(|e| format!("Failed to open file for append: {}", e))?;

        writer
            .write(content)
            .await
            .map_err(|e| format!("Failed to append to file: {}", e))?;

        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete file append: {}", e))?;

        Ok(())
    }

    async fn get_file_mtime(&self, path: &Self::PathType) -> Result<SystemTime, String> {
        let file_stat = self
            .fs
            .get_status(path)
            .await
            .map_err(|e| format!("Failed to get file status: {}", e))?;

        let file_mtime = SystemTime::UNIX_EPOCH + Duration::from_millis(file_stat.mtime as u64);
        Ok(file_mtime)
    }

    async fn ensure_parent_dir(&self, path: &Self::PathType) -> Result<(), String> {
        if let Ok(Some(parent)) = path.parent() {
            self.fs
                .mkdir(&parent, true)
                .await
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }
        Ok(())
    }

    async fn file_exists(&self, path: &Self::PathType) -> Result<bool, String> {
        match self.fs.get_status(path).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn create_empty_file(&self, path: &Self::PathType) -> Result<(), String> {
        let mut writer = self
            .fs
            .create(path, true)
            .await
            .map_err(|e| format!("Failed to create empty file: {}", e))?;

        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete empty file creation: {}", e))?;

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct LocalFileSystemAdapter;

impl LocalFileSystemAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileSystemAdapter for LocalFileSystemAdapter {
    type PathType = PathBuf;

    fn path_to_string(&self, path: &Self::PathType) -> String {
        path.to_string_lossy().to_string()
    }

    async fn read_file(&self, path: &Self::PathType) -> Result<Vec<u8>, String> {
        let mut file = File::open(path)
            .await
            .map_err(|e| format!("Failed to open file: {}", e))?;

        let mut content = Vec::new();
        file.read_to_end(&mut content)
            .await
            .map_err(|e| format!("Failed to read file: {}", e))?;

        Ok(content)
    }

    async fn write_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String> {
        // The credentials file holds long-lived secret keys: create it 0600
        // and tighten an existing file before overwriting it.
        let mut file = open_private(path, true)
            .await
            .map_err(|e| format!("Failed to create file: {}", e))?;

        file.write_all(content)
            .await
            .map_err(|e| format!("Failed to write file: {}", e))?;

        file.sync_all()
            .await
            .map_err(|e| format!("Failed to sync file: {}", e))?;

        Ok(())
    }

    async fn append_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String> {
        let mut file = open_private(path, false)
            .await
            .map_err(|e| format!("Failed to open file for append: {}", e))?;

        file.write_all(content)
            .await
            .map_err(|e| format!("Failed to append to file: {}", e))?;

        file.sync_all()
            .await
            .map_err(|e| format!("Failed to sync file: {}", e))?;

        Ok(())
    }

    async fn get_file_mtime(&self, path: &Self::PathType) -> Result<SystemTime, String> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| format!("Failed to get file metadata: {}", e))?;

        metadata
            .modified()
            .map_err(|e| format!("Failed to get file modification time: {}", e))
    }

    async fn ensure_parent_dir(&self, path: &Self::PathType) -> Result<(), String> {
        use std::os::unix::fs::PermissionsExt;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
            // The directory holds the credentials file, so keep it owner-only.
            tokio::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(|e| format!("Failed to restrict directory permissions: {}", e))?;
        }
        Ok(())
    }

    async fn file_exists(&self, path: &Self::PathType) -> Result<bool, String> {
        Ok(path.exists())
    }

    async fn create_empty_file(&self, path: &Self::PathType) -> Result<(), String> {
        open_private(path, true)
            .await
            .map_err(|e| format!("Failed to create empty file: {}", e))?;
        Ok(())
    }
}

/// Opens a credentials file for writing with owner-only permissions.
///
/// `File::create` uses `0666 & ~umask` (typically `0644`), which would expose
/// every stored secret key to all local users. The file is created `0600` and
/// an existing file is tightened to `0600` before it is opened, so a
/// pre-existing loose file cannot keep its permissions.
async fn open_private(path: &PathBuf, truncate: bool) -> std::io::Result<File> {
    use std::os::unix::fs::PermissionsExt;

    const PRIVATE: u32 = 0o600;

    if let Ok(meta) = tokio::fs::metadata(path).await {
        if meta.permissions().mode() & 0o077 != 0 {
            tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(PRIVATE)).await?;
        }
    }

    let mut opts = OpenOptions::new();
    opts.write(true)
        .create(true)
        .append(!truncate)
        .truncate(truncate)
        .mode(PRIVATE);
    opts.open(path).await
}

pub fn expand_path(path: &str) -> Result<PathBuf, String> {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            let mut home_path = PathBuf::from(home);
            home_path.push(stripped);
            Ok(home_path)
        } else {
            curvine_core_error::err_box!("HOME environment variable not set")
        }
    } else if path == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            Ok(PathBuf::from(home))
        } else {
            curvine_core_error::err_box!("HOME environment variable not set")
        }
    } else {
        Ok(PathBuf::from(path))
    }
}
