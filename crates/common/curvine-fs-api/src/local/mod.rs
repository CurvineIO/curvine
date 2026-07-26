//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

mod local_reader;
pub use self::local_reader::LocalReader;

mod local_filesystem;
mod local_writer;

pub use self::local_filesystem::LocalFilesystem;
pub use self::local_writer::LocalWriter;

struct LocalStatusUtils;

impl LocalStatusUtils {
    fn to_epoch_ms(ts: std::io::Result<std::time::SystemTime>) -> i64 {
        ts.ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn metadata_to_file_status(
        path: &crate::Path,
        meta: &std::fs::Metadata,
    ) -> curvine_model::FileStatus {
        curvine_model::FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: meta.is_dir(),
            mtime: Self::to_epoch_ms(meta.modified()),
            atime: Self::to_epoch_ms(meta.accessed()),
            children_num: 0,
            is_complete: true,
            len: meta.len() as i64,
            replicas: 1,
            block_size: 512,
            file_type: if meta.is_dir() {
                curvine_model::FileType::Dir
            } else {
                curvine_model::FileType::File
            },
            mode: 0o777,
            ..Default::default()
        }
    }

    fn file_to_status(
        path: &crate::Path,
        file: &orpc_rpc::io::LocalFile,
    ) -> curvine_error::FsResult<curvine_model::FileStatus> {
        let meta = file.metadata()?;
        Ok(Self::metadata_to_file_status(path, &meta))
    }
}
