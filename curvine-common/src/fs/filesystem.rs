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

use crate::fs::Path;
use crate::proto::{GetFileStatusResponse, ListStatusResponse};
use crate::state::{FileStatus, SetAttrOpts};
use crate::utils::ProtoUtils;
use crate::FsResult;
use prost::bytes::BytesMut;
use std::future::Future;

pub trait FileSystem<Writer, Reader> {
    fn mkdir(&self, path: &Path, create_parent: bool) -> impl Future<Output = FsResult<bool>>;

    fn create(&self, path: &Path, overwrite: bool) -> impl Future<Output = FsResult<Writer>>;

    fn append(&self, path: &Path) -> impl Future<Output = FsResult<Writer>>;

    fn exists(&self, path: &Path) -> impl Future<Output = FsResult<bool>>;

    fn open(&self, path: &Path) -> impl Future<Output = FsResult<Reader>>;

    fn rename(&self, src: &Path, dst: &Path) -> impl Future<Output = FsResult<bool>>;

    fn delete(&self, path: &Path, recursive: bool) -> impl Future<Output = FsResult<()>>;

    fn get_status(&self, path: &Path) -> impl Future<Output = FsResult<FileStatus>>;

    fn get_status_bytes(&self, path: &Path) -> impl Future<Output = FsResult<BytesMut>> {
        async move {
            let status = self.get_status(path).await?;
            let rep = GetFileStatusResponse {
                status: ProtoUtils::file_status_to_pb(status),
            };
            Ok(ProtoUtils::encode(rep)?)
        }
    }

    fn list_status(&self, path: &Path) -> impl Future<Output = FsResult<Vec<FileStatus>>>;

    fn list_status_bytes(&self, path: &Path) -> impl Future<Output = FsResult<BytesMut>> {
        async move {
            let statuses = self.list_status(path).await?;
            let statuses = statuses
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect::<Vec<_>>();

            let rep = ListStatusResponse { statuses };
            Ok(ProtoUtils::encode(rep)?)
        }
    }

    fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> impl Future<Output = FsResult<()>>;
}
