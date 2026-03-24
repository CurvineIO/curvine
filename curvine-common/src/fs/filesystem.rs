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

use crate::fs::{FsKind, Path};
use crate::proto::{GetFileStatusResponse, ListStatusResponse};
use crate::state::{FileStatus, ListOptions, SetAttrOpts};
use crate::utils::ProtoUtils;
use crate::FsResult;
use async_stream::stream;
use futures::stream::Stream;
use orpc::err_box;
use prost::bytes::BytesMut;
use std::future::Future;
use std::pin::Pin;

pub trait FileSystem<Writer, Reader> {
    fn fs_kind(&self) -> FsKind;

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

    fn list_options(
        &self,
        _path: &Path,
        _opts: ListOptions,
    ) -> impl Future<Output = FsResult<Vec<FileStatus>>> + Send {
        async move { err_box!("not supported list_options") }
    }

    fn list_options_bytes(
        &self,
        path: &Path,
        opts: ListOptions,
    ) -> impl Future<Output = FsResult<BytesMut>> {
        async move {
            let statuses = self
                .list_options(path, opts)
                .await?
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect();

            let rep = ListStatusResponse { statuses };
            Ok(ProtoUtils::encode(rep)?)
        }
    }

    fn list_stream<'a>(
        &'a self,
        path: &'a Path,
        options: ListOptions,
    ) -> Pin<Box<dyn Stream<Item = FsResult<FileStatus>> + 'a>> {
        let stream = stream! {
            let (limit, mut start_after) = (options.limit, options.start_after);
            loop {
                let opts = ListOptions {
                    limit,
                    start_after: start_after.clone(),
                };
                let list = match self.list_options(path, opts).await {
                    Ok(p) => p,
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                };

                if list.is_empty() {
                    break;
                }

                let n = list.len();
                let last_name = list.last().map(|s| s.name.clone());
                for status in list {
                    yield Ok(status);
                }

                if let Some(l) = limit {
                    if n < l {
                        break;
                    }
                }
                start_after = last_name;
            }
        };

        Box::pin(stream)
    }
}
