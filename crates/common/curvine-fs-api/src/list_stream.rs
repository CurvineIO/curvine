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

use std::pin::Pin;

use crate::state::FileStatus;
use crate::FsResult;
use futures::stream::{self, Stream, StreamExt};

pub struct ListStream {
    inner: Pin<Box<dyn Stream<Item = FsResult<FileStatus>> + Send + 'static>>,
}

impl ListStream {
    pub fn new(inner: impl Stream<Item = FsResult<FileStatus>> + Send + 'static) -> Self {
        ListStream {
            inner: Box::pin(inner),
        }
    }

    pub fn from_vec(list: Vec<FileStatus>) -> Self {
        Self::new(stream::iter(list.into_iter().map(Ok)))
    }

    pub async fn collect_vec(&mut self) -> FsResult<Vec<FileStatus>> {
        let mut vec = Vec::new();
        while let Some(item) = self.next().await {
            vec.push(item?);
        }
        Ok(vec)
    }
}

impl From<Vec<FileStatus>> for ListStream {
    fn from(list: Vec<FileStatus>) -> Self {
        Self::from_vec(list)
    }
}

impl Stream for ListStream {
    type Item = FsResult<FileStatus>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
