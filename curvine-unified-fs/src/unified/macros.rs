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

#[macro_export]
macro_rules! impl_writer_for_enum {
    // Accept enum definition and extract variants
    (
        enum $enum_name:ident {
            $(
                $(#[$cfg:meta])*
                $variant:ident($type:ty)
            ),+ $(,)?
        }
    ) => {
        impl ::curvine_fs_api::Writer for $enum_name {
            fn status(&self) -> &::curvine_model::FileStatus {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.status(),
                    )+
                }
            }

            fn path(&self) -> &::curvine_fs_api::Path {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.path(),
                    )+
                }
            }

            fn pos(&self) -> i64 {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.pos(),
                    )+
                }
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.pos_mut(),
                    )+
                }
            }

            fn chunk_mut(&mut self) -> &mut ::bytes::BytesMut {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.chunk_mut(),
                    )+
                }
            }

            fn chunk_size(&self) -> usize {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.chunk_size(),
                    )+
                }
            }

            async fn write_chunk(
                &mut self,
                chunk: ::orpc_rpc::sys::DataSlice,
            ) -> ::curvine_error::FsResult<i64> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.write_chunk(chunk).await,
                    )+
                }
            }

            async fn flush(&mut self) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.flush().await,
                    )+
                }
            }

            async fn complete(&mut self) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.complete().await,
                    )+
                }
            }

            async fn cancel(&mut self) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.cancel().await,
                    )+
                }
            }

            async fn seek(&mut self, pos: i64) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.seek(pos).await,
                    )+
                }
            }

            async fn resize(
                &mut self,
                opts: ::curvine_model::FileAllocOpts,
            ) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.resize(opts).await,
                    )+
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_reader_for_enum {
    // Accept enum definition and extract variants
    (
        enum $enum_name:ident {
            $(
                $(#[$cfg:meta])*
                $variant:ident($type:ty)
            ),+ $(,)?
        }
    ) => {
        #[allow(refining_impl_trait)]
        impl ::curvine_fs_api::Reader for $enum_name {
            fn status(&self) -> &::curvine_model::FileStatus {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.status(),
                    )+
                }
            }

            fn path(&self) -> &::curvine_fs_api::Path {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.path(),
                    )+
                }
            }

            fn len(&self) -> i64 {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.len(),
                    )+
                }
            }

            fn chunk_mut(&mut self) -> &mut ::orpc_rpc::sys::DataSlice {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.chunk_mut(),
                    )+
                }
            }

            fn chunk_size(&self) -> usize {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.chunk_size(),
                    )+
                }
            }

            fn pos(&self) -> i64 {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.pos(),
                    )+
                }
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.pos_mut(),
                    )+
                }
            }

            async fn read_chunk0(&mut self) -> ::curvine_error::FsResult<::orpc_rpc::sys::DataSlice> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.read_chunk0().await,
                    )+
                }
            }

            async fn seek(&mut self, pos: i64) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.seek(pos).await,
                    )+
                }
            }

            async fn complete(&mut self) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.complete().await,
                    )+
                }
            }

            // Forward default methods explicitly to ensure they work correctly
            fn async_read(&mut self, len: Option<usize>) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<::orpc_rpc::sys::DataSlice>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.async_read(len).await,
                        )+
                    }
                }
            }

            fn read_chunk(&mut self, len: Option<usize>) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<::orpc_rpc::sys::DataSlice>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.read_chunk(len).await,
                        )+
                    }
                }
            }

            fn read(&mut self, buf: &mut [u8]) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<usize>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.read(buf).await,
                        )+
                    }
                }
            }

            fn blocking_read(&mut self, rt: &::orpc_rpc::runtime::Runtime) -> ::curvine_error::FsResult<::orpc_rpc::sys::DataSlice> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(v) => v.blocking_read(rt),
                    )+
                }
            }

            fn fuse_read(&mut self, pos: i64, len: usize) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<Vec<::orpc_rpc::sys::DataSlice>>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.fuse_read(pos, len).await,
                        )+
                    }
                }
            }

            fn read_full(&mut self, buf: &mut [u8]) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<usize>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.read_full(buf).await,
                        )+
                    }
                }
            }

            fn read_as_string(&mut self) -> impl ::std::future::Future<Output = ::curvine_error::FsResult<String>> + Send {
                async move {
                    match self {
                        $(
                            $(#[$cfg])*
                            Self::$variant(v) => v.read_as_string().await,
                        )+
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_filesystem_for_enum {
    // Accept enum definition and extract variants
    (
        enum $enum_name:ident {
            $(
                $(#[$cfg:meta])*
                $variant:ident($type:ty)
            ),+ $(,)?
        }
    ) => {
        impl
            ::curvine_fs_api::FileSystem<
                $crate::UnifiedWriter,
                $crate::UnifiedReader,
            > for $enum_name
        {
            fn fs_kind(&self) -> ::curvine_fs_api::FsKind {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.fs_kind(),
                    )+
                }
            }

            async fn mkdir(
                &self,
                path: &::curvine_fs_api::Path,
                create_parent: bool,
            ) -> ::curvine_error::FsResult<bool> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.mkdir(path, create_parent).await,
                    )+
                }
            }

            async fn create(
                &self,
                path: &::curvine_fs_api::Path,
                overwrite: bool,
            ) -> ::curvine_error::FsResult<$crate::UnifiedWriter> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => {
                            let writer = inner.create(path, overwrite).await?;
                            Ok($crate::UnifiedWriter::$variant(writer))
                        }
                    )+
                }
            }

            async fn append(
                &self,
                path: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<$crate::UnifiedWriter> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => {
                            let writer = inner.append(path).await?;
                            Ok($crate::UnifiedWriter::$variant(writer))
                        }
                    )+
                }
            }

            async fn exists(
                &self,
                path: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<bool> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.exists(path).await,
                    )+
                }
            }

            async fn open(
                &self,
                path: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<$crate::UnifiedReader> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => {
                            let reader = inner.open(path).await?;
                            Ok($crate::UnifiedReader::$variant(reader))
                        }
                    )+
                }
            }

            async fn rename(
                &self,
                src: &::curvine_fs_api::Path,
                dst: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<bool> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.rename(src, dst).await,
                    )+
                }
            }

            async fn delete(
                &self,
                path: &::curvine_fs_api::Path,
                recursive: bool,
            ) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.delete(path, recursive).await,
                    )+
                }
            }

            async fn get_status(
                &self,
                path: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<::curvine_model::FileStatus> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.get_status(path).await,
                    )+
                }
            }

            async fn list_status(
                &self,
                path: &::curvine_fs_api::Path,
            ) -> ::curvine_error::FsResult<Vec<::curvine_model::FileStatus>> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.list_status(path).await,
                    )+
                }
            }

            async fn list_options(
                &self,
                path: &::curvine_fs_api::Path,
                opts: ::curvine_model::ListOptions,
            ) -> ::curvine_error::FsResult<Vec<::curvine_model::FileStatus>> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.list_options(path, opts).await,
                    )+
                }
            }

            async fn list_stream(
                &self,
                path: &::curvine_fs_api::Path,
                opts: ::curvine_model::ListOptions,
            ) -> ::curvine_error::FsResult<::curvine_fs_api::ListStream> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.list_stream(path, opts).await,
                    )+
                }
            }

            async fn set_attr(
                &self,
                path: &::curvine_fs_api::Path,
                opts: ::curvine_model::SetAttrOpts,
            ) -> ::curvine_error::FsResult<()> {
                match self {
                    $(
                        $(#[$cfg])*
                        Self::$variant(inner) => inner.set_attr(path, opts).await,
                    )+
                }
            }
        }
    };
}
