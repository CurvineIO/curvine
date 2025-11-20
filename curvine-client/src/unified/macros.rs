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
macro_rules! match_variants {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            Self::Cv(v) => v.$method($($arg),*),

            #[cfg(feature = "s3")]
            Self::S3(v) => v.$method($($arg),*),

            #[cfg(feature = "opendal")]
            Self::OpenDAL(v) => v.$method($($arg),*),
        }
    };
}

#[macro_export]
macro_rules! match_variants_async {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            Self::Cv(v) => v.$method($($arg),*).await,

            #[cfg(feature = "s3")]
            Self::S3(v) => v.$method($($arg),*).await,

            #[cfg(feature = "opendal")]
            Self::OpenDAL(v) => v.$method($($arg),*).await,
        }
    };
}

#[macro_export]
macro_rules! match_fs_variants {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            #[cfg(feature = "s3")]
            Self::S3(inner) => inner.$method($($arg),*),

            #[cfg(feature = "opendal")]
            Self::OpenDAL(inner) => inner.$method($($arg),*),
        }
    };
}

#[macro_export]
macro_rules! match_fs_variants_async {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            #[cfg(feature = "s3")]
            Self::S3(inner) => inner.$method($($arg),*).await,

            #[cfg(feature = "opendal")]
            Self::OpenDAL(inner) => inner.$method($($arg),*).await,
        }
    };
}

#[macro_export]
macro_rules! match_fs_variants_writer {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            #[cfg(feature = "s3")]
            Self::S3(inner) => Ok($crate::unified::UnifiedWriter::S3(inner.$method($($arg),*).await?)),

            #[cfg(feature = "opendal")]
            Self::OpenDAL(inner) => Ok($crate::unified::UnifiedWriter::OpenDAL(inner.$method($($arg),*).await?)),
        }
    };
}

#[macro_export]
macro_rules! match_fs_variants_reader {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            #[cfg(feature = "s3")]
            Self::S3(inner) => Ok($crate::unified::UnifiedReader::S3(inner.$method($($arg),*).await?)),

            #[cfg(feature = "opendal")]
            Self::OpenDAL(inner) => Ok($crate::unified::UnifiedReader::OpenDAL(inner.$method($($arg),*).await?)),
        }
    };
}

#[macro_export]
macro_rules! impl_writer_for_enum {
    ($enum_name:ident) => {
        impl ::curvine_common::fs::Writer for $enum_name {
            fn status(&self) -> &::curvine_common::state::FileStatus {
                match_variants!(self, status)
            }

            fn path(&self) -> &::curvine_common::fs::Path {
                match_variants!(self, path)
            }

            fn pos(&self) -> i64 {
                match_variants!(self, pos)
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match_variants!(self, pos_mut)
            }

            fn chunk_mut(&mut self) -> &mut ::bytes::BytesMut {
                match_variants!(self, chunk_mut)
            }

            fn chunk_size(&self) -> usize {
                match_variants!(self, chunk_size)
            }

            async fn write_chunk(
                &mut self,
                chunk: ::orpc::sys::DataSlice,
            ) -> ::curvine_common::FsResult<i64> {
                match_variants_async!(self, write_chunk, chunk)
            }

            async fn flush(&mut self) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, flush)
            }

            async fn complete(&mut self) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, complete)
            }

            async fn cancel(&mut self) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, cancel)
            }

            async fn seek(&mut self, pos: i64) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, seek, pos)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_reader_for_enum {
    ($enum_name:ident) => {
        impl ::curvine_common::fs::Reader for $enum_name {
            fn status(&self) -> &::curvine_common::state::FileStatus {
                match_variants!(self, status)
            }

            fn path(&self) -> &::curvine_common::fs::Path {
                match_variants!(self, path)
            }

            fn len(&self) -> i64 {
                match_variants!(self, len)
            }

            fn chunk_mut(&mut self) -> &mut ::orpc::sys::DataSlice {
                match_variants!(self, chunk_mut)
            }

            fn chunk_size(&self) -> usize {
                match_variants!(self, chunk_size)
            }

            fn pos(&self) -> i64 {
                match_variants!(self, pos)
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match_variants!(self, pos_mut)
            }

            async fn read_chunk0(&mut self) -> ::curvine_common::FsResult<::orpc::sys::DataSlice> {
                match_variants_async!(self, read_chunk0)
            }

            async fn seek(&mut self, pos: i64) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, seek, pos)
            }

            async fn complete(&mut self) -> ::curvine_common::FsResult<()> {
                match_variants_async!(self, complete)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_filesystem_for_enum {
    ($enum_name:ident) => {
        impl
            ::curvine_common::fs::FileSystem<
                $crate::unified::UnifiedWriter,
                $crate::unified::UnifiedReader,
            > for $enum_name
        {
            async fn mkdir(
                &self,
                path: &::curvine_common::fs::Path,
                create_parent: bool,
            ) -> ::curvine_common::FsResult<bool> {
                match_fs_variants_async!(self, mkdir, path, create_parent)
            }

            async fn create(
                &self,
                path: &::curvine_common::fs::Path,
                overwrite: bool,
            ) -> ::curvine_common::FsResult<$crate::unified::UnifiedWriter> {
                match_fs_variants_writer!(self, create, path, overwrite)
            }

            async fn append(
                &self,
                path: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<$crate::unified::UnifiedWriter> {
                match_fs_variants_writer!(self, append, path)
            }

            async fn exists(
                &self,
                path: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<bool> {
                match_fs_variants_async!(self, exists, path)
            }

            async fn open(
                &self,
                path: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<$crate::unified::UnifiedReader> {
                match_fs_variants_reader!(self, open, path)
            }

            async fn rename(
                &self,
                src: &::curvine_common::fs::Path,
                dst: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<bool> {
                match_fs_variants_async!(self, rename, src, dst)
            }

            async fn delete(
                &self,
                path: &::curvine_common::fs::Path,
                recursive: bool,
            ) -> ::curvine_common::FsResult<()> {
                match_fs_variants_async!(self, delete, path, recursive)
            }

            async fn get_status(
                &self,
                path: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<::curvine_common::state::FileStatus> {
                match_fs_variants_async!(self, get_status, path)
            }

            async fn list_status(
                &self,
                path: &::curvine_common::fs::Path,
            ) -> ::curvine_common::FsResult<Vec<::curvine_common::state::FileStatus>> {
                match_fs_variants_async!(self, list_status, path)
            }

            async fn set_attr(
                &self,
                path: &::curvine_common::fs::Path,
                opts: ::curvine_common::state::SetAttrOpts,
            ) -> ::curvine_common::FsResult<()> {
                match_fs_variants_async!(self, set_attr, path, opts)
            }
        }
    };
}
