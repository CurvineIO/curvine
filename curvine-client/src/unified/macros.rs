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
    ($enum_name:ident { $($variant:ident($ty:ty)),+ }) => {
        impl ::curvine_common::fs::Writer for $enum_name {
            fn status(&self) -> &::curvine_common::state::FileStatus {
                match self {
                    $( $enum_name::$variant(v) => v.status(), )+
                }
            }

            fn path(&self) -> &::curvine_common::fs::Path {
                match self {
                    $( $enum_name::$variant(v) => v.path(), )+
                }
            }

            fn pos(&self) -> i64 {
                match self {
                    $( $enum_name::$variant(v) => v.pos(), )+
                }
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match self {
                    $( $enum_name::$variant(v) => v.pos_mut(), )+
                }
            }

            fn chunk_mut(&mut self) -> &mut ::bytes::BytesMut {
                match self {
                    $( $enum_name::$variant(v) => v.chunk_mut(), )+
                }
            }

            fn chunk_size(&self) -> usize {
                match self {
                    $( $enum_name::$variant(v) => v.chunk_size(), )+
                }
            }

            async fn write_chunk(&mut self, chunk: ::orpc::sys::DataSlice) -> ::curvine_common::FsResult<i64> {
                match self {
                    $( $enum_name::$variant(v) => v.write_chunk(chunk).await, )+
                }
            }

            async fn flush(&mut self) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(v) => v.flush().await, )+
                }
            }

            async fn complete(&mut self) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(v) => v.complete().await, )+
                }
            }

            async fn cancel(&mut self) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(v) => v.cancel().await, )+
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_reader_for_enum {
    ($enum_name:ident { $($variant:ident($ty:ty)),+ }) => {
        impl ::curvine_common::fs::Reader for $enum_name {
            fn path(&self) -> &::curvine_common::fs::Path {
                match self {
                    $( $enum_name::$variant(v) => v.path(), )+
                }
            }

            fn len(&self) -> i64 {
                match self {
                    $( $enum_name::$variant(v) => v.len(), )+
                }
            }

            fn chunk_mut(&mut self) -> &mut ::orpc::sys::DataSlice {
                match self {
                    $( $enum_name::$variant(v) => v.chunk_mut(), )+
                }
            }

            fn chunk_size(&self) -> usize {
                match self {
                    $( $enum_name::$variant(v) => v.chunk_size(), )+
                }
            }

            fn pos(&self) -> i64 {
                match self {
                    $( $enum_name::$variant(v) => v.pos(), )+
                }
            }

            fn pos_mut(&mut self) -> &mut i64 {
                match self {
                    $( $enum_name::$variant(v) => v.pos_mut(), )+
                }
            }

            async fn read_chunk0(&mut self) -> ::curvine_common::FsResult<::orpc::sys::DataSlice> {
                match self {
                    $( $enum_name::$variant(v) => v.read_chunk0().await, )+
                }
            }

            async fn seek(&mut self, pos: i64) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(v) => v.seek(pos).await, )+
                }
            }

            async fn complete(&mut self) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(v) => v.complete().await, )+
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_filesystem_for_enum {
    ($enum_name:ident { $($variant:ident($ty:ty)),+ }) => {
        impl ::curvine_common::fs::FileSystem<$crate::unified::UnifiedWriter, $crate::unified::UnifiedReader, ::curvine_common::conf::UfsConf> for $enum_name
        {
            fn conf(&self) -> &::curvine_common::conf::UfsConf {
                match self {
                    $( $enum_name::$variant(inner) => inner.conf(), )+
                }
            }

            async fn mkdir(&self, path: &::curvine_common::fs::Path, create_parent: bool) -> ::curvine_common::FsResult<bool> {
                match self {
                    $( $enum_name::$variant(inner) => inner.mkdir(path, create_parent).await, )+
                }
            }

            async fn create(&self, path: &::curvine_common::fs::Path, overwrite: bool) -> ::curvine_common::FsResult<$crate::unified::UnifiedWriter> {
                match self {
                    $( $enum_name::$variant(inner) => Ok($crate::unified::UnifiedWriter::$variant(inner.create(path, overwrite).await?)), )+
                }
            }

            async fn append(&self, path: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<$crate::unified::UnifiedWriter> {
                match self {
                    $( $enum_name::$variant(inner) => Ok($crate::unified::UnifiedWriter::$variant(inner.append(path).await?)), )+
                }
            }

            async fn exists(&self, path: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<bool> {
                match self {
                    $( $enum_name::$variant(inner) => inner.exists(path).await, )+
                }
            }

            async fn open(&self, path: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<$crate::unified::UnifiedReader> {
                match self {
                    $( $enum_name::$variant(inner) => Ok($crate::unified::UnifiedReader::$variant(inner.open(path).await?)), )+
                }
            }

            async fn rename(&self, src: &::curvine_common::fs::Path, dst: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<bool> {
                match self {
                    $( $enum_name::$variant(inner) => inner.rename(src, dst).await, )+
                }
            }

            async fn delete(&self, path: &::curvine_common::fs::Path, recursive: bool) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(inner) => inner.delete(path, recursive).await, )+
                }
            }

            async fn get_status(&self, path: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<::curvine_common::state::FileStatus> {
                match self {
                    $( $enum_name::$variant(inner) => inner.get_status(path).await, )+
                }
            }

            async fn list_status(&self, path: &::curvine_common::fs::Path) -> ::curvine_common::FsResult<Vec<::curvine_common::state::FileStatus>> {
                match self {
                    $( $enum_name::$variant(inner) => inner.list_status(path).await, )+
                }
            }

            async fn set_attr(&self, path: &::curvine_common::fs::Path, opts: ::curvine_common::state::SetAttrOpts) -> ::curvine_common::FsResult<()> {
                match self {
                    $( $enum_name::$variant(inner) => inner.set_attr(path, opts).await, )+
                }
            }
        }
    };
}
