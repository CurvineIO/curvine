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

use crate::fs::operator::*;
use crate::raw::fuse_abi::*;
use crate::raw::FuseDirentList;
use crate::{err_fuse, FuseResult};
use orpc::sys::DataSlice;
use std::future::Future;
use tokio_util::bytes::BytesMut;

pub trait FileSystem: Send + Sync + 'static {
    fn init(&self, op: Init<'_>) -> impl Future<Output = FuseResult<fuse_init_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn lookup(&self, op: Lookup<'_>) -> impl Future<Output = FuseResult<fuse_entry_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn get_xattr(&self, op: GetXAttr<'_>) -> impl Future<Output = FuseResult<BytesMut>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn list_xattr(&self, op: ListXAttr<'_>) -> impl Future<Output = FuseResult<BytesMut>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn set_xattr(&self, op: SetXAttr<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn remove_xattr(&self, op: RemoveXAttr<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn get_attr(&self, op: GetAttr<'_>) -> impl Future<Output = FuseResult<fuse_attr_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn set_attr(&self, op: SetAttr<'_>) -> impl Future<Output = FuseResult<fuse_attr_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn access(&self, op: Access<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn open_dir(&self, op: OpenDir<'_>) -> impl Future<Output = FuseResult<fuse_open_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn stat_fs(&self, op: StatFs<'_>) -> impl Future<Output = FuseResult<fuse_kstatfs>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn mkdir(&self, op: MkDir<'_>) -> impl Future<Output = FuseResult<fuse_entry_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn fuse_allocate(&self, op: FAllocate<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn release_dir(&self, op: ReleaseDir<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn read_dir(&self, op: ReadDir<'_>) -> impl Future<Output = FuseResult<FuseDirentList>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn read_dir_plus(
        &self,
        op: ReadDirPlus<'_>,
    ) -> impl Future<Output = FuseResult<FuseDirentList>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn read(&self, op: Read<'_>) -> impl Future<Output = FuseResult<Vec<DataSlice>>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn open(&self, op: Open<'_>) -> impl Future<Output = FuseResult<fuse_open_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn create(&self, op: Create<'_>) -> impl Future<Output = FuseResult<fuse_create_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn write(&self, op: Write<'_>) -> impl Future<Output = FuseResult<fuse_write_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn flush(&self, op: Flush<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn release(&self, op: Release<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn mk_nod(&self, op: MkNod<'_>) -> impl Future<Output = FuseResult<fuse_entry_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn forget(&self, op: Forget<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn batch_forget(&self, op: BatchForget<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn unlink(&self, op: Unlink<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn link(&self, op: Link<'_>) -> impl Future<Output = FuseResult<fuse_entry_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn rm_dir(&self, op: RmDir<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn rename(&self, op: Rename<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn interrupt(&self, op: Interrupt<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn fsync(&self, op: FSync<'_>) -> impl Future<Output = FuseResult<()>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn symlink(&self, op: Symlink<'_>) -> impl Future<Output = FuseResult<fuse_entry_out>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn readlink(&self, op: Readlink<'_>) -> impl Future<Output = FuseResult<BytesMut>> + Send {
        async move { err_fuse!(libc::ENOSYS, "{:?}", op) }
    }

    fn unmount(&self) {}
}
