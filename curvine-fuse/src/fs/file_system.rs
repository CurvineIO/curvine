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
use crate::session::FuseResponse;
use crate::{err_fuse, FuseResult};
use tokio_util::bytes::BytesMut;

#[trait_variant::make(Send)]
pub trait FileSystem: Send + Sync + 'static {
    async fn init(&self, op: Init<'_>) -> FuseResult<fuse_init_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn lookup(&self, op: Lookup<'_>) -> FuseResult<fuse_entry_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn get_xattr(&self, op: GetXAttr<'_>) -> FuseResult<BytesMut> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn list_xattr(&self, op: ListXAttr<'_>) -> FuseResult<BytesMut> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn set_xattr(&self, op: SetXAttr<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn remove_xattr(&self, op: RemoveXAttr<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn get_attr(&self, op: GetAttr<'_>) -> FuseResult<fuse_attr_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn set_attr(&self, op: SetAttr<'_>) -> FuseResult<fuse_attr_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn access(&self, op: Access<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn open_dir(&self, op: OpenDir<'_>) -> FuseResult<fuse_open_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn stat_fs(&self, op: StatFs<'_>) -> FuseResult<fuse_kstatfs> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn mkdir(&self, op: MkDir<'_>) -> FuseResult<fuse_entry_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn fuse_allocate(&self, op: FAllocate<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn release_dir(&self, op: ReleaseDir<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn read_dir(&self, op: ReadDir<'_>) -> FuseResult<FuseDirentList> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn read_dir_plus(&self, op: ReadDirPlus<'_>) -> FuseResult<FuseDirentList> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn read(&self, op: Read<'_>, _rep: FuseResponse) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn open(&self, op: Open<'_>) -> FuseResult<fuse_open_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn create(&self, op: Create<'_>) -> FuseResult<fuse_create_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn write(&self, op: Write<'_>, _rep: FuseResponse) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn flush(&self, op: Flush<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn release(&self, op: Release<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn mk_nod(&self, op: MkNod<'_>) -> FuseResult<fuse_entry_out> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn forget(&self, op: Forget<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn batch_forget(&self, op: BatchForget<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn unlink(&self, op: Unlink<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn rm_dir(&self, op: RmDir<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn rename(&self, op: Rename<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn interrupt(&self, op: Interrupt<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    async fn fsync(&self, op: FSync<'_>) -> FuseResult<()> {
        let msg = format!("{:?}", op);
        async move { err_fuse!(libc::ENOSYS, "{}", msg) }
    }

    fn unmount(&self) {}
}
