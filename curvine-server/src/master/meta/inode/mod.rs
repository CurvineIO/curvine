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

use orpc::sys::RawPtr;

mod inode_file;
pub use self::inode_file::*;

mod inode_dir;
pub use self::inode_dir::*;

mod inode_view;
pub use self::inode_view::*;

mod inode_path;
pub use self::inode_path::InodePath;

mod inodes_children;
pub use self::inodes_children::*;

pub mod ttl;
pub(crate) use self::ttl::*;

pub const ROOT_INODE_ID: i64 = 1000;

pub const ROOT_INODE_NAME: &str = "";

pub const PATH_SEPARATOR: &str = "/";

pub type InodePtr = RawPtr<InodeView>;

pub const EMPTY_PARENT_ID: i64 = -1;

pub trait Inode {
    // inode id
    fn id(&self) -> i64;

    // Previous level id.
    fn parent_id(&self) -> i64;

    // Is it a directory
    fn is_dir(&self) -> bool;

    fn is_file(&self) -> bool {
        !self.is_dir()
    }

    // Modify time
    fn mtime(&self) -> i64;

    // Access time
    fn atime(&self) -> i64;

    // nlink
    fn nlink(&self) -> u32;
}
