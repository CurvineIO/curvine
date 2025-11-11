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

mod vfs_dir;
pub use self::vfs_dir::*;

mod dataset;
pub use self::dataset::*;

mod policy;
pub use self::policy::*;

mod dir_list;
pub use self::dir_list::DirList;

mod version;
pub use self::version::*;

mod dir_state;
pub use self::dir_state::DirState;

mod vfs_dataset;
pub use self::vfs_dataset::VfsDataset;

pub type BlockDataset = VfsDataset;

// Normal block directory.
pub const ACTIVE_DIR: &str = "active";

// Temporary block directory, such as blocks triggered for recovery or replication status
pub const STAGING_DIR: &str = "staging";
