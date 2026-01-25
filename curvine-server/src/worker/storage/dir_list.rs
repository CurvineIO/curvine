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

use crate::worker::storage::{ChoosingPolicy, RobinChoosingPolicy, VfsDir};
use curvine_common::state::{ExtendedBlock, FileType, StorageType};
use indexmap::map::Values;
use indexmap::IndexMap;
use log::{info, warn};
use orpc::common::ByteUnit;
use orpc::CommonResult;
use std::ops::Index;

// Directory list.
pub struct DirList {
    pub(crate) dirs: IndexMap<u32, VfsDir>,
    pub(crate) chooser: ChoosingPolicy,
}

impl DirList {
    pub fn new(list: Vec<VfsDir>) -> Self {
        let mut dirs = IndexMap::new();
        for dir in list {
            if dirs.contains_key(&dir.id()) {
                panic!("Storage ID hash conflict")
            }
            dirs.insert(dir.id(), dir);
        }

        Self {
            dirs,
            chooser: ChoosingPolicy::Robin(RobinChoosingPolicy::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.dirs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dir_iter(&self) -> Values<'_, u32, VfsDir> {
        self.dirs.values()
    }

    pub fn get_dir(&self, dir_id: u32) -> Option<&VfsDir> {
        self.dirs.get(&dir_id)
    }

    pub fn get_dir_index(&self, index: usize) -> Option<&VfsDir> {
        self.dirs.get_index(index).map(|x| x.1)
    }

    // Get the total capacity of all storage directories.
    pub fn capacity(&self) -> i64 {
        let mut capacity = 0i64;
        for dir in self.dir_iter() {
            capacity += dir.capacity();
        }

        capacity
    }

    // Get the total available capacity of all storage
    pub fn available(&self) -> i64 {
        let mut capacity = 0i64;
        for dir in self.dir_iter() {
            capacity += dir.available();
        }

        capacity
    }

    // How much space is used for internal storage
    pub fn fs_used(&self) -> i64 {
        let mut used = 0i64;
        for dir in self.dir_iter() {
            used += dir.fs_used();
        }

        used
    }

    pub fn choose_dir(&mut self, block: &ExtendedBlock) -> CommonResult<&VfsDir> {
        self.chooser.choose_dir(&self.dirs, block)
    }

    // Test-specific.
    pub fn choose_dir_with_str<S: AsRef<str>>(
        &mut self,
        stg_type: StorageType,
        size_str: S,
    ) -> CommonResult<&VfsDir> {
        let block_size = ByteUnit::from_str(size_str.as_ref())?.as_byte();
        let block = ExtendedBlock::new(0, block_size as i64, stg_type, FileType::File);
        self.choose_dir(&block)
    }

    pub fn add_dir(&mut self, dir: VfsDir) {
        if !self.dirs.contains_key(&dir.id()) {
            info!(
                "Add new vfs dir, storage_type: {:?}, path: {}, device id: {}, capacity: {}, available: {}",
                dir.storage_type(),
                dir.path_str(),
                dir.device_id(),
                ByteUnit::byte_to_string(dir.capacity() as u64),
                ByteUnit::byte_to_string(dir.available() as u64),
            );
            self.dirs.insert(dir.id(), dir);
        } else {
            warn!("Dir {:?} already exists", dir)
        }
    }

    pub fn remove_dir(&mut self, dir: &VfsDir) -> CommonResult<()> {
        if self.dirs.swap_remove(&dir.id()).is_some() {
            info!(
                "Remove storage dir, [{:?}:{}]{:?}",
                dir.storage_type(),
                dir.capacity(),
                dir.base_path()
            );
        }

        Ok(())
    }
    pub fn choose_dir_for_container(&mut self, total_size: i64) -> CommonResult<&VfsDir> {
        // Create a temporary block for directory selection
        let block = ExtendedBlock::new(0, total_size, StorageType::default(), FileType::Container);
        println!(
            "DEBUG at DirList, at choose_dir_for_container, block: {:?}",
            block
        );
        self.choose_dir(&block)
    }
}

impl Index<usize> for DirList {
    type Output = VfsDir;

    fn index(&self, index: usize) -> &Self::Output {
        &self.dirs[index]
    }
}

#[cfg(test)]
mod tests {
    use crate::worker::storage::{DirList, VfsDir};
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;
    use orpc::common::{ByteUnit, FileUtils};
    use orpc::CommonResult;

    #[test]
    fn robin() -> CommonResult<()> {
        let id = "";

        FileUtils::delete_path("../testing/robin", true)?;
        let dirs = [
            WorkerDataDir::from_str("[MEM:10MB]../testing/robin/d1")?,
            WorkerDataDir::from_str("[SSD:100MB]../testing/robin/d2")?,
            WorkerDataDir::from_str("[SSD:100MB]../testing/robin/d3")?,
        ];

        let vfs_dir = vec![
            VfsDir::from_dir(id, dirs[0].clone())?,
            VfsDir::from_dir(id, dirs[1].clone())?,
            VfsDir::from_dir(id, dirs[2].clone())?,
        ];

        let mut list = DirList::new(vfs_dir);

        let mem = list.choose_dir_with_str(StorageType::Mem, "5MB")?;
        println!("choose mem: {:?}", mem.base_path());
        assert_eq!(dirs[0].path_str(), mem.path_str());

        println!("available {:?}", list.available());

        let ssd1 = list.choose_dir_with_str(StorageType::Mem, "90MB")?;
        println!("choose ssd1: {:?}", ssd1.base_path());
        assert_eq!(dirs[1].path_str(), ssd1.path_str());

        let ssd2 = list.choose_dir_with_str(StorageType::Mem, "90MB")?;
        println!("choose ssd2: {:?}", ssd2.base_path());
        assert_eq!(dirs[2].path_str(), ssd2.path_str());

        // Check capacity
        println!(
            "capacity = {}, available = {}",
            ByteUnit::byte_to_string(list.capacity() as u64),
            ByteUnit::byte_to_string(list.available() as u64)
        );
        // assert_eq!(list.capacity(), 21 * ByteUnit::GB as i64);
        // assert_eq!(list.available(), 2500 * ByteUnit::MB as i64);

        Ok(())
    }
}
