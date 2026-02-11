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

use crate::master::meta::inode::ROOT_INODE_ID;
use orpc::sync::AtomicLong;
use orpc::{err_box, CommonResult};

pub struct InodeId(AtomicLong);

impl InodeId {
    pub const ID_BYTES: i64 = 40;
    pub const SEQ_BYTES: i64 = 24;
    pub const ID_MASK: i64 = (1i64 << Self::ID_BYTES) - 1;
    pub const SEQ_MASK: i64 = (1i64 << Self::SEQ_BYTES) - 1;
}

impl Default for InodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeId {
    pub fn new() -> Self {
        Self(AtomicLong::new(ROOT_INODE_ID))
    }

    pub fn next(&self) -> CommonResult<i64> {
        let id = self.0.next();
        if id > Self::ID_MASK {
            err_box!("inode id exceeds maximum value {}", Self::ID_MASK)
        } else {
            Ok(id)
        }
    }

    pub fn create_block_id(id: i64, seq: i64) -> CommonResult<i64> {
        println!(
            "DEBUG at InodeId, at create_block_id, id: {:?}, seq: {:?}",
            id, seq
        );
        if id > Self::ID_MASK {
            return err_box!("inode id exceeds maximum value {}", Self::ID_MASK);
        }
        if seq > Self::SEQ_MASK {
            return err_box!("seq id exceeds maximum value {}", Self::ID_MASK);
        }

        let block_id = ((id & Self::ID_MASK) << Self::SEQ_BYTES) | (seq & Self::SEQ_MASK);
        Ok(block_id)
    }

    pub fn get_id(id: i64) -> i64 {
        (id >> Self::SEQ_BYTES) & Self::ID_MASK
    }

    pub fn get_seq(id: i64) -> i64 {
        id & Self::SEQ_MASK
    }

    pub fn is_root(id: i64) -> bool {
        Self::get_id(id) == ROOT_INODE_ID
    }

    pub fn current(&self) -> i64 {
        self.0.get()
    }

    pub fn reset(&self, new_value: i64) -> CommonResult<()> {
        loop {
            let c = self.current();
            if new_value < c {
                return err_box!(
                    "Cannot reset to less than the current value: {}, \
                where newValue {}",
                    c,
                    new_value
                );
            }
            if self.0.compare_and_set(c, new_value) {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::master::meta::InodeId;
    use orpc::common::Utils;
    use orpc::CommonResult;

    #[test]
    fn test_id_create() -> CommonResult<()> {
        let inode_id = InodeId::new();

        let file_id = inode_id.next()?;
        let seq_id = Utils::rand_id() as i64 & InodeId::SEQ_MASK;

        let block_id = InodeId::create_block_id(file_id, seq_id)?;

        let file_id_parse = InodeId::get_id(block_id);
        let seq_id_parse = InodeId::get_seq(block_id);

        assert_eq!(file_id, file_id_parse);
        assert_eq!(seq_id, seq_id_parse);

        Ok(())
    }
}
