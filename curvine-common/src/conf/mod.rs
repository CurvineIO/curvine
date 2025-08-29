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

mod master_conf;
pub use self::master_conf::*;

mod worker_conf;
pub use self::worker_conf::*;

mod cluster_conf;
pub use self::cluster_conf::*;

mod client_conf;
pub use self::client_conf::*;

mod fuse_conf;
pub use self::fuse_conf::FuseConf;

mod journal_conf;
pub use self::journal_conf::JournalConf;

mod size_string;
pub use self::size_string::SizeString;

mod duration_string;
pub use self::duration_string::DurationString;

mod ufs_conf;
pub use self::ufs_conf::UfsConf;
pub use self::ufs_conf::UfsConfBuilder;

mod job_conf;
pub use self::job_conf::JobConf;

#[cfg(test)]
mod tests {
    use crate::conf::{ClusterConf, WorkerDataDir};
    use crate::state::StorageType;
    use orpc::common::ByteUnit;

    #[test]
    fn cluster() {
        let path = "../etc/curvine-cluster.toml";
        let conf = ClusterConf::from(path).unwrap();
        println!("conf = {:#?}", conf)
    }

    #[test]
    fn data_dir() {
        let list = vec![
            ("/disk", WorkerDataDir::from_str("/disk").unwrap()),
            (
                "[SSD]/disk",
                WorkerDataDir::new(StorageType::Ssd, 0, "/disk"),
            ),
            (
                "[1GB]/disk",
                WorkerDataDir::new(StorageType::Disk, ByteUnit::GB, "/disk"),
            ),
            (
                "[MEM:1GB]/disk",
                WorkerDataDir::new(StorageType::Mem, ByteUnit::GB, "/disk"),
            ),
        ];

        for (path, obj) in list {
            let res = WorkerDataDir::from_str(path).unwrap();
            println!("res = {:?}", res);
            assert_eq!(res, obj);
        }
    }
}
