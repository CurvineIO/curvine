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

pub use curvine_config::*;
pub use curvine_fault::FaultHttpConfig;
pub use curvine_raft::conf::JournalConfExt;

#[cfg(test)]
mod tests {
    use curvine_config::{ClusterConf, WorkerDataDir};
    use curvine_model::StorageType;
    use curvine_runtime::common::ByteUnit;

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
