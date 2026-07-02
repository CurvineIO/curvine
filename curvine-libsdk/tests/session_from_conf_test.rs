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

use curvine_libsdk::FilesystemConf;

#[test]
fn master_addrs_build_cluster_conf() {
    let fs_conf =
        FilesystemConf::with_master_addrs(["10.0.0.1:8995", "10.0.0.2:8995"]).expect("fs conf");
    let cluster = fs_conf.into_cluster_conf().expect("cluster conf");
    assert_eq!(cluster.client.master_addrs.len(), 2);
    assert_eq!(cluster.client.master_addrs[0].hostname, "10.0.0.1");
    assert_eq!(cluster.client.master_addrs[0].port, 8995);
}

#[test]
fn empty_master_addrs_is_rejected() {
    let err = FilesystemConf::with_master_addrs(Vec::<&str>::new()).unwrap_err();
    assert!(err.to_string().contains("master_addrs"));
}

#[cfg(feature = "rust-sdk")]
#[test]
fn sdk_connect_masters_rejects_empty() {
    let result = curvine_libsdk::lib_curvine::LibCurvine::connect_masters(Vec::<String>::new());
    assert!(result.is_err());
    let err = result.err().expect("error");
    assert!(err.to_string().contains("master_addrs"));
}
