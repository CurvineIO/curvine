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

// Guards the public paths downstream crates used before the infrastructure
// crates were split out of `orpc`. A failure here is a compatibility break.

use orpc::io::block_io::{BlockDevice, BlockIO};
use orpc::io::io_error::IOError;
use orpc::io::net::{ConnState, InetAddr, NetAddress, NetUtils, NodeAddr};
use orpc::io::retry::{LimitedRetry, RetryPolicy, TimeoutRetry};
use orpc::io::spdk_conf::{BdevInfo, NvmeTarget, SpdkConf};
use orpc::io::{IOResult, LocalFile};

fn assert_send<T: Send>() {}

#[test]
fn legacy_io_error_path_is_preserved() {
    assert_send::<IOError>();

    let err = IOError::conn_reset();
    assert!(!err.is_would_block());

    let _: IOResult<()> = Ok(());
}

#[test]
fn legacy_block_io_path_is_preserved() {
    assert_send::<BlockDevice>();
    assert_send::<LocalFile>();

    let _: Option<&dyn BlockIO> = None;
}

#[test]
fn legacy_net_and_retry_paths_are_preserved() {
    assert_send::<InetAddr>();
    assert_send::<NodeAddr>();
    assert_send::<NetAddress>();
    assert_send::<ConnState>();
    assert_send::<NetUtils>();

    assert_eq!(LimitedRetry::new(1, 1).count(), 0);
    assert_send::<TimeoutRetry>();

    let _: Option<&dyn RetryPolicy> = None;
}

#[test]
fn legacy_spdk_conf_path_is_preserved() {
    assert_send::<SpdkConf>();
    assert_send::<NvmeTarget>();
    assert_send::<BdevInfo>();
}
