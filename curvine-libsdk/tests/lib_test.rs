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

#[test]
fn compiles_with_default_java_sdk_feature() {
    let _ = std::any::type_name::<curvine_libsdk::LibFilesystem>();
}

#[cfg(feature = "rust-sdk")]
#[test]
fn compiles_with_rust_sdk_feature() {
    use curvine_libsdk::lib_curvine::{ConnectOptions, LibCurvine};
    let _ = ConnectOptions::MasterAddrs(vec!["127.0.0.1:8995".to_string()]);
    let _ = LibCurvine::builder();
}
