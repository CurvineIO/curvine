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

#[cfg(feature = "oss-hdfs-ffi-test")]
mod tests {
    use std::os::raw::c_longlong;

    #[repr(C)]
    #[derive(Clone, Copy)]
    enum JindoStatus {
        Ok = 0,
        Error = 1,
        FileNotFound = 2,
    }

    #[link(name = "jindosdk_ffi")]
    extern "C" {
        fn jindo_test_default_filesystem_store_shards() -> usize;
        fn jindo_test_max_filesystem_store_shards() -> usize;
        fn jindo_test_filesystem_store_shard_wait_timeout_ms() -> c_longlong;
        fn jindo_test_is_reusable_operation_ctx(status: JindoStatus) -> bool;
    }

    #[test]
    fn filesystem_store_pool_is_bounded_and_lazy() {
        unsafe {
            assert_eq!(jindo_test_default_filesystem_store_shards(), 16);
            assert_eq!(jindo_test_max_filesystem_store_shards(), 64);
            assert_eq!(jindo_test_filesystem_store_shard_wait_timeout_ms(), 3_000);
        }
    }

    #[test]
    fn filesystem_store_pool_reuses_not_found_contexts() {
        unsafe {
            assert!(jindo_test_is_reusable_operation_ctx(JindoStatus::Ok));
            assert!(jindo_test_is_reusable_operation_ctx(
                JindoStatus::FileNotFound
            ));
            assert!(!jindo_test_is_reusable_operation_ctx(JindoStatus::Error));
        }
    }
}
