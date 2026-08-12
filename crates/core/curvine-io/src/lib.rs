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

mod block_io;
pub use self::block_io::{BlockDevice, BlockIO};

mod cache_manager;
pub use self::cache_manager::{CacheManager, ReadAheadTask};

mod data_slice;
pub use self::data_slice::DataSlice;

mod io_error;
pub use self::io_error::IOError;

mod local_file;
pub use self::local_file::LocalFile;

mod spdk_conf;
pub use self::spdk_conf::{BdevInfo, NvmeTarget, SpdkConf};

pub type IOResult<T> = Result<T, IOError>;

#[cfg(test)]
mod tests {
    use super::{
        BdevInfo, BlockDevice, BlockIO, CacheManager, DataSlice, IOError, IOResult, LocalFile,
        NvmeTarget, ReadAheadTask, SpdkConf,
    };

    #[test]
    fn exports_generic_io_api() {
        fn assert_send<T: Send>() {}

        assert_send::<BlockDevice>();
        assert_send::<BdevInfo>();
        assert_send::<CacheManager>();
        assert_send::<DataSlice>();
        assert_send::<IOError>();
        assert_send::<LocalFile>();
        assert_send::<NvmeTarget>();
        assert_send::<ReadAheadTask>();
        assert_send::<SpdkConf>();

        let _: Option<&dyn BlockIO> = None;
        let _: IOResult<()> = Ok(());
    }
}
