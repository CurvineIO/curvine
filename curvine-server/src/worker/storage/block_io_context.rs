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

use orpc::io::{BlockDevice, BlockIO, IOError, IOResult, LocalFile};
use orpc::sys::DataSlice;
use orpc::{err_box, try_err};

fn absolute_offset(device_base: i64, block_off: i64) -> IOResult<i64> {
    device_base
        .checked_add(block_off)
        .ok_or_else(|| IOError::from("absolute block offset overflow"))
}

/// Block-relative write session produced by layout.
pub struct BlockWriteContext {
    device: BlockDevice,
    /// Base offset of the block on the backing device (0 for file-backed blocks).
    device_base: i64,
    block_size: i64,
    block_pos: i64,
}

impl BlockWriteContext {
    /// Establish the invariant `device.pos() == device_base + block_pos` by
    /// unconditionally seeking the underlying device. Callers must not rely on
    /// the layout having pre-positioned the device.
    pub fn new(
        mut device: BlockDevice,
        device_base: i64,
        block_size: i64,
        initial_off: i64,
    ) -> IOResult<Self> {
        if initial_off < 0 || initial_off > block_size {
            return err_box!(
                "Invalid initial offset: {}, block length: {}",
                initial_off,
                block_size
            );
        }
        let absolute = absolute_offset(device_base, initial_off)?;
        try_err!(device.seek(absolute));
        Ok(Self {
            device,
            device_base,
            block_size,
            block_pos: initial_off,
        })
    }

    pub fn path(&self) -> &str {
        self.device.path()
    }

    pub fn block_pos(&self) -> i64 {
        self.block_pos
    }

    pub fn supports_resize(&self) -> bool {
        self.device.supports_resize()
    }

    pub fn seek_to(&mut self, block_off: i64) -> IOResult<()> {
        if block_off < 0 || block_off > self.block_size {
            return err_box!(
                "Invalid seek offset: {}, block length: {}",
                block_off,
                self.block_size
            );
        }
        if block_off != self.block_pos {
            let absolute = absolute_offset(self.device_base, block_off)?;
            try_err!(self.device.seek(absolute));
            self.block_pos = block_off;
        }
        Ok(())
    }

    pub fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        let data_len = region.len() as i64;
        let Some(write_end) = self.block_pos.checked_add(data_len) else {
            return err_box!(
                "Write range overflow: offset={}, length={}",
                self.block_pos,
                data_len
            );
        };
        if write_end > self.block_size {
            return err_box!(
                "Write range [{}, {}) exceeds block size {}",
                self.block_pos,
                write_end,
                self.block_size
            );
        }
        try_err!(self.device.write_region(region));
        self.block_pos = write_end;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.device.flush()
    }

    /// Callers must gate on `supports_resize()`; see `WriteHandler::resize`.
    pub fn resize(&mut self, truncate: bool, block_off: i64, len: i64, mode: i32) -> IOResult<()> {
        if block_off < 0 || len < 0 {
            return err_box!("Invalid resize range: offset={}, length={}", block_off, len);
        }
        let resize_end = block_off.checked_add(len).ok_or_else(|| {
            IOError::from(format!(
                "Resize range overflow: offset={}, length={}",
                block_off, len
            ))
        })?;
        if resize_end > self.block_size {
            return err_box!(
                "Resize range [{}, {}) exceeds block size {}",
                block_off,
                resize_end,
                self.block_size
            );
        }
        let absolute = absolute_offset(self.device_base, block_off)?;
        try_err!(self.device.resize(truncate, absolute, len, mode));
        Ok(())
    }

    pub fn device_len(&self) -> i64 {
        self.device.len()
    }
}

/// Block-relative read session produced by layout.
pub struct BlockReadContext {
    device: BlockDevice,
    /// Base offset of the block on the backing device (0 for file-backed blocks).
    device_base: i64,
    block_size: i64,
    block_pos: i64,
}

impl BlockReadContext {
    /// Establish the invariant `device.pos() == device_base + block_pos` by
    /// unconditionally seeking the underlying device. Callers must not rely on
    /// the layout having pre-positioned the device.
    pub fn new(
        mut device: BlockDevice,
        device_base: i64,
        block_size: i64,
        initial_off: i64,
    ) -> IOResult<Self> {
        if initial_off < 0 || initial_off > block_size {
            return err_box!(
                "Invalid initial offset: {}, block length: {}",
                initial_off,
                block_size
            );
        }
        let absolute = absolute_offset(device_base, initial_off)?;
        try_err!(device.seek(absolute));
        Ok(Self {
            device,
            device_base,
            block_size,
            block_pos: initial_off,
        })
    }

    pub fn path(&self) -> &str {
        self.device.path()
    }

    pub fn block_pos(&self) -> i64 {
        self.block_pos
    }

    pub fn seek_to(&mut self, block_off: i64) -> IOResult<()> {
        if block_off < 0 || block_off > self.block_size {
            return err_box!(
                "Invalid seek offset: {}, block length: {}",
                block_off,
                self.block_size
            );
        }
        if block_off != self.block_pos {
            let absolute = absolute_offset(self.device_base, block_off)?;
            try_err!(self.device.seek(absolute));
            self.block_pos = block_off;
        }
        Ok(())
    }

    pub fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let remaining = self.block_size - self.block_pos;
        let read_len = (len as i64).min(remaining);
        if read_len <= 0 {
            return err_box!(
                "offset exceeds block length, length={}, offset={}",
                self.block_size,
                self.block_pos
            );
        }
        let region = self.device.read_region(enable_send_file, read_len as i32)?;
        // Advance by the bytes represented by the returned region.
        self.block_pos += region.len() as i64;
        Ok(region)
    }

    pub fn supports_send_file(&self) -> bool {
        self.device.supports_send_file()
    }

    pub fn as_local_mut(&mut self) -> Option<&mut LocalFile> {
        self.device.as_local_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orpc::common::Utils;
    use std::fs::remove_file;

    /// Open the underlying LocalFile at position 0 (no pre-positioning) so
    /// `BlockWriteContext::new` / `BlockReadContext::new` are solely responsible
    /// for establishing the `device.pos() == device_base + block_pos` invariant.
    /// This mirrors the shared-device scenario (bdev/pool) that the abstraction
    /// targets.
    fn shared_write_device(path: &str) -> IOResult<BlockDevice> {
        Ok(BlockDevice::Local(LocalFile::with_write(path, false)?))
    }

    fn shared_read_device(path: &str) -> IOResult<BlockDevice> {
        Ok(BlockDevice::Local(LocalFile::with_read(path, 0)?))
    }

    fn ensure_parent(path: &str) -> std::io::Result<()> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    #[test]
    fn read_context_translates_block_relative_offsets() -> Result<(), Box<dyn std::error::Error>> {
        let path = Utils::test_file();
        ensure_parent(&path)?;
        LocalFile::write_string(&path, "AAAAbbbbccccDDDD", true)?;

        let device = shared_read_device(&path)?;
        let mut ctx = BlockReadContext::new(device, 4, 8, 0)?;
        let region = ctx.read_region(false, 2)?;
        let bytes = match &region {
            DataSlice::Bytes(b) => &b[..],
            DataSlice::Buffer(b) => &b[..],
            other => panic!("unexpected slice variant: {:?}", other),
        };
        assert_eq!(bytes, b"bb");

        ctx.seek_to(4)?;
        let region = ctx.read_region(false, 4)?;
        let bytes = match &region {
            DataSlice::Bytes(b) => &b[..],
            DataSlice::Buffer(b) => &b[..],
            other => panic!("unexpected slice variant: {:?}", other),
        };
        assert_eq!(bytes, b"cccc");

        let _ = remove_file(&path);
        Ok(())
    }

    /// Non-zero `device_base` plus in-block seek must translate correctly and
    /// still enforce boundary checks against `block_size`, not device length.
    #[test]
    fn write_context_translates_offsets_with_boundary() -> Result<(), Box<dyn std::error::Error>> {
        let path = Utils::test_file();
        ensure_parent(&path)?;
        LocalFile::write_string(&path, "AAAAxxxxxxxxCCCC", true)?;

        let device = shared_write_device(&path)?;
        let mut ctx = BlockWriteContext::new(device, 4, 8, 0)?;

        let data = DataSlice::Bytes(bytes::Bytes::from_static(b"BB"));
        ctx.write_region(&data)?;
        ctx.seek_to(4)?;
        let data = DataSlice::Bytes(bytes::Bytes::from_static(b"YY"));
        ctx.write_region(&data)?;
        assert_eq!(ctx.block_pos(), 6);

        // Seek exactly to EOF of the block is allowed; any subsequent write must fail.
        ctx.seek_to(8)?;
        assert_eq!(ctx.block_pos(), 8);
        let overflow = DataSlice::Bytes(bytes::Bytes::from_static(b"Z"));
        let err = ctx.write_region(&overflow).unwrap_err().to_string();
        assert!(err.contains("exceeds block size"));

        // Beyond block_size seek is rejected.
        let err = ctx.seek_to(9).unwrap_err().to_string();
        assert!(err.contains("Invalid seek offset"));

        drop(ctx);
        let raw = std::fs::read(&path)?;
        assert_eq!(&raw[..4], b"AAAA");
        assert_eq!(&raw[4..6], b"BB");
        assert_eq!(&raw[6..8], b"xx");
        assert_eq!(&raw[8..10], b"YY");
        assert_eq!(&raw[10..12], b"xx");
        assert_eq!(&raw[12..16], b"CCCC");

        let _ = remove_file(&path);
        Ok(())
    }
}
