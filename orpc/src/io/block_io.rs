use crate::io::IOResult;
use crate::io::LocalFile;
use crate::sys::DataSlice;
use std::fmt::{Display, Formatter};

// ---------------------------------------------------------------------------
// BlockIO trait — generic block I/O interface
// ---------------------------------------------------------------------------

pub trait BlockIO: Send {
    fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice>;
    fn write_region(&mut self, region: &DataSlice) -> IOResult<()>;
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()>;
    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()>;
    fn flush(&mut self) -> IOResult<()>;
    fn seek(&mut self, pos: i64) -> IOResult<i64>;
    fn pos(&self) -> i64;
    fn len(&self) -> i64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn path(&self) -> &str;
    fn resize(&mut self, truncate: bool, off: i64, len: i64, mode: i32) -> IOResult<()>;
}

// ---------------------------------------------------------------------------
// BlockDevice — zero-cost enum dispatch for LocalFile / SpdkBdev
// ---------------------------------------------------------------------------
/// Enum-based block device that dispatches to either `LocalFile` or `SpdkBdev`
pub enum BlockDevice {
    Local(LocalFile),
    #[cfg(feature = "spdk")]
    Spdk(crate::io::SpdkBdev),
}
/// Macro to delegate a method call to the inner variant.
#[cfg(feature = "spdk")]
macro_rules! delegate {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match $self {
            BlockDevice::Local(f) => f.$method($($arg),*),
            BlockDevice::Spdk(b) => b.$method($($arg),*),
        }
    };
}

#[cfg(not(feature = "spdk"))]
macro_rules! delegate {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match $self {
            BlockDevice::Local(f) => f.$method($($arg),*),
        }
    };
}
impl BlockIO for BlockDevice {
    fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        delegate!(self, read_region, enable_send_file, len)
    }
    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        delegate!(self, write_region, region)
    }
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        delegate!(self, write_all, buf)
    }
    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        delegate!(self, read_all, buf)
    }
    fn flush(&mut self) -> IOResult<()> {
        delegate!(self, flush)
    }
    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        delegate!(self, seek, pos)
    }
    fn pos(&self) -> i64 {
        delegate!(self, pos)
    }
    fn len(&self) -> i64 {
        delegate!(self, len)
    }
    fn path(&self) -> &str {
        delegate!(self, path)
    }
    fn resize(&mut self, truncate: bool, off: i64, len: i64, mode: i32) -> IOResult<()> {
        delegate!(self, resize, truncate, off, len, mode)
    }
}
impl BlockDevice {
    /// Whether this device supports OS page cache read-ahead.
    /// Only `LocalFile` supports this; SPDK bypasses kernel.
    pub fn supports_read_ahead(&self) -> bool {
        matches!(self, BlockDevice::Local(_))
    }
    /// Whether this device supports sendfile (zero-copy kernel→socket).
    /// Only `LocalFile` supports this; SPDK uses userspace DMA buffers.
    pub fn supports_send_file(&self) -> bool {
        matches!(self, BlockDevice::Local(_))
    }
    /// Whether this device supports short-circuit local I/O (filesystem path).
    /// SPDK bdevs have no filesystem path.
    pub fn supports_short_circuit(&self) -> bool {
        matches!(self, BlockDevice::Local(_))
    }
    /// Get the inner `LocalFile`, if this is a local device.
    #[allow(unreachable_patterns)]
    pub fn as_local(&self) -> Option<&LocalFile> {
        match self {
            BlockDevice::Local(f) => Some(f),
            _ => None,
        }
    }
    /// Get the inner `LocalFile` mutably, if this is a local device.
    #[allow(unreachable_patterns)]
    pub fn as_local_mut(&mut self) -> Option<&mut LocalFile> {
        match self {
            BlockDevice::Local(f) => Some(f),
            _ => None,
        }
    }
}
impl Display for BlockDevice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockDevice::Local(file) => write!(f, "BlockDevice::Local({})", file.path()),
            #[cfg(feature = "spdk")]
            BlockDevice::Spdk(bdev) => write!(f, "BlockDevice::Spdk({})", bdev.name()),
        }
    }
}
// Keep BlockIO impl for LocalFile for backward compatibility
// (existing code that uses LocalFile directly as BlockIO).

impl BlockIO for LocalFile {
    fn read_region(&mut self, enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        LocalFile::read_region(self, enable_send_file, len)
    }
    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        LocalFile::write_region(self, region)
    }
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        LocalFile::write_all(self, buf)
    }
    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        LocalFile::read_all(self, buf)
    }
    fn flush(&mut self) -> IOResult<()> {
        LocalFile::flush(self)
    }
    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        LocalFile::seek(self, pos)
    }
    fn pos(&self) -> i64 {
        LocalFile::pos(self)
    }
    fn len(&self) -> i64 {
        LocalFile::len(self)
    }
    fn path(&self) -> &str {
        LocalFile::path(self)
    }
    fn resize(&mut self, truncate: bool, off: i64, len: i64, mode: i32) -> IOResult<()> {
        LocalFile::resize(self, truncate, off, len, mode)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::Utils;
    use crate::io::LocalFile;
    use std::fs::remove_file;

    #[test]
    fn block_device_local_write_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let path = Utils::test_file();

        let test_path = std::path::Path::new(&path);
        if let Some(parent) = test_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = b"hello BlockDevice enum";
        // Write via BlockDevice::Local
        let mut writer = BlockDevice::Local(LocalFile::with_write(&path, true)?);

        assert_eq!(writer.pos(), 0);
        assert!(writer.supports_read_ahead());
        assert!(writer.supports_short_circuit());
        writer.write_all(data).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.pos(), data.len() as i64);
        assert_eq!(writer.path(), path.as_str());
        drop(writer);

        // Read via BlockDevice::Local
        let mut reader = BlockDevice::Local(LocalFile::with_read(&path, 0)?);
        assert_eq!(reader.len(), data.len() as i64);
        let region = reader.read_region(false, data.len() as i32)?;
        assert_eq!(region.len(), data.len());

        // Verify data integrity
        let mut buf = vec![0u8; data.len()];
        let mut reader2 = BlockDevice::Local(LocalFile::with_read(&path, 0)?);
        reader2.read_all(&mut buf)?;
        assert_eq!(&buf, data);

        remove_file(&path)?;
        Ok(())
    }
}
