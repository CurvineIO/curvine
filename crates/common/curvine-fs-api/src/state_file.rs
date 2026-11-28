//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::FILE_BUFFER_SIZE;
use curvine_core_error::try_err;
use curvine_io::{IOError, IOResult};
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub struct StateWriter {
    path: String,
    inner: BufWriter<File>,
}

impl StateWriter {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::create(path.as_ref())?;
        Ok(Self::from_file(path, file))
    }

    /// Creates a state file that only the current user can read or write.
    ///
    /// The file must not exist yet. `create_new` makes the open fail on any
    /// existing entry, including a symlink, so a name planted in a shared
    /// directory cannot redirect the write or truncate another file.
    pub fn create_private<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path.as_ref())
            .map_err(|e| IOError::with_ctx(e, format!("create state file {}", path.as_ref())))?;
        Ok(Self::from_file(path, file))
    }

    fn from_file<T: AsRef<str>>(path: T, file: File) -> Self {
        let inner = BufWriter::with_capacity(FILE_BUFFER_SIZE, file);
        Self {
            path: path.as_ref().to_string(),
            inner,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner
            .get_ref()
            .metadata()
            .map(|x| x.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn write_len(&mut self, len: u64) -> IOResult<()> {
        self.write_struct(&len)
    }

    pub fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        self.inner.write_all(buf)?;
        Ok(())
    }

    pub fn write_struct<T: Serialize>(&mut self, obj: &T) -> IOResult<()> {
        SerdeUtils::serialize_json_into(&mut self.inner, obj)?;
        writeln!(self.inner)?;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.inner.flush()?;
        Ok(())
    }
}

pub struct StateReader {
    path: String,
    inner: BufReader<File>,
}

impl StateReader {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::open(path.as_ref())?;
        Ok(Self::from_file(path, file))
    }

    /// Opens a state file only if it is a regular file, owned by the current
    /// user and not writable by others.
    ///
    /// The open does not follow a symlink, and `O_NONBLOCK` keeps a FIFO
    /// placed at the path from blocking it; the checks then run on the
    /// opened file itself, so the path cannot be swapped after the check.
    pub fn open_private<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let ctx = |e: io::Error| IOError::with_ctx(e, format!("open state file {}", path.as_ref()));
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.as_ref())
            .map_err(ctx)?;
        let meta = file.metadata().map_err(ctx)?;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        check_private_state_file(&meta, euid).map_err(ctx)?;
        Ok(Self::from_file(path, file))
    }

    fn from_file<T: AsRef<str>>(path: T, file: File) -> Self {
        let inner = BufReader::with_capacity(FILE_BUFFER_SIZE, file);
        Self {
            path: path.as_ref().to_string(),
            inner,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.inner
            .get_ref()
            .metadata()
            .map(|x| x.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> IOResult<()> {
        self.inner.read_exact(buf)?;
        Ok(())
    }

    pub fn read_len(&mut self) -> IOResult<u64> {
        self.read_json_line::<u64>()
    }

    pub fn read_struct<T: for<'de> Deserialize<'de>>(&mut self) -> IOResult<T> {
        self.read_json_line()
    }

    fn read_json_line<T: for<'de> Deserialize<'de>>(&mut self) -> IOResult<T> {
        let mut line = String::new();
        self.inner.read_line(&mut line)?;
        let res = try_err!(serde_json::from_str(line.trim_end()));
        Ok(res)
    }
}

// Only the "others" write bit is rejected. State files written before
// `StateWriter::create_private` existed were created with the process umask
// (0644 under 022, 0664 under 002), and a hot upgrade restores the file that
// the previous version wrote; refusing it would leave the mount without a
// daemon.
fn check_private_state_file(meta: &Metadata, euid: u32) -> io::Result<()> {
    if !meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state file is not a regular file",
        ));
    }
    if meta.uid() != euid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "state file is owned by uid {}, expected uid {}",
                meta.uid(),
                euid
            ),
        ));
    }
    if meta.mode() & 0o002 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "state file mode {:o} is writable by others",
                meta.mode() & 0o7777
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_private_state_file, StateReader, StateWriter};
    use curvine_runtime::common::{FastHashMap, FastHashSet, Utils};
    use std::ffi::CString;
    use std::fs;
    use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};

    fn private_dir(name: &str) -> String {
        let dir = Utils::test_sub_dir(format!("state-file-{}-{}", name, Utils::rand_id()));
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    fn write_private(path: &str, value: u64) {
        let mut writer = StateWriter::create_private(path).unwrap();
        writer.write_len(value).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn create_private_round_trips_with_owner_only_mode() {
        let path = format!("{}/state.data", private_dir("round-trip"));
        write_private(&path, 42);

        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.mode() & 0o777, 0o600);
        let mut reader = StateReader::open_private(&path).unwrap();
        assert_eq!(reader.read_len().unwrap(), 42);
    }

    #[test]
    fn create_private_refuses_an_existing_file() {
        let path = format!("{}/state.data", private_dir("existing"));
        fs::write(&path, b"keep").unwrap();

        assert!(StateWriter::create_private(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"keep");
    }

    #[test]
    fn create_private_does_not_follow_a_symlink() {
        let dir = private_dir("symlink");
        let target = format!("{}/victim", dir);
        let dangling = format!("{}/missing", dir);
        fs::write(&target, b"keep").unwrap();
        let link = format!("{}/state.data", dir);
        let dangling_link = format!("{}/dangling.data", dir);
        symlink(&target, &link).unwrap();
        symlink(&dangling, &dangling_link).unwrap();

        assert!(StateWriter::create_private(&link).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"keep");
        assert!(StateWriter::create_private(&dangling_link).is_err());
        assert!(fs::symlink_metadata(&dangling).is_err());
    }

    #[test]
    fn open_private_rejects_a_symlink_to_a_valid_state_file() {
        let dir = private_dir("open-symlink");
        let real = format!("{}/real.data", dir);
        write_private(&real, 7);
        let link = format!("{}/state.data", dir);
        symlink(&real, &link).unwrap();

        assert!(StateReader::open_private(&link).is_err());
        assert!(StateReader::open_private(&real).is_ok());
    }

    #[test]
    fn open_private_rejects_non_regular_files_without_blocking() {
        let dir = private_dir("non-regular");
        assert!(StateReader::open_private(&dir).is_err());

        let fifo = format!("{}/state.fifo", dir);
        let c_fifo = CString::new(fifo.clone()).unwrap();
        // SAFETY: c_fifo is a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(c_fifo.as_ptr(), 0o600) }, 0);
        // A blocking open of a FIFO with no writer would hang this test.
        assert!(StateReader::open_private(&fifo).is_err());
    }

    #[test]
    fn open_private_rejects_files_writable_by_others() {
        let path = format!("{}/state.data", private_dir("writable"));
        write_private(&path, 1);

        for mode in [0o602, 0o666] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            assert!(StateReader::open_private(&path).is_err(), "mode {:o}", mode);
        }
        // Files written by StateWriter::new under a 022 or 002 umask stay readable.
        for mode in [0o644, 0o664] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            assert!(StateReader::open_private(&path).is_ok(), "mode {:o}", mode);
        }
    }

    #[test]
    fn private_check_rejects_a_file_owned_by_another_user() {
        let path = format!("{}/state.data", private_dir("owner"));
        write_private(&path, 1);
        let meta = fs::metadata(&path).unwrap();

        assert!(check_private_state_file(&meta, meta.uid()).is_ok());
        assert!(check_private_state_file(&meta, meta.uid().wrapping_add(1)).is_err());
    }

    #[test]
    fn test_write_read() {
        let test_path = Utils::test_file();
        let _ = fs::remove_file(&test_path);

        let test_len: u64 = 12345;
        let test_tuple = (100u64, "test".to_string());
        let mut test_map = FastHashMap::new();
        test_map.insert("key1".to_string(), 42i32);
        test_map.insert("key2".to_string(), 84i32);
        let mut test_set = FastHashSet::new();
        test_set.insert(1u64);
        test_set.insert(2u64);

        let mut writer = StateWriter::new(&test_path).unwrap();
        writer.write_len(test_len).unwrap();
        writer.write_struct(&test_tuple).unwrap();
        writer.write_struct(&test_map).unwrap();
        writer.write_struct(&test_set).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let mut reader = StateReader::new(&test_path).unwrap();
        assert_eq!(reader.read_len().unwrap(), test_len);
        let read_tuple: (u64, String) = reader.read_struct().unwrap();
        assert_eq!(read_tuple, test_tuple);
        let read_map: FastHashMap<String, i32> = reader.read_struct().unwrap();
        assert_eq!(read_map.len(), test_map.len());
        assert_eq!(read_map.get("key1"), test_map.get("key1"));
        assert_eq!(read_map.get("key2"), test_map.get("key2"));
        let read_set: FastHashSet<u64> = reader.read_struct().unwrap();
        assert_eq!(read_set.len(), 2);
        assert!(read_set.contains(&1));
        assert!(read_set.contains(&2));
    }
}
