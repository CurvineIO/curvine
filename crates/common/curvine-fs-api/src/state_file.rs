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
use curvine_io::IOResult;
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};

pub struct StateWriter {
    path: String,
    inner: BufWriter<File>,
}

impl StateWriter {
    pub fn new<T: AsRef<str>>(path: T) -> IOResult<Self> {
        let file = File::create(path.as_ref())?;
        let inner = BufWriter::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
    }

    /// Creates a new state file that only the current user can read or write.
    ///
    /// The file is created exclusively and without following symlinks, with
    /// mode `0600`, so a file or symlink planted at `path` is never opened or
    /// truncated. The parent directory must not be writable by other users
    /// unless it has the sticky bit, as `/tmp` does.
    #[cfg(unix)]
    pub fn new_private<T: AsRef<str>>(path: T) -> IOResult<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        let file_path = std::path::Path::new(path.as_ref());
        let dir = match file_path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => std::path::Path::new("."),
        };
        check_state_dir(dir)?;

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(file_path)?;
        let inner = BufWriter::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
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
        let inner = BufReader::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
    }

    /// Opens a state file written by `StateWriter::new_private`.
    ///
    /// Fails if `path` is a symlink, is not a regular file, is owned by another
    /// user, or can be accessed by group or others.
    #[cfg(unix)]
    pub fn open_private<T: AsRef<str>>(path: T) -> IOResult<Self> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        // O_NONBLOCK keeps a FIFO at `path` from blocking the open until a writer
        // connects; it is then rejected below. It has no effect on regular files.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.as_ref())?;
        let meta = file.metadata()?;
        if !meta.is_file() {
            return Err(state_file_error(format!(
                "state file {} is not a regular file",
                path.as_ref()
            )));
        }
        let uid = unsafe { libc::geteuid() };
        if meta.uid() != uid {
            return Err(state_file_error(format!(
                "state file {} is owned by uid {}, expected {}",
                path.as_ref(),
                meta.uid(),
                uid
            )));
        }
        if meta.mode() & 0o077 != 0 {
            return Err(state_file_error(format!(
                "state file {} has mode {:o}, expected no group or other access",
                path.as_ref(),
                meta.mode() & 0o777
            )));
        }
        let inner = BufReader::with_capacity(FILE_BUFFER_SIZE, file);

        Ok(Self {
            path: path.as_ref().to_string(),
            inner,
        })
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

#[cfg(unix)]
fn state_file_error(msg: String) -> curvine_io::IOError {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, msg).into()
}

/// Checks that `dir` is safe to keep state files in.
///
/// It must be owned by the current effective user or by root, because the owner
/// of a directory can remove files from it even when the sticky bit is set. It
/// must not be writable by group or others unless it has the sticky bit, as
/// `/tmp` does, which stops them from replacing our files.
#[cfg(unix)]
pub fn check_state_dir(dir: &std::path::Path) -> IOResult<()> {
    use std::os::unix::fs::MetadataExt;

    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() {
        return Err(state_file_error(format!(
            "state directory {} is not a directory",
            dir.display()
        )));
    }
    let uid = unsafe { libc::geteuid() };
    if meta.uid() != uid && meta.uid() != 0 {
        return Err(state_file_error(format!(
            "state directory {} is owned by uid {}, expected {} or root",
            dir.display(),
            meta.uid(),
            uid
        )));
    }
    let mode = meta.mode();
    if mode & 0o022 != 0 && mode & 0o1000 == 0 {
        return Err(state_file_error(format!(
            "state directory {} has mode {:o}: writable by other users without the sticky bit",
            dir.display(),
            mode & 0o7777
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{StateReader, StateWriter};
    use curvine_runtime::common::{FastHashMap, FastHashSet, Utils};
    use std::fs;

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

#[cfg(all(test, unix))]
mod private_tests {
    use super::{StateReader, StateWriter};
    use curvine_runtime::common::Utils;
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::PathBuf;

    fn private_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("state-file-{}", Utils::rand_str(12)));
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    fn path_of(dir: &std::path::Path, name: &str) -> String {
        dir.join(name).to_string_lossy().to_string()
    }

    fn mode_of(path: &str) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn new_private_round_trips_with_owner_only_mode() {
        let dir = private_dir();
        let path = path_of(&dir, "state.data");

        let mut writer = StateWriter::new_private(&path).unwrap();
        writer.write_len(7).unwrap();
        writer.write_struct(&"payload".to_string()).unwrap();
        writer.flush().unwrap();
        drop(writer);
        assert_eq!(mode_of(&path), 0o600);

        let mut reader = StateReader::open_private(&path).unwrap();
        assert_eq!(reader.read_len().unwrap(), 7);
        assert_eq!(reader.read_struct::<String>().unwrap(), "payload");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_private_does_not_truncate_an_existing_file() {
        let dir = private_dir();
        let path = path_of(&dir, "state.data");
        fs::write(&path, "keep me").unwrap();

        assert!(StateWriter::new_private(&path).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "keep me");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_private_does_not_follow_a_symlink() {
        let dir = private_dir();
        let target = path_of(&dir, "target");
        let link = path_of(&dir, "state.data");
        fs::write(&target, "keep me").unwrap();
        symlink(&target, &link).unwrap();

        assert!(StateWriter::new_private(&link).is_err());
        assert_eq!(fs::read_to_string(&target).unwrap(), "keep me");

        // A dangling symlink must not be created through either.
        let dangling_target = path_of(&dir, "missing");
        let dangling = path_of(&dir, "dangling.data");
        symlink(&dangling_target, &dangling).unwrap();
        assert!(StateWriter::new_private(&dangling).is_err());
        assert!(!std::path::Path::new(&dangling_target).exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_private_rejects_a_directory_other_users_can_write() {
        let dir = private_dir();
        let path = path_of(&dir, "state.data");

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o777)).unwrap();
        let err = StateWriter::new_private(&path).err().unwrap();
        assert!(err.to_string().contains("sticky"), "{err}");
        assert!(!std::path::Path::new(&path).exists());

        // The sticky bit, as on /tmp, makes a shared directory acceptable.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o1777)).unwrap();
        StateWriter::new_private(&path).unwrap();

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_private_rejects_a_missing_directory() {
        let dir = private_dir();
        let path = path_of(&dir.join("missing"), "state.data");
        assert!(StateWriter::new_private(&path).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_rejects_a_symlink() {
        let dir = private_dir();
        let target = path_of(&dir, "target");
        let link = path_of(&dir, "state.data");
        let mut writer = StateWriter::new_private(&target).unwrap();
        writer.write_len(1).unwrap();
        writer.flush().unwrap();
        symlink(&target, &link).unwrap();

        assert!(StateReader::open_private(&link).is_err());
        assert!(StateReader::open_private(&target).is_ok());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_rejects_group_or_other_access() {
        let dir = private_dir();
        for mode in [0o640, 0o604, 0o644, 0o666] {
            let path = path_of(&dir, &format!("state-{mode:o}.data"));
            fs::write(&path, "1\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            let err = StateReader::open_private(&path).err().unwrap();
            assert!(
                err.to_string().contains("no group or other access"),
                "{err}"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_rejects_a_directory() {
        let dir = private_dir();
        assert!(StateReader::open_private(dir.to_string_lossy()).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_rejects_a_file_of_another_owner() {
        // Changing the owner needs root, so only run where that is possible.
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let dir = private_dir();
        let path = path_of(&dir, "state.data");
        fs::write(&path, "1\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::chown(&path, Some(12345), None).unwrap();

        let err = StateReader::open_private(&path).err().unwrap();
        assert!(err.to_string().contains("owned by uid"), "{err}");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_rejects_a_fifo_without_blocking() {
        let dir = private_dir();
        let path = path_of(&dir, "state.fifo");
        let c_path = std::ffi::CString::new(path.clone()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

        // Opening a FIFO for reading blocks until a writer connects, so run it on
        // another thread to fail the test instead of hanging it.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(StateReader::open_private(&path).is_err());
        });
        let rejected = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("open_private blocked on a FIFO");
        assert!(rejected);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_private_rejects_a_directory_of_another_owner() {
        // Changing the owner needs root, so only run where that is possible.
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let dir = private_dir();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o1777)).unwrap();
        std::os::unix::fs::chown(&dir, Some(12345), None).unwrap();

        let path = path_of(&dir, "state.data");
        let err = StateWriter::new_private(&path).err().unwrap();
        assert!(err.to_string().contains("owned by uid"), "{err}");
        assert!(!std::path::Path::new(&path).exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_private_reports_a_missing_file() {
        let dir = private_dir();
        let path = path_of(&dir, "missing.data");
        let err = StateReader::open_private(&path).err().unwrap();
        assert!(
            err.to_string().to_lowercase().contains("no such file"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
