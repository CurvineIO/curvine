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

//! Owning wrappers over the subset of the libhdfs C API used to reach COSN.
//!
//! Despite the `hdfs` naming, nothing here is HDFS-specific. libhdfs forwards any
//! name-node string containing `://` straight to `java.net.URI.create`, so
//! handing it the mount URI as written (`cosn://<bucket>`, short or
//! `bucket-appid`) makes it resolve `fs.cosn.impl` and hand back Tencent's
//! `CosFileSystem`.

use curvine_core_error::ErrorExt;
use curvine_error::{FsError, FsResult};
use hdfs_sys::*;
use std::ffi::{CStr, CString};
use std::io;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::sync::Arc;

/// Copies libhdfs's root-cause message for the last failure **on this thread**.
///
/// The returned pointer aliases libhdfs's thread-local state, which libhdfs frees
/// and replaces on the next failure, so the text is copied out here and must not
/// be freed. Being per-thread also means errors have to be built on the thread
/// that made the failing call — for this adapter, inside the `spawn_blocking`
/// closure rather than after it returns.
fn last_root_cause() -> Option<String> {
    let ptr: *mut c_char = unsafe { hdfsGetLastExceptionRootCause() };
    if ptr.is_null() {
        return None;
    }

    let msg = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .trim()
        .to_owned();

    (!msg.is_empty()).then_some(msg)
}

/// Classifies a libhdfs failure, kept separate from `cosn_error` so the mapping
/// is testable without provoking a real errno.
fn map_os_error(os_err: &io::Error, detail: &str, path: &str) -> FsError {
    match os_err.kind() {
        io::ErrorKind::NotFound => FsError::file_not_found(path).ctx(detail.to_owned()),
        io::ErrorKind::AlreadyExists => FsError::file_exists(path).ctx(detail.to_owned()),
        io::ErrorKind::DirectoryNotEmpty => FsError::dir_not_empty(path).ctx(detail.to_owned()),
        _ => FsError::common(format!("{} on {}", detail, path)),
    }
}

/// Builds an `FsError` from the errno a failed libhdfs call left behind,
/// enriched with the Java-side root cause when one is available.
pub fn cosn_error(operation: &str, path: &str) -> FsError {
    let os_err = io::Error::last_os_error();
    let detail = match last_root_cause() {
        Some(cause) => format!("{}: {} ({})", operation, cause, os_err),
        None => format!("{}: {}", operation, os_err),
    };

    map_os_error(&os_err, &detail, path)
}

/// Converts to a `CString`, turning an interior NUL into a normal error rather
/// than a panic — mount properties are user-supplied.
pub fn cstring(value: &str) -> FsResult<CString> {
    CString::new(value).map_err(|e| {
        FsError::common(format!(
            "value {:?} cannot be passed to libhdfs: {}",
            value, e
        ))
    })
}

/// A `hdfsBuilder` under construction, owned until dropped or consumed by
/// [`CosnBuilder::connect`].
pub struct CosnBuilder {
    raw: *mut hdfsBuilder,
    name_node: String,
    // libhdfs stores raw pointers from SetNameNode/SetUserName/ConfSetStr; keep
    // CString values alive until connect() consumes the builder.
    _name_node_c: CString,
    _user_c: Option<CString>,
    _conf_opts: Vec<(CString, CString)>,
}

impl CosnBuilder {
    /// Starts a builder targeting `name_node`, which for COSN is the filesystem
    /// URI `cosn://<bucket>-<appid>` rather than a NameNode address.
    pub fn new(name_node: impl Into<String>) -> FsResult<Self> {
        let name_node = name_node.into();
        let raw = unsafe { hdfsNewBuilder() };
        if raw.is_null() {
            return Err(FsError::common("failed to allocate a libhdfs builder"));
        }

        let name_node_c = cstring(&name_node)?;
        unsafe { hdfsBuilderSetNameNode(raw, name_node_c.as_ptr()) };

        Ok(Self {
            raw,
            name_node,
            _name_node_c: name_node_c,
            _user_c: None,
            _conf_opts: Vec::new(),
        })
    }

    pub fn user(&mut self, user: &str) -> FsResult<()> {
        let value = cstring(user)?;
        unsafe { hdfsBuilderSetUserName(self.raw, value.as_ptr()) };
        self._user_c = Some(value);
        Ok(())
    }

    /// Connects through `FileSystem.newInstance` instead of `FileSystem.get`.
    ///
    /// Hadoop's filesystem cache is keyed by `(scheme, authority, ugi)`, so
    /// without this a second mount of the same bucket would be handed the first
    /// mount's cached instance and silently run on its credentials.
    pub fn force_new_instance(&self) {
        unsafe { hdfsBuilderSetForceNewInstance(self.raw) };
    }

    /// Sets one Hadoop property (e.g. `fs.cosn.userinfo.secretId`) on the fresh
    /// `Configuration` that libhdfs builds for this connection.
    pub fn conf_set(&mut self, key: &str, value: &str) -> FsResult<()> {
        let (key_c, value_c) = (cstring(key)?, cstring(value)?);
        let rc = unsafe { hdfsBuilderConfSetStr(self.raw, key_c.as_ptr(), value_c.as_ptr()) };
        if rc != 0 {
            return Err(cosn_error(
                &format!("set COSN property {}", key),
                &self.name_node,
            ));
        }
        self._conf_opts.push((key_c, value_c));
        Ok(())
    }

    /// Connects, handing ownership of the builder to libhdfs.
    ///
    /// `hdfsBuilderConnect` frees the builder on both its success and failure
    /// paths, so this consumes `self` and suppresses our own `Drop` to avoid a
    /// double free.
    pub fn connect(self) -> FsResult<CosnFs> {
        let this = ManuallyDrop::new(self);
        let raw = unsafe { hdfsBuilderConnect(this.raw) };
        if raw.is_null() {
            return Err(cosn_error("connect to COSN", &this.name_node));
        }
        Ok(CosnFs { raw })
    }
}

impl Drop for CosnBuilder {
    fn drop(&mut self) {
        unsafe { hdfsFreeBuilder(self.raw) };
    }
}

/// A live COSN connection, holding a JNI global reference to a Java
/// `org.apache.hadoop.fs.FileSystem`.
pub struct CosnFs {
    raw: hdfsFS,
}

// libhdfs attaches whichever thread calls into it, and the handle is a JNI
// global reference rather than thread-local state, so it is safe to share.
unsafe impl Send for CosnFs {}
unsafe impl Sync for CosnFs {}

impl CosnFs {
    pub fn as_raw(&self) -> hdfsFS {
        self.raw
    }

    /// Opens a file. `flags` uses the POSIX constants that libhdfs forwards to
    /// `FileSystem.open` / `create` (e.g. `libc::O_RDONLY`).
    pub fn open_file(&self, path: &CStr, flags: i32) -> FsResult<hdfsFile> {
        let raw = unsafe { hdfsOpenFile(self.raw, path.as_ptr(), flags, 0, 0, 0) };
        if raw.is_null() {
            return Err(cosn_error("open file", &path.to_string_lossy()));
        }
        Ok(raw)
    }
}

impl Drop for CosnFs {
    fn drop(&mut self) {
        if unsafe { hdfsDisconnect(self.raw) } != 0 {
            log::warn!(
                "failed to close COSN filesystem: {}",
                io::Error::last_os_error()
            );
        }
    }
}

/// An open COSN stream. `hdfsCloseFile` needs the owning filesystem handle, so
/// the stream keeps the connection alive for as long as it is open.
pub struct CosnFile {
    fs: Arc<CosnFs>,
    raw: hdfsFile,
}

unsafe impl Send for CosnFile {}
unsafe impl Sync for CosnFile {}

impl CosnFile {
    pub fn new(fs: Arc<CosnFs>, raw: hdfsFile) -> Self {
        Self { fs, raw }
    }

    pub fn as_raw(&self) -> hdfsFile {
        self.raw
    }

    pub fn fs_raw(&self) -> hdfsFS {
        self.fs.as_raw()
    }

    /// Reads `[offset, offset+len)` via `hdfsPread`, looping over short reads.
    ///
    /// Hadoop 3.1's libhdfs does not expose `hdfsPreadFully`, so callers must
    /// tolerate partial returns until EOF (`n == 0`) or an error (`n < 0`).
    pub fn pread_loop(&self, offset: i64, len: usize) -> FsResult<Vec<u8>> {
        pread_loop(self.fs_raw(), self.as_raw(), offset, len, "pread")
    }
}

/// Pure short-read loop over `hdfsPread`, extracted so the fill logic can be
/// unit-tested with a mock callback.
pub(crate) fn pread_loop(
    fs: hdfsFS,
    file: hdfsFile,
    offset: i64,
    len: usize,
    path: &str,
) -> FsResult<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let want = (len - filled) as i32;
        let n = unsafe {
            hdfsPread(
                fs,
                file,
                offset + filled as i64,
                buffer[filled..].as_mut_ptr() as *mut std::os::raw::c_void,
                want,
            )
        };
        if n < 0 {
            return Err(cosn_error("pread", path));
        }
        if n == 0 {
            break;
        }
        filled += n as usize;
    }
    buffer.truncate(filled);
    Ok(buffer)
}

/// Test helper: drives the short-read assembly logic without touching libhdfs.
#[cfg(test)]
pub(crate) fn assemble_short_reads(
    chunks: &[&[u8]],
    requested: usize,
) -> Result<Vec<u8>, &'static str> {
    let mut buffer = vec![0u8; requested];
    let mut filled = 0usize;
    let mut chunk_idx = 0usize;
    while filled < requested {
        if chunk_idx >= chunks.len() {
            break; // EOF
        }
        let chunk = chunks[chunk_idx];
        chunk_idx += 1;
        if chunk.is_empty() {
            break; // EOF signal
        }
        let copy = chunk.len().min(requested - filled);
        buffer[filled..filled + copy].copy_from_slice(&chunk[..copy]);
        filled += copy;
    }
    buffer.truncate(filled);
    Ok(buffer)
}

impl Drop for CosnFile {
    fn drop(&mut self) {
        if unsafe { hdfsCloseFile(self.fs.as_raw(), self.raw) } != 0 {
            log::warn!(
                "failed to close COSN stream: {}",
                io::Error::last_os_error()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_error::FsError;

    #[test]
    fn cstring_rejects_interior_nul() {
        let err = cstring("fs.cosn\0.impl").expect_err("interior NUL must be rejected");
        assert!(
            err.to_string().contains("cannot be passed to libhdfs"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn cstring_accepts_ordinary_value() {
        let value = cstring("cosn://bucket-1250000000").expect("plain value must convert");
        assert_eq!(value.to_str().unwrap(), "cosn://bucket-1250000000");
    }

    #[test]
    fn not_found_errno_maps_to_file_not_found() {
        let os_err = io::Error::from(io::ErrorKind::NotFound);
        let err = map_os_error(&os_err, "stat", "cosn://b/missing");
        assert!(
            matches!(err, FsError::FileNotFound(_)),
            "expected FileNotFound, got {:?}",
            err
        );
    }

    #[test]
    fn already_exists_errno_maps_to_file_exists() {
        let os_err = io::Error::from(io::ErrorKind::AlreadyExists);
        let err = map_os_error(&os_err, "create", "cosn://b/dup");
        assert!(
            matches!(err, FsError::FileAlreadyExists(_)),
            "expected FileAlreadyExists, got {:?}",
            err
        );
    }

    #[test]
    fn not_empty_errno_maps_to_dir_not_empty() {
        let os_err = io::Error::from(io::ErrorKind::DirectoryNotEmpty);
        let err = map_os_error(&os_err, "delete", "cosn://b/dir");
        assert!(
            matches!(err, FsError::DirNotEmpty(_)),
            "expected DirNotEmpty, got {:?}",
            err
        );
    }

    // Anything libhdfs reports that has no direct filesystem meaning (auth
    // failures, network errors, Java exceptions) must stay a generic error so the
    // Java-side root cause survives in the message.
    #[test]
    fn other_errno_maps_to_common_and_keeps_detail() {
        let os_err = io::Error::from(io::ErrorKind::PermissionDenied);
        let err = map_os_error(&os_err, "open: AccessDenied", "cosn://b/secret");
        assert!(
            matches!(err, FsError::Common(_)),
            "expected Common, got {:?}",
            err
        );
        let msg = err.to_string();
        assert!(msg.contains("AccessDenied"), "detail lost: {}", msg);
        assert!(msg.contains("cosn://b/secret"), "path lost: {}", msg);
    }

    #[test]
    fn assemble_short_reads_fills_across_chunks() {
        let data = assemble_short_reads(&[b"hel", b"lo wor", b"ld"], 11).unwrap();
        assert_eq!(&data, b"hello world");
    }

    #[test]
    fn assemble_short_reads_stops_on_eof() {
        let data = assemble_short_reads(&[b"ab", b"", b"cd"], 10).unwrap();
        assert_eq!(&data, b"ab");
    }

    #[test]
    fn assemble_short_reads_truncates_to_requested() {
        let data = assemble_short_reads(&[b"abcdef"], 3).unwrap();
        assert_eq!(&data, b"abc");
    }
}
