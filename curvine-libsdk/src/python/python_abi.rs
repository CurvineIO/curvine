#![allow(clippy::missing_safety_doc)]

use crate::python::PythonFilesystem;
use crate::{LibFsReader, LibFsWriter};
use curvine_common::error::{ErrorKind, FsError};
use orpc::sys::DataSlice;
use orpc::sys::{FFIUtils, RawVec};
use pyo3::exceptions::*;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

#[pyfunction]
pub fn python_io_curvine_curvine_native_new_filesystem(conf_path: String) -> PyResult<i64> {
    let fs = PythonFilesystem::new(conf_path).map_err(|e| e.into_py_err())?;
    Ok(FFIUtils::into_raw_ptr(fs))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_create(
    ptr: usize,
    path: String,
    overwrite: bool,
) -> PyResult<i64> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let writer = fs.create(path, overwrite).map_err(|e| e.into_py_err())?;
    Ok(FFIUtils::into_raw_ptr(writer))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_append(
    ptr: usize,
    path: String,
    tmp: &Bound<'_, PyList>,
) -> PyResult<i64> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let writer = fs.append(path).map_err(|e| e.into_py_err())?;
    let arr = writer.pos();

    tmp.set_item(0, arr)?;
    Ok(FFIUtils::into_raw_ptr(writer))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_write(ptr: usize, buf: i64, len: i32) -> PyResult<()> {
    let writer_ptr = ptr as *mut LibFsWriter;
    let writer = unsafe { &mut *writer_ptr };
    let raw_vec = RawVec::from_raw(buf as *mut u8, len as usize);
    let buf = DataSlice::MemSlice(raw_vec);
    writer.write(buf).map_err(|e| e.into_py_err())?;

    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_flush(ptr: usize) -> PyResult<()> {
    let writer_ptr = ptr as *mut LibFsWriter;
    let writer = unsafe { &mut *writer_ptr };
    writer.flush().map_err(|e| e.into_py_err())?;
    Ok(())
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_writer(ptr: usize) -> PyResult<()> {
    let writer_ptr = ptr as *mut LibFsWriter;
    let writer = unsafe { &mut *writer_ptr };
    writer.complete().map_err(|e| e.into_py_err())?;
    FFIUtils::free_raw_ptr(writer_ptr);
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_open(
    ptr: usize,
    path: String,
    tmp: &Bound<'_, PyList>,
) -> PyResult<i64> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let reader = fs.open(path).map_err(|e| e.into_py_err())?;
    let arr = reader.len();
    tmp.set_item(0, arr)?;
    Ok(FFIUtils::into_raw_ptr(reader))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_read(ptr: usize, tmp: &Bound<'_, PyList>) -> PyResult<()> {
    let reader_ptr = ptr as *mut LibFsReader;
    let reader = unsafe { &mut *reader_ptr };
    let bytes = reader.read().map_err(|e| e.into_py_err())?;
    let bytes_ptr = bytes.as_ptr() as i64;
    let bytes_len = bytes.len() as i64;
    tmp.set_item(0, bytes_ptr)?;
    tmp.set_item(1, bytes_len)?;

    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_seek(ptr: usize, pos: i64) -> PyResult<()> {
    let reader_ptr = ptr as *mut LibFsReader;
    let reader = unsafe { &mut *reader_ptr };
    reader.seek(pos).map_err(|e| e.into_py_err())?;
    Ok(())
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_reader(ptr: usize) -> PyResult<()> {
    let reader_ptr = ptr as *mut LibFsReader;
    let reader = &mut *reader_ptr;
    reader.complete().map_err(|e| e.into_py_err())?;
    FFIUtils::free_raw_ptr(reader_ptr);
    Ok(())
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_filesystem(ptr: usize) -> PyResult<()> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    FFIUtils::free_raw_ptr(fs_ptr);
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_mkdir(
    ptr: usize,
    path: String,
    create_parent: bool,
) -> PyResult<bool> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let is_success = fs.mkdir(path, create_parent).map_err(|e| e.into_py_err())?;
    Ok(is_success)
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_get_file_status<'py>(
    ptr: usize,
    path: String,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let status = fs.get_file_status(path).map_err(|e| e.into_py_err())?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_list_status<'py>(
    ptr: usize,
    path: String,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let status = fs.list_status(path).map_err(|e| e.into_py_err())?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_rename(
    ptr: usize,
    src: String,
    dst: String,
) -> PyResult<bool> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let is_rename = fs.rename(src, dst).map_err(|e| e.into_py_err())?;
    Ok(is_rename)
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_delete(
    ptr: usize,
    path: String,
    recursive: bool,
) -> PyResult<()> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    fs.delete(path, recursive).map_err(|e| e.into_py_err())?;
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_get_master_info<'py>(
    ptr: usize,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>> {
    let fs_ptr = ptr as *mut PythonFilesystem;
    let fs = unsafe { &*fs_ptr };
    let status = fs.get_master_info().map_err(|e| e.into_py_err())?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

pub trait ToPyErr {
    fn into_py_err(self) -> PyErr;
}

impl ToPyErr for FsError {
    fn into_py_err(self) -> PyErr {
        let err_kind = self.kind();
        match err_kind {
            ErrorKind::IO => PyIOError::new_err(self.to_string()),
            ErrorKind::NotLeaderMaster => PyException::new_err(self.to_string()),
            ErrorKind::Raft => PyException::new_err(self.to_string()),
            ErrorKind::Timeout => PyTimeoutError::new_err(self.to_string()),
            ErrorKind::PBDecode => PyConnectionError::new_err(self.to_string()),
            ErrorKind::PBEncode => PyConnectionError::new_err(self.to_string()),
            ErrorKind::FileAlreadyExists => PyFileExistsError::new_err(self.to_string()),
            ErrorKind::FileNotFound => PyFileNotFoundError::new_err(self.to_string()),
            ErrorKind::InvalidFileSize => PyValueError::new_err(self.to_string()),
            ErrorKind::ParentNotDir => PyNotADirectoryError::new_err(self.to_string()),
            ErrorKind::DirNotEmpty => PyIOError::new_err(self.to_string()),
            ErrorKind::AbnormalData => PyValueError::new_err(self.to_string()),
            ErrorKind::BlockIsWriting => PyBlockingIOError::new_err(self.to_string()),
            ErrorKind::BlockInfo => PyBlockingIOError::new_err(self.to_string()),
            ErrorKind::Lease => PyException::new_err(self.to_string()),
            ErrorKind::InvalidPath => PyValueError::new_err(self.to_string()),
            ErrorKind::DiskOutOfSpace => PyException::new_err(self.to_string()),
            ErrorKind::InProgress => PyException::new_err(self.to_string()),
            ErrorKind::Unsupported => PyException::new_err(self.to_string()),
            ErrorKind::Ufs => PyException::new_err(self.to_string()),
            ErrorKind::Expired => PyIOError::new_err(self.to_string()),
            ErrorKind::Common => PyException::new_err(self.to_string()),
        }
    }
}

#[pymodule]
fn curvine_libsdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_new_filesystem,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_create,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_append,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_write, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_flush, m)?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_close_writer,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_open, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_read, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_seek, m)?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_close_reader,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_close_filesystem,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_mkdir, m)?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_get_file_status,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_list_status,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_rename,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_delete,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        python_io_curvine_curvine_native_get_master_info,
        m
    )?)?;

    Ok(())
}
