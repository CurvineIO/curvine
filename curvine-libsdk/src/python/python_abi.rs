#![allow(clippy::missing_safety_doc)]

use crate::{LibFsReader, LibFsWriter};
use crate::python::PythonFilesystem;
use orpc::sys::DataSlice;
use orpc::sys::{FFIUtils, RawVec};
use pyo3::prelude::*; 
use pyo3::types::{PyList, PyBytes};


#[pyfunction] 
pub fn python_io_curvine_curvine_native_new_filesystem(conf: String) -> PyResult<i64>{
    let fs = PythonFilesystem::new(conf)?;
    Ok(FFIUtils::into_raw_ptr(fs))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_create(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    overwrite: bool
) -> PyResult<i64>{

    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let writer = fs.create(path,overwrite)?;
    Ok(FFIUtils::into_raw_ptr(writer))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_append(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    tmp: &Bound<'_, PyList>,
) -> PyResult<i64>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let writer = fs.append(path)?; 
    let arr = writer.pos();
    
    tmp.set_item(0, arr)?;  
    Ok(FFIUtils::into_raw_ptr(writer))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_write(
    bound_writer: &Bound<'_, LibFsWriter>,
    buf: i64,  
    len: i32,
) -> PyResult<()>{
    let raw_ptr = bound_writer.as_ptr();
    let writer_ptr = raw_ptr.cast::<LibFsWriter>();
    let writer = unsafe{ &mut *writer_ptr};
    let raw_vec = RawVec::from_raw(buf as *mut u8, len as usize);
    let buf = DataSlice::MemSlice(raw_vec);
    writer.write(buf)?;

    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_flush(bound_writer: &Bound<'_, LibFsWriter>) -> PyResult<()>{
    let raw_ptr = bound_writer.as_ptr();
    let writer_ptr = raw_ptr.cast::<LibFsWriter>();
    let writer = unsafe{ &mut *writer_ptr};
    writer.flush()?;
    Ok(())
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_writer(bound_writer: &Bound<'_, LibFsWriter>) -> PyResult<()>{
    let raw_ptr = bound_writer.as_ptr();
    let writer_ptr = raw_ptr.cast::<LibFsWriter>();
    let writer = &mut *writer_ptr;
    writer.complete()?;
    FFIUtils::free_raw_ptr(writer_ptr);
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_open(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    tmp: &Bound<'_, PyList>,
) -> PyResult<i64>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let reader = fs.open(path)?;
    let arr = reader.len();
    tmp.set_item(0, arr)?; 
    Ok(FFIUtils::into_raw_ptr(reader))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_read(
    bound_reader: &Bound<'_, LibFsReader>,
    tmp: &Bound<'_, PyList>,
) -> PyResult<()>{
    let raw_ptr = bound_reader.as_ptr();
    let reader_ptr = raw_ptr.cast::<LibFsReader>();
    let reader = unsafe{ &mut *reader_ptr};
    let bytes = reader.read()?; 

    let bytes_ptr = bytes.as_ptr() as i64; 
    let bytes_len = bytes.len() as i64; 
    tmp.set_item(0, bytes_ptr)?; 
    tmp.set_item(0, bytes_len)?; 
    
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_seek(
    bound_reader: &Bound<'_, LibFsReader>,
    pos: i64,
) -> PyResult<()>{
    let raw_ptr = bound_reader.as_ptr();
    let reader_ptr = raw_ptr.cast::<LibFsReader>();
    let reader = unsafe{ &mut *reader_ptr};
    reader.seek(pos)?;
    Ok(())
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_reader(bound_reader: &Bound<'_, LibFsReader>) -> PyResult<()>{
    let raw_ptr = bound_reader.as_ptr();
    let reader_ptr = raw_ptr.cast::<LibFsReader>();
    let reader = &mut *reader_ptr;
    reader.complete()?;
    FFIUtils::free_raw_ptr(reader_ptr);
    Ok(()) 
}

#[pyfunction]
pub unsafe fn python_io_curvine_curvine_native_close_filesystem(bound_fs: &Bound<'_, PythonFilesystem>) -> PyResult<()>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    FFIUtils::free_raw_ptr(fs_ptr);
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_mkdir(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    create_parent: bool,
) -> PyResult<bool>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let is_success = fs.mkdir(path, create_parent)?;
    Ok(is_success)
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_get_file_status<'py>(
    bound_fs: &Bound<'py, PythonFilesystem>,
    path: String,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let status = fs.get_file_status(path)?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_list_status<'py>(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let status = fs.list_status(path)?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_rename(
    bound_fs: &Bound<'_, PythonFilesystem>,
    src: String,
    dst: String,
) -> PyResult<bool>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let is_rename = fs.rename(src, dst)?;
    Ok(is_rename)
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_delete(
    bound_fs: &Bound<'_, PythonFilesystem>,
    path: String,
    recursive: bool,
) -> PyResult<()>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    fs.delete(path, recursive)?;
    Ok(())
}

#[pyfunction]
pub fn python_io_curvine_curvine_native_get_master_info<'py>(
    bound_fs: &Bound<'_, PythonFilesystem>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyBytes>>{
    let raw_ptr = bound_fs.as_ptr();
    let fs_ptr = raw_ptr.cast::<PythonFilesystem>();
    let fs = unsafe{ &*fs_ptr};
    let status = fs.get_master_info()?;
    let bytes_data = status.freeze().to_vec();
    Ok(PyBytes::new(py, &bytes_data))
}

#[pymodule]
fn curvine_libsdk(m: &Bound<'_, PyModule>) -> PyResult<()>{
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_new_filesystem, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_create, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_append, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_write, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_flush, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_close_writer, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_open, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_read, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_seek, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_close_reader, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_close_filesystem, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_mkdir, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_get_file_status, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_list_status, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_rename, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_delete, m)?)?;
    m.add_function(wrap_pyfunction!(python_io_curvine_curvine_native_get_master_info, m)?)?;
    
    Ok(())
}