use std::ffi::CString;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{Schema, SchemaRef};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Convert a Rust `RecordBatch` to a `pyarrow.RecordBatch` via Arrow C Data Interface.
pub fn record_batch_to_pyarrow(py: Python<'_>, batch: &RecordBatch) -> PyResult<PyObject> {
    let struct_array = StructArray::from(batch.clone());
    let data = struct_array.to_data();

    let (ffi_array, ffi_schema) = arrow_array::ffi::to_ffi(&data)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let array_name = CString::new("arrow_array").unwrap();
    let array_capsule = PyCapsule::new(py, ffi_array, Some(array_name))?;

    let schema_name = CString::new("arrow_schema").unwrap();
    let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_name))?;

    let pyarrow = py.import("pyarrow")?;
    let pa_batch = pyarrow
        .getattr("RecordBatch")?
        .call_method1("_import_from_c_capsule", (schema_capsule, array_capsule))?;
    Ok(pa_batch.unbind())
}

/// Convert a `pyarrow.RecordBatch` or `pyarrow.Table` to a Rust `RecordBatch`.
pub fn record_batch_from_pyarrow(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let pyarrow = py.import("pyarrow")?;
    let pa_record_batch = pyarrow.getattr("RecordBatch")?;

    let batch_obj = if obj.is_instance(&pa_record_batch)? {
        obj.clone()
    } else {
        let pa_table = pyarrow.getattr("Table")?;
        if obj.is_instance(&pa_table)? {
            obj.call_method0("combine_chunks")?
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected pyarrow.RecordBatch or pyarrow.Table",
            ));
        }
    };

    // Use __arrow_c_array__ (PyCapsule protocol, pyarrow >= 14)
    let capsule_tuple = batch_obj.call_method0("__arrow_c_array__")?;
    let tuple: (Bound<'_, PyCapsule>, Bound<'_, PyCapsule>) = capsule_tuple.extract()?;

    let schema_capsule = &tuple.0;
    let array_capsule = &tuple.1;

    // Transfer ownership from PyCapsule to Rust via from_raw.
    // This replaces the originals with empty structs (release=None) so capsule
    // destructors are no-ops, preventing double-free.
    let ffi_schema_ptr = schema_capsule.pointer() as *mut arrow_schema::ffi::FFI_ArrowSchema;
    let ffi_schema = unsafe { arrow_schema::ffi::FFI_ArrowSchema::from_raw(ffi_schema_ptr) };

    let ffi_array_ptr = array_capsule.pointer() as *mut arrow_array::ffi::FFI_ArrowArray;
    let ffi_array = unsafe { arrow_array::ffi::FFI_ArrowArray::from_raw(ffi_array_ptr) };

    let array_data = unsafe { arrow_array::ffi::from_ffi(ffi_array, &ffi_schema) }
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    // Convert ArrayData to StructArray, then extract schema and columns
    let struct_array = StructArray::from(array_data);
    let fields = struct_array.fields().clone();
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let columns: Vec<_> = struct_array.columns().to_vec();

    RecordBatch::try_new(schema, columns)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Convert multiple Rust `RecordBatch`es to a `pyarrow.Table`.
pub fn batches_to_pyarrow_table(py: Python<'_>, batches: &[RecordBatch]) -> PyResult<PyObject> {
    if batches.is_empty() {
        let pyarrow = py.import("pyarrow")?;
        let empty: Vec<PyObject> = Vec::new();
        return Ok(pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (empty,))?
            .unbind());
    }

    let py_batches: Vec<PyObject> = batches
        .iter()
        .map(|b| record_batch_to_pyarrow(py, b))
        .collect::<PyResult<_>>()?;

    let pyarrow = py.import("pyarrow")?;
    let table = pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_batches,))?;
    Ok(table.unbind())
}
