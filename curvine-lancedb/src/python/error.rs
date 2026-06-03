use lancedb_upstream::error::Error;
use pyo3::exceptions::*;
use pyo3::PyErr;

pub fn to_py_err(err: Error) -> PyErr {
    let msg = err.to_string();
    match &err {
        Error::InvalidTableName { .. } | Error::InvalidInput { .. } | Error::Schema { .. } => {
            PyValueError::new_err(msg)
        }
        Error::TableNotFound { .. }
        | Error::DatabaseNotFound { .. }
        | Error::IndexNotFound { .. } => PyFileNotFoundError::new_err(msg),
        Error::TableAlreadyExists { .. } | Error::DatabaseAlreadyExists { .. } => {
            PyFileExistsError::new_err(msg)
        }
        Error::NotSupported { .. } => PyNotImplementedError::new_err(msg),
        Error::Timeout { .. } => PyTimeoutError::new_err(msg),
        _ => PyRuntimeError::new_err(msg),
    }
}
