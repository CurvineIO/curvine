use pyo3::prelude::*;

use super::error;
use super::py_connection::PyConnection;
use crate::connection::ConnectBuilder as RustConnectBuilder;

fn already_consumed_err() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err("builder already consumed")
}

#[pyclass(name = "ConnectBuilder")]
pub struct PyConnectBuilder {
    inner: Option<RustConnectBuilder>,
}

impl PyConnectBuilder {
    pub fn new(builder: RustConnectBuilder) -> Self {
        Self {
            inner: Some(builder),
        }
    }
}

#[pymethods]
impl PyConnectBuilder {
    fn storage_option(mut slf: PyRefMut<Self>, key: String, value: String) -> PyResult<PyRefMut<Self>> {
        let builder = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(builder.storage_option(key, value));
        Ok(slf)
    }

    fn storage_options(
        mut slf: PyRefMut<Self>,
        pairs: Vec<(String, String)>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(builder.storage_options(pairs));
        Ok(slf)
    }

    fn execute<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let builder = self.inner.take().ok_or_else(already_consumed_err)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let conn = builder.execute().await.map_err(error::to_py_err)?;
            Ok(PyConnection::new(conn))
        })
    }
}

#[pyfunction]
pub fn connect(uri: &str) -> PyConnectBuilder {
    let builder = crate::connection::connect(uri);
    PyConnectBuilder::new(builder)
}
