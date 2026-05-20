use lancedb_upstream::Connection as UpstreamConnection;
use pyo3::prelude::*;

use super::arrow_bridge;
use super::error;
use super::py_table::PyTable;

#[pyclass(name = "Connection")]
pub struct PyConnection {
    inner: UpstreamConnection,
}

impl PyConnection {
    pub fn new(conn: UpstreamConnection) -> Self {
        Self { inner: conn }
    }
}

#[pymethods]
impl PyConnection {
    fn table_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let names = conn.table_names().execute().await.map_err(error::to_py_err)?;
            Ok(names)
        })
    }

    fn open_table<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.inner.clone();
        let name = name.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let table = conn
                .open_table(&name)
                .execute()
                .await
                .map_err(error::to_py_err)?;
            Ok(PyTable::new(table))
        })
    }

    fn create_table<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        data: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.inner.clone();
        let name = name.to_string();
        let batch = arrow_bridge::record_batch_from_pyarrow(py, data)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let table = conn
                .create_table(&name, batch)
                .execute()
                .await
                .map_err(error::to_py_err)?;
            Ok(PyTable::new(table))
        })
    }

    fn drop_table<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.inner.clone();
        let name = name.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            conn.drop_table(&name, &[])
                .await
                .map_err(error::to_py_err)?;
            Ok(())
        })
    }
}
