use lancedb_upstream::Table as UpstreamTable;
use pyo3::prelude::*;

use super::arrow_bridge;
use super::error;
use super::py_query::PyQuery;

#[pyclass(name = "Table")]
pub struct PyTable {
    inner: UpstreamTable,
}

impl PyTable {
    pub fn new(table: UpstreamTable) -> Self {
        Self { inner: table }
    }
}

#[pymethods]
impl PyTable {
    fn name(&self) -> &str {
        self.inner.name()
    }

    #[pyo3(signature = (filter=None))]
    fn count_rows<'py>(
        &self,
        py: Python<'py>,
        filter: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let table = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = table.count_rows(filter).await.map_err(error::to_py_err)?;
            Ok(count)
        })
    }

    fn add<'py>(&self, py: Python<'py>, data: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let table = self.inner.clone();
        let batch = arrow_bridge::record_batch_from_pyarrow(py, data)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            table.add(batch).execute().await.map_err(error::to_py_err)?;
            Ok(())
        })
    }

    fn delete<'py>(&self, py: Python<'py>, predicate: &str) -> PyResult<Bound<'py, PyAny>> {
        let table = self.inner.clone();
        let predicate = predicate.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            table.delete(&predicate).await.map_err(error::to_py_err)?;
            Ok(())
        })
    }

    fn search(&self) -> PyQuery {
        PyQuery::new(self.inner.query())
    }
}
