use futures::TryStreamExt;
use lancedb_upstream::query::{
    ExecutableQuery, Query as UpstreamQuery, QueryBase, Select, VectorQuery as UpstreamVectorQuery,
};
use pyo3::prelude::*;

use super::arrow_bridge;
use super::error;

fn already_consumed_err() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err("query already executed or consumed")
}

#[pyclass(name = "Query")]
pub struct PyQuery {
    inner: Option<UpstreamQuery>,
}

impl PyQuery {
    pub fn new(query: UpstreamQuery) -> Self {
        Self { inner: Some(query) }
    }
}

#[pymethods]
impl PyQuery {
    fn limit(mut slf: PyRefMut<Self>, limit: usize) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.limit(limit));
        Ok(slf)
    }

    fn offset(mut slf: PyRefMut<Self>, offset: usize) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.offset(offset));
        Ok(slf)
    }

    #[pyo3(name = "where")]
    fn only_if(mut slf: PyRefMut<Self>, filter: String) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.only_if(filter));
        Ok(slf)
    }

    fn select(mut slf: PyRefMut<Self>, columns: Vec<String>) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        slf.inner = Some(q.select(Select::columns(&refs)));
        Ok(slf)
    }

    fn vector(mut slf: PyRefMut<Self>, vec: Vec<f32>) -> PyResult<PyVectorQuery> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        let vq = q.nearest_to(vec).map_err(error::to_py_err)?;
        Ok(PyVectorQuery::new(vq))
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_arrow<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let q = self.inner.take().ok_or_else(already_consumed_err)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = q.execute().await.map_err(error::to_py_err)?;
            let batches: Vec<_> = stream.try_collect().await.map_err(error::to_py_err)?;
            Python::with_gil(|py| arrow_bridge::batches_to_pyarrow_table(py, &batches))
        })
    }
}

#[pyclass(name = "VectorQuery")]
pub struct PyVectorQuery {
    inner: Option<UpstreamVectorQuery>,
}

impl PyVectorQuery {
    pub fn new(query: UpstreamVectorQuery) -> Self {
        Self { inner: Some(query) }
    }
}

#[pymethods]
impl PyVectorQuery {
    fn limit(mut slf: PyRefMut<Self>, limit: usize) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.limit(limit));
        Ok(slf)
    }

    fn offset(mut slf: PyRefMut<Self>, offset: usize) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.offset(offset));
        Ok(slf)
    }

    #[pyo3(name = "where")]
    fn only_if(mut slf: PyRefMut<Self>, filter: String) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.only_if(filter));
        Ok(slf)
    }

    fn nprobes(mut slf: PyRefMut<Self>, nprobes: usize) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.nprobes(nprobes));
        Ok(slf)
    }

    fn refine_factor(mut slf: PyRefMut<Self>, refine_factor: u32) -> PyResult<PyRefMut<Self>> {
        let q = slf.inner.take().ok_or_else(already_consumed_err)?;
        slf.inner = Some(q.refine_factor(refine_factor));
        Ok(slf)
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_arrow<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let q = self.inner.take().ok_or_else(already_consumed_err)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = q.execute().await.map_err(error::to_py_err)?;
            let batches: Vec<_> = stream.try_collect().await.map_err(error::to_py_err)?;
            Python::with_gil(|py| arrow_bridge::batches_to_pyarrow_table(py, &batches))
        })
    }
}
