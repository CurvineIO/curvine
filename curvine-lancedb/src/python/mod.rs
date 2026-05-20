mod arrow_bridge;
mod error;
mod py_connect;
mod py_connection;
mod py_query;
mod py_table;

use pyo3::prelude::*;

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_connect::connect, m)?)?;
    m.add_class::<py_connect::PyConnectBuilder>()?;
    m.add_class::<py_connection::PyConnection>()?;
    m.add_class::<py_table::PyTable>()?;
    m.add_class::<py_query::PyQuery>()?;
    m.add_class::<py_query::PyVectorQuery>()?;
    Ok(())
}
