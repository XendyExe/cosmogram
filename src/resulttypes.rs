use std::hash::{Hash, Hasher};
#[cfg(feature = "python")]
use pyo3::pyclass;
#[cfg(feature = "python")]
use pyo3_stub_gen::derive::gen_stub_pyclass;

#[cfg(not(feature = "python"))]
pub struct TransferOverviewResult {
    pub src: bool,
    pub table: String,
    pub top: Vec<(String, f64)>,
    pub search_time: f64,
    pub table_time: f64,
    pub top_time: f64,
    pub result_logs: u64,
    pub total_logs: u64,
}

#[cfg(feature = "python")]
#[gen_stub_pyclass]
#[pyclass]
pub struct TransferOverviewResult {
    #[pyo3(get)]
    pub src: bool,
    #[pyo3(get)]
    pub table: String,
    #[pyo3(get)]
    pub top: Vec<(String, f64)>,
    #[pyo3(get)]
    pub search_time: f64,
    #[pyo3(get)]
    pub table_time: f64,
    #[pyo3(get)]
    pub top_time: f64,
    #[pyo3(get)]
    pub result_logs: u64,
    #[pyo3(get)]
    pub total_logs: u64,
}

#[cfg(not(feature = "python"))]
pub struct ShipRecord {
    pub time: u32,
    pub name: String,
    pub color: u32,
}


#[cfg(feature = "python")]
#[gen_stub_pyclass]
#[pyclass]
pub struct ShipRecord {
    #[pyo3(get)]
    pub time: u32,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub color: u32,
}

impl PartialEq for ShipRecord {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.color == other.color
    }
}

impl Eq for ShipRecord {}

impl Hash for ShipRecord {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.color.hash(state);
    }
}