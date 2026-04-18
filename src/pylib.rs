use std::collections::HashSet;
use std::path::Path;
use pyo3::{pyclass, pymethods};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use crate::Cosmogram;
use crate::resulttypes::{ShipRecord, TransferOverviewResult};
use pyo3::pymodule;
use pyo3::Bound;
use pyo3::prelude::PyModule;
use pyo3::prelude::PyModuleMethods;
use pyo3::PyResult;
use pyo3_stub_gen::define_stub_info_gatherer;
use crate::utils::{pack_ship_hex, ship_hex_to_hash};

define_stub_info_gatherer!(stub_info);
#[pymodule]
fn cosmogram(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCosmogram>()?;
    m.add_class::<TransferOverviewResult>()?;
    Ok(())
}

#[gen_stub_pyclass]
#[pyclass]
#[pyo3(name = "Cosmogram")]
pub struct PyCosmogram {
    inner: Cosmogram
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCosmogram {
    #[new]
    pub fn new(path: String) -> PyCosmogram {
        let inner = Cosmogram::new(Path::new(&path));
        PyCosmogram { inner }
    }

    pub fn reload(&mut self) {
        self.inner.reload();
    }

    pub fn get_ship_data(&self, hex: String, include_rares: bool) -> (HashSet<ShipRecord>, Vec<(u32, f64)>, usize, usize) {
        self.inner.get_ship_data(hex.as_str(), include_rares)
    }

    pub fn get_transfer_overview_by_src_hex(&self, hex: &str, start_time: Option<u32>, end_time: Option<u32>) -> TransferOverviewResult {
        self.inner.get_transfer_overview_by_src_hex(hex, start_time, end_time)
    }

    pub fn get_transfer_overview_by_dst_hex(&self, hex: &str, start_time: Option<u32>, end_time: Option<u32>) -> TransferOverviewResult {
        self.inner.get_transfer_overview_by_dst_hex(hex, start_time, end_time)
    }

    pub fn get_latest_ship_name_from_hash(&self, hash: u64) {
        let hex = (hash & 0xFFFFFFFF) as u32;
        let lz = (hash >> 32) as u8;
        self.inner.get_latest_name(hex, lz);
    }

    pub fn get_latest_ship_name(&self, hex: &str) -> Option<String> {
        let (hex, lz) = pack_ship_hex(hex);
        self.inner.get_latest_name(hex, lz)
    }

    pub fn ship_exists(&self, hex: &str) -> bool {
        let hash = ship_hex_to_hash(hex);
        self.inner.ship_exists(hash)
    }

    pub fn get_networth_leaderboard_no_rares(&self) -> &Vec<(u64, f64)> {
        &self.inner.leaderboard_flux_no_rares
    }
    pub fn get_networth_leaderboard_with_rares(&self) -> &Vec<(u64, f64)> {
        &self.inner.leaderboard_flux_rares
    }
}