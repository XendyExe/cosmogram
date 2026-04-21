use crate::fetcher::{fetch, get_item_data};
use crate::jsontypes::ItemSchema;
use crate::resulttypes::{ShipRecord, TransferCountItemsResult, TransferOverviewResult};
use crate::types::{MemoryShipEntry, PackedShipEntry, PackedTransferLog, ShipNameEntry, TransferLog, TransferSource};
use crate::utils::{is_hash_4_digit, normalize_name, pack_ship_hex, packed_hex_to_string, packed_ship_hex_to_hash};
use chrono::Datelike;
use chrono::{Duration, NaiveDate, Utc};
use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use nohash_hasher::{IntMap, IntSet};
use num_enum::TryFromPrimitive;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::option::Option;
use std::path::{absolute, Path};
use std::sync::Arc;
use std::time::Instant;
use std::{fs, time};

pub mod types;
mod jsontypes;
mod fetcher;
mod jsonstreamer;
pub mod utils;
mod resulttypes;

#[cfg(feature = "python")]
pub mod pylib;

// What items are rares?
pub const RARES: [u16; 5] = [102, 246, 305, 307, 306];

/// This is the definition of "just throw more ram" at it until it works
pub struct Cosmogram {
    // total should use about 3 gb ram
    pub trans: Vec<Vec<PackedTransferLog>>,
    sorted_src_trans: Vec<Vec<u32>>,
    sorted_dst_trans: Vec<Vec<u32>>,

    accumulated_ships: IntMap<u64, u32>,
    pub ships: Vec<IntMap<u64, MemoryShipEntry>>,
    pub ship_names: IntMap<u64, Vec<ShipNameEntry>>,
    items: IntMap<u16, ItemSchema>,
    item_worth: HashMap<u16, f64>,
    last_fetched: (u32, u32, u32),
    all_ships: IntSet<u64>,
    log_count: u64,

    pub leaderboard_flux_rares_rankings: IntMap<u64, usize>,
    pub leaderboard_flux_rares: Vec<(u64, f64)>,
    pub leaderboard_flux_no_rares_rankings: IntMap<u64, usize>,
    pub leaderboard_flux_no_rares: Vec<(u64, f64)>,
}

impl Cosmogram {
    /// Creates a new Cosmogram
    /// * `path` - Path of where to store the cache
    pub fn new(path: &Path) -> Cosmogram {
        if path.is_file() {
            panic!("Path must be to a directory");
        }
        if !path.exists() {
            fs::create_dir(path).unwrap();
        }
        let (items, item_worth) = get_item_data();

        let start = NaiveDate::from_ymd_opt(2022, 11, 23).unwrap();
        let today = Utc::now().date_naive();
        let days = today.signed_duration_since(start).num_days() as usize;

        let dates: Vec<(NaiveDate, i64)> = (0..=days as i64).map(|i| (start + Duration::days(i), i)).collect();

        let progress_container = Arc::new(MultiProgress::new());
        let main_progress_bar = progress_container.add(ProgressBar::new(days as u64));
        let display_path = absolute(path).unwrap().display().to_string();
        main_progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(&format!("[Cosmogram] Loading econ logs to {}\n{}", display_path, "[Cosmogram] [{bar:50}] {pos}/{len} (ETA: {eta})"))
                .unwrap(),
        );
        let worker_bars: Vec<_> = (0..rayon::current_num_threads())
        .map(|i| {
            let pb = progress_container.add(ProgressBar::new_spinner());
            pb.set_message(format!("Worker {} idle", i));
            pb.enable_steady_tick(time::Duration::from_millis(200));
            pb
        })
        .collect();

        let latest_name = Arc::new(DashMap::<u64, (String, u32)>::new());

        let results: Vec<_> = dates.par_iter().map({
            let worker_bars = worker_bars.clone();
            let main_progress_bar = main_progress_bar.clone();
            let latest_name = latest_name.clone();
            move |data| {
                let (date, offset) = data;
                let worker_id = rayon::current_thread_index().unwrap_or(0);
                let status_update = &worker_bars[worker_id];
                let year = date.year() as u32;
                let month = date.month();
                let day = date.day();
                let result = fetch(year, month, day, path, status_update);
                main_progress_bar.inc(1);
                let transfer = result.0;
                let ships = result.1;
                let mut new_names: Vec<(u64, ShipNameEntry)> = Vec::new();
                for ship in &ships {
                    let existing = latest_name.get(ship.0).map(|r| (r.0.clone(), r.1));

                    let changed = match &existing {
                        Some(current) => current.0 != ship.1.name || current.1 != ship.1.color,
                        None => true,
                    };

                    if changed {
                        latest_name.insert(*ship.0, (ship.1.name.clone(), ship.1.color));
                        new_names.push((*ship.0, ShipNameEntry {
                            index: *offset as usize,
                            name: ship.1.name.clone(),
                            normalized_name: normalize_name(&ship.1.name),
                            color: ship.1.color,
                        }));
                    }
                }
                let mem_ships: IntMap<u64, MemoryShipEntry> = ships.iter().map(|value| (*value.0, MemoryShipEntry {
                    items: value.1.items.clone(),
                    time: *offset as u32,
                    hex: value.1.hex,
                    hex_lz: value.1.hex_lz,
                })).collect();


                (offset, transfer, mem_ships, new_names)
            }
        }).collect();
        main_progress_bar.finish_and_clear();
        for worker_bar in worker_bars {
            worker_bar.finish_and_clear();
        }
        println!("Collecting data...");
        let mut trans: Vec<Vec<PackedTransferLog>> = Vec::with_capacity(days);
        let mut sorted_src_trans: Vec<Vec<u32>> = Vec::with_capacity(days);
        let mut sorted_dst_trans: Vec<Vec<u32>> = Vec::with_capacity(days);
        let mut ships: Vec<IntMap<u64, MemoryShipEntry>> = Vec::with_capacity(days);
        let mut accumulated_ships = IntMap::with_capacity_and_hasher(100000, Default::default());
        let mut all_ships = IntSet::with_capacity_and_hasher(100000, Default::default());
        let mut ship_names: IntMap<u64, Vec<ShipNameEntry>> = IntMap::with_capacity_and_hasher(latest_name.len(), Default::default());
        let mut total_log_count: u64 = 0;
        for (day, transfer_data, ships_data, new_names) in results {
            for (ship_key, _) in &ships_data {
                all_ships.insert(*ship_key);
            }

            for (ship_key, _) in &ships_data {
                accumulated_ships.insert(*ship_key, *day as u32);
            }

            for (hash, name) in new_names {
                if ship_names.contains_key(&hash) {
                    ship_names.get_mut(&hash).unwrap().push(name);
                } else {
                    ship_names.insert(hash, vec![name]);
                }
            }

            ships.push(ships_data);
            total_log_count += transfer_data.logs.len() as u64;
            trans.push(transfer_data.logs);
            sorted_src_trans.push(transfer_data.src_indexes);
            sorted_dst_trans.push(transfer_data.dst_indexes);
        }
        all_ships.shrink_to_fit();
        accumulated_ships.shrink_to_fit();
        let last_date = dates[dates.len() - 1];
        println!("Done!");
        let mut cosmos = Cosmogram {
            trans, sorted_dst_trans, sorted_src_trans, ships, ship_names, items,
            accumulated_ships,
            item_worth,
            last_fetched: (last_date.0.year() as u32, last_date.0.month(), last_date.0.day()),
            log_count: total_log_count,
            all_ships,
            leaderboard_flux_rares: Vec::new(),
            leaderboard_flux_no_rares: Vec::new(),
            leaderboard_flux_rares_rankings: IntMap::with_capacity_and_hasher(0, Default::default()),
            leaderboard_flux_no_rares_rankings: IntMap::with_capacity_and_hasher(0, Default::default())
        };
        cosmos.load_leaderboards(days);
        cosmos
    }

    fn load_leaderboards(&mut self, days: usize) {
        println!("Loading leaderboards...");
        self.leaderboard_flux_rares = self.accumulated_ships.par_iter().map(|(ship_key, last_index)| (*ship_key, self.get_networth(&self.ships[*last_index as usize][ship_key].items, is_hash_4_digit(*ship_key), true))).collect();
        self.leaderboard_flux_no_rares = self.accumulated_ships.par_iter().map(|(ship_key, last_index)| (*ship_key, self.get_networth(&self.ships[*last_index as usize][ship_key].items, false, false))).collect();
        self.leaderboard_flux_rares.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
        self.leaderboard_flux_no_rares.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));

        self.leaderboard_flux_rares_rankings = self
            .leaderboard_flux_rares
            .iter()
            .enumerate()
            .map(|(idx, (ship_key, _))| (*ship_key, idx))
            .collect();

        self.leaderboard_flux_no_rares_rankings = self
            .leaderboard_flux_no_rares
            .iter()
            .enumerate()
            .map(|(idx, (ship_key, _))| (*ship_key, idx))
            .collect();

        println!("Done!")
    }

    /// Attempts to load more, updated data
    pub fn reload(&mut self) {

    }

    pub fn get_ship_worth(&self, hex: &str) -> (f64, f64) {
        let (hex_u32, lz) = pack_ship_hex(hex);
        let is_four_digit = is_hash_4_digit(packed_ship_hex_to_hash(hex_u32, lz));
        let Some(latest) = self.find_latest_ship(hex_u32, lz) else { return (0.0, 0.0) };
        self.get_both_networth(&latest.items, is_four_digit)
    }

    pub fn get_rares_leaderboard(&self, search: Option<&str>, strict_search: bool) -> Vec<((u64, f64), u32)> {
        if let Some(mut search) = search {
            search = search.trim();
            let normalized_search = normalize_name(search);
            let mut indexes = HashSet::new();
            for i in 0..self.leaderboard_flux_rares.len() {
                let entry = &self.leaderboard_flux_rares[i];
                if let Some(ship_name) = self.get_latest_ship_name_entry_from_hash(entry.0) {
                    if ship_name.name.contains(search) { indexes.insert(i); }
                    if strict_search { continue; }
                    if ship_name.normalized_name.contains(&normalized_search) { indexes.insert(i); }
                }
            }
            let mut result = indexes.iter().collect::<Vec<&usize>>();
            result.sort_unstable_by_key(|i| **i);
            result.iter().map(|i| (self.leaderboard_flux_rares[**i], **i as u32)).collect()
        } else { self.leaderboard_flux_rares.clone().into_iter().zip(0..self.leaderboard_flux_rares.len() as u32).collect() }
    }

    pub fn get_no_rares_leaderboard(&self, search: Option<&str>, strict_search: bool) -> Vec<((u64, f64), u32)> {
        if let Some(mut search) = search {
            search = search.trim();
            let normalized_search = normalize_name(search);
            let mut indexes = HashSet::new();
            for i in 0..self.leaderboard_flux_no_rares.len() {
                let entry = &self.leaderboard_flux_no_rares[i];
                if let Some(ship_name) = self.get_latest_ship_name_entry_from_hash(entry.0) {
                    if ship_name.name.contains(search) { indexes.insert(i); }
                    if strict_search { continue; }
                    if ship_name.normalized_name.contains(&normalized_search) { indexes.insert(i); }
                }
            }
            let mut result = indexes.iter().collect::<Vec<&usize>>();
            result.sort_unstable_by_key(|i| **i);
            result.iter().map(|i| (self.leaderboard_flux_no_rares[**i], **i as u32)).collect()
        } else { self.leaderboard_flux_no_rares.clone().into_iter().zip(0..self.leaderboard_flux_no_rares.len() as u32).collect() }
    }

    pub fn get_ship_data(&self, hex: &str, include_rares: bool) -> (HashSet<ShipRecord>, Vec<(u32, f64)>, usize, usize) {
        let (hex_u32, lz) = pack_ship_hex(hex);
        let hash = packed_ship_hex_to_hash(hex_u32, lz);
        let results = self.search_ships(hex_u32, lz);
        let mut past_names: HashSet<ShipRecord> = HashSet::new();
        let mut networth: Vec<(u32, f64)> = Vec::with_capacity(self.ships.len());
        let is_four_digit = is_hash_4_digit(packed_ship_hex_to_hash(hex_u32, lz));

        for (time, ship) in results {
            past_names.insert(ShipRecord {time, name: ship.name, color: ship.color});
            networth.push((time, self.get_networth(&ship.items, is_four_digit, include_rares)));
        }

        (past_names, networth, self.leaderboard_flux_no_rares_rankings[&hash], self.leaderboard_flux_rares_rankings[&hash])
    }

    pub fn get_networth(&self, items: &Vec<(u16, u32)>, four_digit: bool, include_rares: bool) -> f64 {
        let mut worth = if four_digit && include_rares { self.item_worth[&65535] } else { 0.0 };
        for (item_id, count) in items {
            if !include_rares && RARES.contains(&(*item_id)) { continue }
            let Some(item_worth) = self.item_worth.get(&(*item_id)) else { continue };
            worth += item_worth * *count as f64;
        }
        worth
    }

    pub fn get_both_networth(&self, items: &Vec<(u16, u32)>, four_digit: bool) -> (f64, f64) {
        let mut worth = 0.0;
        let mut worth_rares = if four_digit {self.item_worth[&65535]} else {0.0};
        for (item_id, count) in items {
            if RARES.contains(&(*item_id)) {
                worth_rares += *count as f64;
                continue;
            }
            worth += *count as f64;
            worth_rares += *count as f64;
        }
        (worth, worth_rares)
    }

    pub fn get_transfer_item_count_by_src_hex(&self, hex: &str, item: u16, start_time: Option<u32>, end_time: Option<u32>) -> TransferCountItemsResult {
        let (src, lz) = pack_ship_hex(hex);
        let start_time = if start_time.is_some() { start_time.unwrap() } else { 0 };
        let end_time = if end_time.is_some() { end_time.unwrap() } else { u32::MAX };
        let timer = Instant::now();
        let result: Vec<TransferLog> = self.get_transfers_by_src(src, lz, start_time, end_time).iter().filter(|log| log.item == item).cloned().collect();
        let search_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let save_file = self.create_table(&result);
        let table_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let mut counts: IntMap<u64, u32> = IntMap::with_capacity_and_hasher(0, Default::default());
        let mut total_count: u32 = 0;
        for log in &result {
            let count = log.count as u32;
            total_count += count;
            if counts.contains_key(&log.dst_hash) {
                counts.insert(log.dst_hash, counts[&log.dst_hash] + count);
            } else {
                counts.insert(log.dst_hash, count);
            }
        }
        let mut entries: Vec<(u64, u32)> = counts.iter().map(|(&k, &v)| (k, v)).collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top: Vec<(String, u32)> = entries.into_iter().take(10).map(|a| (self.get_latest_name((a.0 & 0xFFFFFFFF) as u32, (a.0 >> 32) as u8).unwrap(), a.1)).collect();
        let top_time = timer.elapsed().as_secs_f64();
        TransferCountItemsResult {
            src: true,
            count: total_count,
            table: save_file,
            top,
            search_time,
            table_time,
            top_time,
            result_logs: result.len() as u64,
            total_logs: self.total_log_count(),
        }
    }

    pub fn get_transfer_item_count_by_dst_hex(&self, hex: &str, item: u16, start_time: Option<u32>, end_time: Option<u32>) -> TransferCountItemsResult {
        let (src, lz) = pack_ship_hex(hex);
        let start_time = if start_time.is_some() { start_time.unwrap() } else { 0 };
        let end_time = if end_time.is_some() { end_time.unwrap() } else { u32::MAX };
        let timer = Instant::now();
        let result: Vec<TransferLog> = self.get_transfers_by_dst(src, lz, start_time, end_time).iter().filter(|log| log.item == item).cloned().collect();
        let search_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let save_file = self.create_table(&result);
        let table_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let mut counts: IntMap<u64, u32> = IntMap::with_capacity_and_hasher(0, Default::default());
        let mut total_count: u32 = 0;
        for log in &result {
            let count = log.count as u32;
            total_count += count;
            if counts.contains_key(&log.src_hash) {
                counts.insert(log.src_hash, counts[&log.src_hash] + count);
            } else {
                counts.insert(log.src_hash, count);
            }
        }
        let mut entries: Vec<(u64, u32)> = counts.iter().map(|(&k, &v)| (k, v)).collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top: Vec<(String, u32)> = entries.into_iter().take(10).map(|a| (self.get_latest_name((a.0 & 0xFFFFFFFF) as u32, (a.0 >> 32) as u8).unwrap(), a.1)).collect();
        let top_time = timer.elapsed().as_secs_f64();
        TransferCountItemsResult {
            src: false,
            count: total_count,
            table: save_file,
            top,
            search_time,
            table_time,
            top_time,
            result_logs: result.len() as u64,
            total_logs: self.total_log_count(),
        }
    }

    pub fn get_transfer_overview_by_src_hex(&self, hex: &str, start_time: Option<u32>, end_time: Option<u32>) -> TransferOverviewResult {
        let (src, lz) = pack_ship_hex(hex);
        let start_time = if start_time.is_some() { start_time.unwrap() } else { 0 };
        let end_time = if end_time.is_some() { end_time.unwrap() } else { u32::MAX };
        let timer = Instant::now();
        let result = self.get_transfers_by_src(src, lz, start_time, end_time);
        let search_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let save_file = self.create_table(&result);
        let table_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let mut worths: IntMap<u64, f64> = IntMap::with_capacity_and_hasher(0, Default::default());
        for log in &result {
            if RARES.contains(&log.item) { continue }
            if worths.contains_key(&log.dst_hash) {
                worths.insert(log.dst_hash, worths[&log.dst_hash] + (self.item_worth[&log.item] * log.count as f64));
            } else {
                worths.insert(log.dst_hash, self.item_worth[&log.item] * log.count as f64);
            }
        }
        let mut entries: Vec<(u64, f64)> = worths.iter().map(|(&k, &v)| (k, v)).collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top: Vec<(String, f64)> = entries.into_iter().take(10).map(|a| (self.get_latest_name((a.0 & 0xFFFFFFFF) as u32, (a.0 >> 32) as u8).unwrap(), a.1)).collect();
        let top_time = timer.elapsed().as_secs_f64();
        TransferOverviewResult {
            src: true,
            table: save_file,
            top,
            search_time,
            table_time,
            top_time,
            result_logs: result.len() as u64,
            total_logs: self.total_log_count(),
        }
    }

    pub fn get_transfer_overview_by_dst_hex(&self, hex: &str, start_time: Option<u32>, end_time: Option<u32>) -> TransferOverviewResult {
        let (src, lz) = pack_ship_hex(hex);
        let start_time = if start_time.is_some() { start_time.unwrap() } else { 0 };
        let end_time = if end_time.is_some() { end_time.unwrap() } else { u32::MAX };
        let timer = Instant::now();
        let result = self.get_transfers_by_dst(src, lz, start_time, end_time);
        let search_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let save_file = self.create_table(&result);
        let table_time = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        let mut worths: IntMap<u64, f64> = IntMap::with_capacity_and_hasher(0, Default::default());
        for log in &result {
            if RARES.contains(&log.item) { continue }
            if worths.contains_key(&log.src_hash) {
                worths.insert(log.src_hash, worths[&log.src_hash] + (self.item_worth[&log.item] * log.count as f64));
            } else {
                worths.insert(log.src_hash, self.item_worth[&log.item] * log.count as f64);
            }
        }
        let mut entries: Vec<(u64, f64)> = worths.iter().map(|(&k, &v)| (k, v)).collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top: Vec<(String, f64)> = entries.into_iter().take(10).map(|a| (self.get_latest_name((a.0 & 0xFFFFFFFF) as u32, (a.0 >> 32) as u8).unwrap(), a.1)).collect();
        let top_time = timer.elapsed().as_secs_f64();
        TransferOverviewResult {
            src: false,
            table: save_file,
            top,
            search_time,
            table_time,
            top_time,
            result_logs: result.len() as u64,
            total_logs: self.total_log_count(),
        }
    }

    pub fn get_transfers_by_src(&self, src: u32, lz: u8, start_time: u32, end_time: u32) -> Vec<TransferLog> {
        let hash = packed_ship_hex_to_hash(src, lz);
        let src_name = self.get_latest_name(src, lz).unwrap();

        // multithreading my beloved <3
        let mut packed_logs: Vec<_> = (0..self.sorted_src_trans.len())
            .into_par_iter()
            .flat_map(|i| {
                let sorted = &self.sorted_src_trans[i];
                let logs = &self.trans[i];
                let length = logs.len();
                let mut transfer_logs = Vec::new();
                // binary search to find the instance of the ship we are looking for
                let mut search_start_bound = 0;
                let mut search_end_bound = (length - 1) as i32;
                let mut result = None;
                while search_start_bound <= search_end_bound {
                    let search = search_start_bound + ((search_end_bound - search_start_bound) >> 1);
                    let search_log = logs[sorted[search as usize] as usize];
                    let search_hash = packed_ship_hex_to_hash(search_log.src, search_log.src_lz());

                    if search_hash == hash { result = Some(search); break }
                    else if search_hash > hash {
                        search_end_bound = search - 1
                    }
                    else { search_start_bound = search + 1 }
                }
                if let Some(search) = result {
                    // scrub backwards and forwards from this point to gather all instances of this ship's logs
                    // logs are still sorted by time as a secondary, so this should be safe
                    let mut back_search = search as usize;
                    loop {
                        let log = logs[sorted[back_search] as usize];
                        if log.start_time < start_time { break }
                        let search_hash = packed_ship_hex_to_hash(log.src, log.src_lz());
                        if search_hash != hash { break }
                        let dst_name = self.get_latest_name(log.dst, log.dst_lz());
                        let item_name = self.get_item_name(log.item);
                        transfer_logs.push(TransferLog::create(&log, src_name.clone(), dst_name.unwrap(), item_name));
                        if back_search == 0 { break }
                        back_search -= 1;
                    }
                    let mut forward_search = search as usize + 1;
                    while forward_search < length {
                        let log = logs[sorted[forward_search] as usize];
                        if log.start_time > end_time { break }
                        let search_hash = packed_ship_hex_to_hash(log.src, log.src_lz());
                        if search_hash != hash { break }
                        let dst_name = self.get_latest_name(log.dst, log.dst_lz());
                        let item_name = self.get_item_name(log.item);
                        transfer_logs.push(TransferLog::create(&log, src_name.clone(), dst_name.unwrap(), item_name));
                        forward_search += 1;
                    }
                }
                transfer_logs
            }).collect();
        packed_logs.sort_unstable_by_key(|log| log.start_time);
        packed_logs
    }

    pub fn get_transfers_by_dst(&self, dst: u32, lz: u8, start_time: u32, end_time: u32) -> Vec<TransferLog> {
        let hash = packed_ship_hex_to_hash(dst, lz);
        let dst_name = self.get_latest_name(dst, lz).unwrap();

        // multithreading my beloved <3
        let mut packed_logs: Vec<_> = (0..self.sorted_src_trans.len())
            .into_par_iter()
            .flat_map(|i| {
                let sorted = &self.sorted_dst_trans[i];
                let logs = &self.trans[i];
                let length = logs.len();
                let mut transfer_logs = Vec::new();
                // binary search to find the instance of the ship we are looking for
                let mut search_start_bound = 0;
                let mut search_end_bound = (length - 1) as i32;
                let mut result = None;
                while search_start_bound <= search_end_bound {
                    let search = search_start_bound + ((search_end_bound - search_start_bound) >> 1);
                    let search_log = logs[sorted[search as usize] as usize];
                    let search_hash = packed_ship_hex_to_hash(search_log.dst, search_log.dst_lz());

                    if search_hash == hash { result = Some(search); break }
                    else if search_hash > hash {
                        search_end_bound = search - 1
                    }
                    else { search_start_bound = search + 1 }
                }
                if let Some(search) = result {
                    // scrub backwards and forwards from this point to gather all instances of this ship's logs
                    // logs are still sorted by time as a secondary, so this should be safe
                    let mut back_search = search as usize;
                    loop {
                        let log = logs[sorted[back_search] as usize];
                        if log.start_time < start_time { break }
                        let search_hash = packed_ship_hex_to_hash(log.dst, log.dst_lz());
                        if search_hash != hash { break }
                        let src_name = self.get_latest_name(log.src, log.src_lz());
                        let item_name = self.get_item_name(log.item);
                        transfer_logs.push(TransferLog::create(&log, src_name.unwrap(), dst_name.clone(), item_name));
                        if back_search == 0 { break }
                        back_search -= 1;
                    }
                    let mut forward_search = search as usize + 1;
                    while forward_search < length {
                        let log = logs[sorted[forward_search] as usize];
                        if log.start_time > end_time { break }
                        let search_hash = packed_ship_hex_to_hash(log.dst, log.dst_lz());
                        if search_hash != hash { break }
                        let src_name = self.get_latest_name(log.src, log.src_lz());
                        let item_name = self.get_item_name(log.item);
                        transfer_logs.push(TransferLog::create(&log, src_name.unwrap(), dst_name.clone(), item_name));
                        forward_search += 1;
                    }
                }
                transfer_logs
            }).collect();
        packed_logs.sort_unstable_by_key(|log| log.start_time);
        packed_logs
    }

    pub fn search_ships(&self, hex: u32, lz: u8) -> Vec<(u32, PackedShipEntry)> {
        let mut result: Vec<(u32, PackedShipEntry)> = Vec::new();
        let hash = packed_ship_hex_to_hash(hex, lz);
        let ship_names = &self.ship_names[&hash];
        for log_index in 0..self.ships.len() {
            let day = &self.ships[log_index];
            if let Some(ship) = day.get(&hash) {
                let i = ship_names.partition_point(|entry| entry.index <= log_index);
                let name = &ship_names[i.saturating_sub(1)];

                result.push((log_index as u32, PackedShipEntry {
                    items: ship.items.clone(),
                    name: name.name.clone(),
                    color: name.color.clone(),
                    hex,
                    hex_lz: lz,
                }));
            }
        }
        result
    }

    pub fn find_latest_ship(&self, hex: u32, lz: u8) -> Option<PackedShipEntry> {
        let hash = packed_ship_hex_to_hash(hex, lz);
        let ship_names = &self.ship_names[&hash];
        for log_index in (0..self.ships.len()).rev() {
            let day = &self.ships[log_index];
            if let Some(ship) = day.get(&hash) {
                let i = ship_names.partition_point(|entry| entry.index <= log_index);
                let name = &ship_names[i.saturating_sub(1)];
                return Some(PackedShipEntry {
                    items: ship.items.clone(),
                    name: name.name.clone(),
                    color: name.color.clone(),
                    hex,
                    hex_lz: lz,
                });
            }
        }
        None
    }

    pub fn get_latest_name(&self, hex: u32, lz: u8) -> Option<String> {
        if hex == 0 && lz == 0{
            return Some("killed".to_string());
        }
        if lz == 0b111 {
            return Some(TransferSource::try_from_primitive(hex).unwrap().to_string());
        }
        let hex_hash = packed_ship_hex_to_hash(hex, lz);
        let Some(names) = self.ship_names.get(&hex_hash) else { return None; };
        let latest = names.last().unwrap();
        Some(format!("{} [{}]", latest.name, packed_hex_to_string(hex, lz)))
    }

    pub fn get_latest_name_from_hash(&self, hex_hash: u64) -> Option<String> {
        self.get_latest_name((hex_hash & 0xFFFFFFFF) as u32, (hex_hash >> 32) as u8)
    }

    pub fn get_latest_ship_name_entry_from_hash(&self, hex_hash: u64) -> Option<&ShipNameEntry> {
        let Some(names) = self.ship_names.get(&hex_hash) else { return None; };
        let latest = names.last().unwrap();
        Some(latest)
    }

    pub fn get_item_name(&self, item_id: u16) -> String {
        self.items[&item_id].name.clone()
    }

    pub fn total_log_count(&self) -> u64 {
        self.log_count
    }

    pub fn ship_exists(&self, ship: u64) -> bool {
        self.all_ships.contains(&ship)
    }

    fn create_table(&self, logs: &Vec<TransferLog>) -> String {
        let mut rows: Vec<(String, String, String, String, String)> = Vec::with_capacity(logs.len());
        let mut longest_time = 4;
        let mut longest_zone = 4;
        let mut longest_item = 4;
        for log in logs {
            let server_text = format!("S{}", log.server);
            let time_string = if log.eject_length != 0 { format!("{} (over {} sec, {} logs)", log.start_time_string, log.eject_length, log.consolidated) }
                                  else { log.start_time_string.clone() };
            let zone_string = log.zone.to_string();
            let item = format!("{}x {}", log.count, log.item_name);
            let transfer = if log.partial_hurt {
                format!("{} -> {} (partially hurt)", log.src_name, log.dst_name)
            }
            else if log.hurt {
                format!("{} -> {} (hurt)", log.src_name, log.dst_name)
            }
            else {
                format!("{} -> {}", log.src_name, log.dst_name)
            };

            longest_time = longest_time.max(time_string.len());
            longest_zone = longest_zone.max(zone_string.len());
            longest_item = longest_item.max(item.len());
            rows.push((server_text, time_string, zone_string, item, transfer));
        }
        let guess_capacity = (17 + longest_time + longest_item + longest_zone + 32) * rows.len() + 500;
        let mut result = String::with_capacity(guess_capacity);
        result.push_str(&*format!("╭─S#─┬─{}─┬─{}─┬─{}─┬─{}\n", "Time".to_owned() + &*"─".repeat(longest_time - 4), "Zone".to_owned() + &*"─".repeat(longest_zone - 4), "Item".to_owned() + &*"─".repeat(longest_item - 4), "Transfer".to_owned() + TRANSFER_END_STUFF));
        for row in rows {
            let time_length = row.1.len();
            let time = row.1 + &*" ".repeat(longest_time - time_length);
            let zone_length = row.2.len();
            let zone = row.2 + &*" ".repeat(longest_zone - zone_length);
            let item_length = row.3.len();
            let item = row.3 + &*" ".repeat(longest_item - item_length);
            result.push_str(&*format!("│ {} ┆ {} ┆ {} ┆ {} ┆ {}\n", row.0, time, zone, item, row.4));
        }
        result.push_str(&*format!("╰────┴─{}─┴─{}─┴─{}─┴─────────{}", "─".repeat(longest_time), "─".repeat(longest_zone), "─".repeat(longest_item), TRANSFER_END_STUFF));
        result
    }
}

const TRANSFER_END_STUFF: &str = "────────── ──── ──── ─── ─── ─── ── ── ── ── ─ ─ ─ ─ ─";

use deepsize::DeepSizeOf;
impl Cosmogram {
    pub fn print_memory_usage(&self) {
        let mb = |bytes: usize| bytes as f64 / 1024.0 / 1024.0;

        println!("=== Cosmogram Memory Usage ===");
        println!("trans:                      {:.2} MB", mb(self.trans.deep_size_of()));
        println!("sorted_src_trans:           {:.2} MB", mb(self.sorted_src_trans.deep_size_of()));
        println!("sorted_dst_trans:           {:.2} MB", mb(self.sorted_dst_trans.deep_size_of()));
        println!("accumulated_ships:          {:.2} MB", mb(self.accumulated_ships.deep_size_of()));
        println!("ships:                      {:.2} MB", mb(self.ships.deep_size_of()));
        println!("ship_names:                 {:.2} MB", mb(self.ship_names.deep_size_of()));
        println!("items:                      {:.2} MB", mb(self.items.deep_size_of()));
        println!("item_worth:                 {:.2} MB", mb(self.item_worth.deep_size_of()));
        println!("all_ships:                  {:.2} MB", mb(self.all_ships.deep_size_of()));
        println!("leaderboard_flux_rares:     {:.2} MB", mb(self.leaderboard_flux_rares.deep_size_of()));
        println!("leaderboard_flux_no_rares:  {:.2} MB", mb(self.leaderboard_flux_no_rares.deep_size_of()));
        println!("lb_rares_rankings:          {:.2} MB", mb(self.leaderboard_flux_rares_rankings.deep_size_of()));
        println!("lb_no_rares_rankings:       {:.2} MB", mb(self.leaderboard_flux_no_rares_rankings.deep_size_of()));

        let total = self.trans.deep_size_of()
            + self.sorted_src_trans.deep_size_of()
            + self.sorted_dst_trans.deep_size_of()
            + self.accumulated_ships.deep_size_of()
            + self.ships.deep_size_of()
            + self.ship_names.deep_size_of()
            + self.items.deep_size_of()
            + self.item_worth.deep_size_of()
            + self.all_ships.deep_size_of()
            + self.leaderboard_flux_rares.deep_size_of()
            + self.leaderboard_flux_no_rares.deep_size_of()
            + self.leaderboard_flux_rares_rankings.deep_size_of()
            + self.leaderboard_flux_no_rares_rankings.deep_size_of();
        println!("------------------------------");
        println!("TOTAL (struct fields):      {:.2} MB", mb(total));
    }
}