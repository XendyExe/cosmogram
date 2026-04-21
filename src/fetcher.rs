use crate::jsonstreamer::stream_json_array;
use crate::jsontypes::{ItemSchema, JsonSummaryLog, ShipJsonEntry, TransferJsonEntry};
use crate::types::{PackedShipEntry, PackedTransferLog, ShipDataFile, ShipNameEntry, TransferSource, TransferZone};
use crate::utils::{pack_ship_hex, packed_ship_hex_to_hash};
use bytemuck::cast_slice;
use bytemuck::checked::try_cast_slice;
use flate2::read::GzDecoder;
use indicatif::ProgressBar;
use nohash_hasher::IntMap;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

const SIMPLIFICATION_TIME: u32 = 30;
const SIZE_OF_TRANSFER_ENTRY: usize = size_of::<PackedTransferLog>();
const TRANSFER_FILE_EXTENSION: &str = "trans";
const SHIPS_FILE_EXTENSION: &str = "ships";
const SUMMARY_FILE_EXTENSION: &str = "smmry";

/// Fetches a log of the particular day.
/// Note that logs are usually of the PREVIOUS day, eg the log labeled 11/23/2022 would actually
/// contain the data from 11/22/2022 (I LOVE COGG SO MUCH YES)
///
/// Also contains a few variables to create a pretty terminal interface while loading because this
/// can take a few seconds to load and having a progress bar is nice
pub fn fetch(year: u32, month: u32, day: u32, path: &Path, progress: &ProgressBar) -> (TransferResult, IntMap<u64, PackedShipEntry>) {
    let timer = Instant::now();
    let mut transfer = try_get_transfer(year, month, day, &path);
    let mut ship = try_get_ship(year, month, day, &path);
    if transfer.is_some() && ship.is_some() {
        progress.set_message(format!("Cache loaded {year}-{month}-{day} in {}!", timer.elapsed().as_secs_f64()));
        return (transfer.unwrap(), ship.unwrap());
    }
    let mut summary = try_get_summary(year, month, day, &path);
    if summary.is_none() {
        progress.set_message(format!("Fetching {year}-{month}-{day}'s summary..."));
        let smmry_path = path.join(format!("{year}-{month}-{day}.{SUMMARY_FILE_EXTENSION}"));
        let response = reqwest::blocking::get(format!("https://pub.drednot.io/prod/econ/{}_{}_{}/summary.json", year, month, day).as_str()).expect("Failed to fetch summary");
        let result: JsonSummaryLog = response.json().expect("Failed to parse json");
        write_compressed_file(&smmry_path, &postcard::to_allocvec(&result).unwrap());
        summary = Some(result);
    }
    let summary_ref = summary.as_ref().unwrap();
    if transfer.is_none() {
        progress.set_message(format!("Fetching {year}-{month}-{day}'s transfer logs..."));
        let trans_path = path.join(format!("{year}-{month}-{day}.{TRANSFER_FILE_EXTENSION}"));
        let transfer_data = fetch_transfer(year, month, day, summary_ref.count_ships, SIMPLIFICATION_TIME, progress);
        let entries = transfer_data.logs.len();
        let mut save_file = Vec::with_capacity(4 + (entries * SIZE_OF_TRANSFER_ENTRY) + (entries * 8));
        save_file.extend_from_slice(&(entries as u32).to_le_bytes());
        let transfer_bytes: &[u8] = cast_slice(&*transfer_data.logs);
        save_file.extend_from_slice(transfer_bytes);
        for i in 0..entries {
            let src = (transfer_data.src_indexes[i] as u32).to_le_bytes();
            let dst = (transfer_data.dst_indexes[i] as u32).to_le_bytes();
            save_file.extend_from_slice(&src);
            save_file.extend_from_slice(&dst);
        }
        write_compressed_file(&trans_path, &save_file);
        transfer = Some(transfer_data);
    }
    if ship.is_none() {
        progress.set_message(format!("Fetching {year}-{month}-{day}'s ship logs..."));
        let ship_path = path.join(format!("{year}-{month}-{day}.{SHIPS_FILE_EXTENSION}"));
        let ship_data = fetch_ships(year, month, day, summary_ref.count_ships);
        let ship_bytes = postcard::to_allocvec(&ship_data).expect("Failed to parse ship data");
        write_compressed_file(&ship_path, &ship_bytes);
        ship = Some(ship_data);
    }
    progress.set_message(format!("Loaded {year}-{month}-{day} in {}!", timer.elapsed().as_secs_f64()));
    (
        transfer.unwrap(),
        ship.unwrap()
    )
}

pub fn write_compressed_file(path: &Path, ship_bytes: &[u8]) {
    let output_file = File::create(path).expect("Failed to create output file");
    zstd::stream::copy_encode(ship_bytes, output_file, 15).expect("Failed to write ship bytes");
}

pub struct TransferResult {
    pub logs: Vec<PackedTransferLog>,
    pub src_indexes: Vec<u32>,
    pub dst_indexes: Vec<u32>
}

pub fn try_get_transfer(year: u32, month: u32, day: u32, path: &Path) -> Option<TransferResult> {
    let trans_path = path.join(format!("{year}-{month}-{day}.{TRANSFER_FILE_EXTENSION}"));
    if !trans_path.is_file() { return None; }
    let trans_stream = File::open(&trans_path).expect("Failed to open transfer log file");
    let mut bytes: Vec<u8> = Vec::new();
    let res = zstd::stream::copy_decode(trans_stream, &mut bytes);
    if res.is_err() { return None; }
    let entries = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected_size = 4 + (SIZE_OF_TRANSFER_ENTRY * entries) + (entries * 8);
    if bytes.len() != expected_size {
        let _ = std::fs::remove_file(&trans_path);
        return None;
    }

    let transfer_end_index = SIZE_OF_TRANSFER_ENTRY * entries + 4;
    let transfer_log_bytes = &bytes[4..transfer_end_index];
    let cast = try_cast_slice(transfer_log_bytes);
    if cast.is_err() { return None; }
    let transfer_log_slice: &[PackedTransferLog] = cast.unwrap();
    let logs = transfer_log_slice.to_vec();

    let mut src_indexes = Vec::with_capacity(entries);
    let mut dst_indexes = Vec::with_capacity(entries);
    for i in 0..entries {
        let start = transfer_end_index + (i * 8);
        let b = &bytes[start..(start + 4)];
        src_indexes.push(u32::from_le_bytes(b.try_into().unwrap()));
        let b = &bytes[(start + 4)..(start + 8)];
        dst_indexes.push(u32::from_le_bytes(b.try_into().unwrap()));
    }
    Some(TransferResult {
        logs, src_indexes, dst_indexes
    })
}

pub fn try_get_ship(year: u32, month: u32, day: u32, path: &Path) -> Option<IntMap<u64, PackedShipEntry>> {
    let ship_path = path.join(format!("{year}-{month}-{day}.{SHIPS_FILE_EXTENSION}"));
    if !ship_path.is_file() { return None; }
    let trans_stream = File::open(ship_path).expect("Failed to open ship log file");
    let mut bytes: Vec<u8> = Vec::new();
    let res = zstd::stream::copy_decode(trans_stream, &mut bytes);
    if res.is_err() { return None; }
    let data = postcard::from_bytes::<ShipDataFile>(&bytes);
    if data.is_err() {
        return None;
    }
    let data = data.unwrap();
    Some(data.0)
}

pub fn try_get_summary(year: u32, month: u32, day: u32, path: &Path) -> Option<JsonSummaryLog> {
    let smmry_path = path.join(format!("{year}-{month}-{day}.{SUMMARY_FILE_EXTENSION}"));
    if !smmry_path.is_file() { return None; }
    let trans_stream = File::open(smmry_path).expect("Failed to open summary file");
    let mut bytes: Vec<u8> = Vec::new();
    let res = zstd::stream::copy_decode(trans_stream, &mut bytes);
    if res.is_err() { return None; }
    let summary: JsonSummaryLog = postcard::from_bytes(&bytes).expect("Failed to decode json stream");
    Some(summary)
}



/// Fetches a transfer log of a particular year/month/day.
/// Will merge transfer logs of time less than simplification time
/// Returns the packed transfer logs, source sorted indexes, and destination sorted indexes
pub fn fetch_transfer(year: u32, month: u32, day: u32, count: u32, simplification_time: u32, progress_bar: &ProgressBar) -> TransferResult {
    let response = reqwest::blocking::get(format!("https://pub.drednot.io/prod/econ/{}_{}_{}/log.json.gz", year, month, day));
    let response = response.expect("Failed to fetch transfer log");
    let stream = GzDecoder::new(response);

    // This will overallocate the number of needed packed transfer logs, but it's better to
    // overallocate than underallocate. We allocate half the amount of reported transfer logs
    let mut packed: Vec<PackedTransferLog> = Vec::with_capacity((count >> 1) as usize);

    let mut original_count = 0;
    for result in stream_json_array::<TransferJsonEntry, _>(stream) {
        original_count += 1;
        let entry = result.expect("Failed to parse a transfer log");
        let src_str = entry.src.strip_suffix(" hurt").unwrap_or(&*entry.src);
        // if it removes something then length's won't match
        let hurt = src_str.len() != entry.src.len();

        let (src, src_lz) = if entry.src.starts_with("{") {
            let hex = &src_str[1..src_str.len()-1];
            pack_ship_hex(hex)
        } else {
            (
                TransferSource::from_str(src_str).expect("Failed to parse transfer source") as u32,
                0b111
            )
        };

        let (dst, dst_lz) = if entry.dst == "killed" { (0, 0) }
        else {
            let hex = &entry.dst[1..entry.dst.len()-1];
            pack_ship_hex(hex)
        };

        let zone = TransferZone::from_str(&*entry.zone).expect("Failed to parse transfer zone") as u8;

        let mut combined = false;
        for prev_log in packed.iter_mut().rev() {
            if entry.time - (prev_log.start_time + prev_log.eject_length as u32) > simplification_time {
                break;
            }

            if entry.item != prev_log.item { continue; }
            if zone != prev_log.zone() { continue; }
            if entry.serv != prev_log.server() { continue; }

            let eject_length = entry.time - prev_log.start_time;
            if eject_length > u16::MAX as u32 { continue; }

            // Continuing an eject is much more common than ejecting back
            if prev_log.eq_src(src, src_lz) && prev_log.eq_dst(dst, dst_lz) {
                prev_log.log_count += 1;
                prev_log.eject_length = eject_length as u16;
                prev_log.count += entry.count;
                if hurt != prev_log.hurt() { prev_log.set_partial_hurt_true() }
                combined = true;
                break;
            }
            else if prev_log.eq_src(dst, dst_lz) && prev_log.eq_dst(src, src_lz) {
                prev_log.log_count += 1;
                prev_log.eject_length = eject_length as u16;
                prev_log.count -= entry.count;
                if hurt != prev_log.hurt() { prev_log.set_partial_hurt_true() }
                combined = true;
                break;
            }
        }
        if !combined {
            packed.push(PackedTransferLog {
                src,
                dst,
                start_time: entry.time,
                count: entry.count,
                eject_length: 0,
                log_count: 1,
                item: entry.item,
                packed_1: PackedTransferLog::create_pack_one(zone, src_lz),
                packed_2: PackedTransferLog::create_pack_two(entry.serv, hurt, false, dst_lz)
            });
        }
    }
    let condensed_count = packed.len();
    progress_bar.println(format!("Condensation ratio: {}/{} = {:.2}%", condensed_count, original_count, (condensed_count as f64 / original_count as f64) * 100.0));
    packed.shrink_to_fit();
    // Make sure count is always positive, swap negative counts
    for entry in packed.iter_mut() {
        if entry.count < 0 {
            let src = entry.src;
            let src_lz = entry.src_lz();
            let dst = entry.dst;
            let dst_lz = entry.dst_lz();
            entry.src = dst;
            entry.dst = src;
            entry.set_src_lz(dst_lz);
            entry.set_dst_lz(src_lz);
            entry.count = -entry.count;
        }
    }

    // Sort by src and dst so we can binary search through the logs when searching for a particular ship
    let mut src_indexes: Vec<u32> = (0..packed.len() as u32).collect();
    let mut dst_indexes: Vec<u32> = (0..packed.len() as u32).collect();
    src_indexes.sort_by_key(|&i| packed_ship_hex_to_hash(packed[i as usize].src, packed[i as usize].src_lz()));
    dst_indexes.sort_by_key(|&i| packed_ship_hex_to_hash(packed[i as usize].dst, packed[i as usize].dst_lz()));
    TransferResult {
        logs: packed,
        src_indexes,
        dst_indexes
    }
}


/// Fetches a bunch of ships and return a hashmap
pub fn fetch_ships(year: u32, month: u32, day: u32, count: u32) -> IntMap<u64, PackedShipEntry> {
    let response = reqwest::blocking::get(format!("https://pub.drednot.io/prod/econ/{}_{}_{}/ships.json.gz", year, month, day)).expect("Failed to fetch ship logs");
    let gz = GzDecoder::new(response);

    let mut map: IntMap<u64, PackedShipEntry> = IntMap::with_capacity_and_hasher(count as usize, Default::default());

    for result in stream_json_array::<ShipJsonEntry, _>(gz) {
        let entry = result.expect("Failed to parse ship log");
        let (ship_hex, ship_lz) = pack_ship_hex(&*entry.hex_code);
        let ship_hash: u64 = packed_ship_hex_to_hash(ship_hex, ship_lz);

        let mut items = Vec::with_capacity(entry.items.len());
        for (item_key, item_count) in entry.items {
            let item = u32::from_str(&*item_key).unwrap_or_else(|_| panic!("Failed to parse item: {}", item_key));
            if item > u16::MAX as u32 { continue; }
            items.push((item as u16, item_count));
        }

        map.insert(ship_hash, PackedShipEntry {
            name: entry.name,
            color: entry.color,
            hex: ship_hex,
            hex_lz: ship_lz,
            items
        });
    }
    map
}

pub fn get_item_data() -> (IntMap<u16, ItemSchema>, HashMap<u16, f64>) {
    let data = reqwest::blocking::get("https://pub.drednot.io/prod/econ/item_schema.json").unwrap();
    let items: Vec<ItemSchema> = data.json().expect("Failed to parse item schema");
    let mut worth_string = include_str!("worth.jsonc").to_string();
    json_strip_comments::strip(&mut *worth_string).expect("Failed to strip comments");
    let mut item_worth: HashMap<u16, f64> = serde_json::from_str(&*worth_string).unwrap();
    // Compressed iron
    item_worth.insert(50, item_worth.get(&1).unwrap() * 64.0);
    // Compressed explosives
    item_worth.insert(49, item_worth.get(&2).unwrap() * 64.0);

    let mut item_names = IntMap::with_capacity_and_hasher(items.len(), Default::default());
    for mut item in items {
        match item.id {
            167 | // Cooling cell (hot)
            244 | // Old loader
            327 // Elimination lootbox
            => { continue }
            _ => {}
        }
        if !item_worth.contains_key(&item.id) {
            let fab_recipe = item.fab_recipe.unwrap_or_else(|| panic!("Failed to find worth: {}, {}, {}", item.id, item.name, item.image));
            let count = fab_recipe.count;
            let mut worth = 0f64;
            for input in &fab_recipe.input {
                worth += item_worth[&input.id] * input.count as f64;
            }
            worth /= count as f64;
            item_worth.insert(item.id, worth);
            item.fab_recipe = Some(fab_recipe)
        }
        item_names.insert(item.id, item);
    }
    // Cooling cell hot = cooling cell
    item_worth.insert(167, *item_worth.get(&166).unwrap());
    // Old loader
    item_worth.insert(244, *item_worth.get(&252).unwrap());
    // Locked Elimination lootbox
    item_worth.insert(327, *item_worth.get(&326).unwrap());

    (item_names, item_worth)
}