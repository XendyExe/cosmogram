use std::path::Path;
use cosmogram::Cosmogram;

fn main() {
    let save_path = Path::new("./data/");
    let logger = Cosmogram::new(&save_path);
    let search_ship = "90DB";
    let result = logger.get_transfer_overview_by_src_hex(search_ship, None, None);
    println!("trans: {}", logger.trans.iter().map(|v| v.len()).sum::<usize>());
    println!("ships: {}", logger.ships.len());
    println!("ship_names: {}", logger.ship_names.len());
    logger.print_memory_usage();
    // println!("{}", result.table);
    // println!("Fetched and converted {} logs from {} total logs in {} ms", result.result_logs, result.total_logs, result.search_time * 1000.0);
    // println!("Created a neat chart in {} ms", result.table_time * 1000.0);
    // println!("Found top in {} ms", result.top_time * 1000.0);
    // println!("Top 10 most transfered: ");
    // for (ship_name, flux) in result.top {
    //     println!("{}: {}", ship_name, flux);
    // }
}
