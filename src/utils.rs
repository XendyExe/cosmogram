pub fn count_hex_leading_zeros(s: &str) -> u8 {
    s.chars().take_while(|&c| c == '0').count() as u8
}

pub fn pack_ship_hex(hex: &str) -> (u32, u8) {
    let hex_32 = u32::from_str_radix(hex, 16).unwrap_or_else(|_| panic!("Failed to parse src: {hex}"));
    let lz = count_hex_leading_zeros(hex);
    (hex_32, lz)
}

pub fn packed_hex_to_string(hex: u32, lz: u8) -> String {
    format!("{}{:X}", "0".repeat(lz as usize), hex)
}

#[inline(always)]
pub fn packed_ship_hex_to_hash(hex: u32, lz: u8) -> u64 {
    // lz as high bits because lz has lower entropy, which hashmaps care greatly about
    (hex as u64) | ((lz as u64) << 32)
}

pub fn ship_hex_to_hash(hex: &str) -> u64 {
    let packed = pack_ship_hex(hex);
    packed_ship_hex_to_hash(packed.0, packed.1)
}

pub fn is_hash_4_digit(hash: u64) -> bool {
    let value = hash as u32;
    let lz = (hash >> 32) as u32;
    let hex_len =  ((32 - value.leading_zeros()) + 3) >> 2;
    lz + hex_len <= 4
}