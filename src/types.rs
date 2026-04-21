use crate::utils::{packed_hex_to_string, packed_ship_hex_to_hash};
use bytemuck::{Pod, Zeroable};
use chrono::DateTime;
use deepsize::DeepSizeOf;
use nohash_hasher::IntMap;
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

#[derive(Clone, Debug)]
pub struct TransferLog {
    pub src_name: String,
    pub dst_name: String,
    pub start_time_string: String,
    pub item_name: String,
    pub start_time: u32,
    pub eject_length: u16,
    pub count: i32,
    pub consolidated: u16,
    pub item: u16,
    pub zone: TransferZone,
    pub server: u8,
    pub hurt: bool,
    pub partial_hurt: bool,
    pub src_hash: u64,
    pub dst_hash: u64
}


impl TransferLog {
    pub fn create(p: &PackedTransferLog, src_name: String, dst_name: String, item_name: String) -> TransferLog {
        let datetime = DateTime::from_timestamp_secs(p.start_time as i64).expect("Invalid timestamp");
        let start_time_string = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        TransferLog {
            src_name,
            dst_name,
            item_name,
            start_time_string,
            start_time: p.start_time,
            eject_length: p.eject_length,
            count: p.count,
            consolidated: p.log_count,
            item: p.item,
            zone: TransferZone::try_from_primitive(p.zone()).unwrap(),
            server: p.server(),
            hurt: p.hurt(),
            partial_hurt: p.partial_hurt(),
            src_hash: packed_ship_hex_to_hash(p.src, p.src_lz()),
            dst_hash: packed_ship_hex_to_hash(p.dst, p.dst_lz())
        }
    }
}

#[derive(DeepSizeOf)]
pub struct ShipNameEntry {
    pub index: usize,
    pub name: String,
    pub normalized_name: String,
    pub color: u32
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, DeepSizeOf)]
pub struct PackedTransferLog {
    pub src: u32,
    pub dst: u32,
    pub start_time: u32,
    pub count: i32,
    pub eject_length: u16,
    pub log_count: u16,
    pub item: u16,
    // zone: 5 bits
    // src length: 3 bits
    pub packed_1: u8,
    // server: 2 bits
    // hurt: 1 bit
    // partial_hurt: 1 bit
    // dst length: 3 bits
    pub packed_2: u8
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackedShipEntry {
    pub items: Vec<(u16, u32)>,
    pub name: String,
    pub hex: u32,
    pub color: u32,
    pub hex_lz: u8,
}

#[derive(DeepSizeOf)]
pub struct MemoryShipEntry {
    pub items: Vec<(u16, u32)>,
    pub time: u32,
    pub hex: u32,
    pub hex_lz: u8
}

impl PackedTransferLog {
    #[inline(always)]
    pub fn zone(&self) -> u8 {
        self.packed_1 >> 3
    }
    #[inline(always)]
    pub fn src_lz(&self) -> u8 {
        self.packed_1 & 0b111
    }
    #[inline(always)]
    pub fn server(&self) -> u8 {
        self.packed_2 >> 5
    }
    #[inline(always)]
    pub fn hurt(&self) -> bool {
        (self.packed_2 & 0b0010000) > 0
    }
    #[inline(always)]
    pub fn partial_hurt(&self) -> bool {
        (self.packed_2 & 0b0001000) > 0
    }
    #[inline(always)]
    pub fn dst_lz(&self) -> u8 {
        self.packed_2 & 0b111
    }

    #[inline(always)]
    pub fn set_src_lz(&mut self, lz: u8) {
        self.packed_1 = (self.packed_1 & 0b11111000) | lz
    }
    #[inline(always)]
    pub fn set_dst_lz(&mut self, lz: u8) {
        self.packed_2 = (self.packed_2 & 0b11111000) | lz
    }
    #[inline(always)]
    pub fn set_partial_hurt_true(&mut self) {
        self.packed_2 |= 0b00001000
    }

    #[inline(always)]
    pub fn create_pack_one(zone: u8, src_lz: u8) -> u8 {
        (zone << 3) | src_lz
    }

    #[inline(always)]
    pub fn create_pack_two(server: u8, hurt: bool, partial_hurt: bool, dst_lz: u8) -> u8 {
        (server << 5) | ((hurt as u8) << 4) | ((partial_hurt as u8) << 3) | dst_lz
    }

    #[inline(always)]
    pub fn eq_src(&self, other: u32, other_lz: u8) -> bool {
        (other == self.src) && (self.src_lz() == other_lz)
    }
    #[inline(always)]
    pub fn eq_dst(&self, other: u32, other_lz: u8) -> bool {
        (other == self.dst) && (self.dst_lz() == other_lz)
    }

    pub fn src_string(&self) -> String {
        let src_lz = self.src_lz();
        if src_lz == 0b111 { TransferSource::try_from_primitive(self.src).expect("Failed to convert to TransferSource").to_string() }
        else { packed_hex_to_string(self.src, src_lz) }
    }

    pub fn dst_string(&self) -> String {
        let lz = self.dst_lz();
        if self.dst == 0 && lz == 0 { "killed".to_string() }
        else { packed_hex_to_string(self.dst, lz) }
    }

    pub fn zone_string(&self) -> String {
        TransferZone::try_from_primitive(self.zone()).expect("Failed to convert to TransferZone").to_string()
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, EnumString, Display, TryFromPrimitive, Zeroable)]
pub enum TransferZone {
    #[strum(serialize = "The Nest")]
    TheNest = 0,
    #[strum(serialize = "Hummingbird")]
    Hummingbird = 1,
    #[strum(serialize = "Finch")]
    Finch = 2,
    #[strum(serialize = "Sparrow")]
    Sparrow = 3,
    #[strum(serialize = "Raven")]
    Raven = 4,
    #[strum(serialize = "Falcon")]
    Falcon = 5,
    #[strum(serialize = "Canary")]
    Canary = 6,
    #[strum(serialize = "The Pits")]
    ThePits = 7,
    #[strum(serialize = "Vulture")]
    Vulture = 8,
    #[strum(serialize = "Event Lobby")]
    EventLobby = 9,
    #[strum(serialize = "Freeport I")]
    FreeportI = 10,
    #[strum(serialize = "Freeport II")]
    FreeportII = 11,
    #[strum(serialize = "Freeport III")]
    FreeportIII = 12,
    #[strum(serialize = "Combat Simulator")]
    CombatSimulator = 13,
    // Super Special Event Zone, but shorter
    #[strum(serialize = "Super Special Event Zone")]
    SuperSpecialEventZone = 14,
    #[strum(serialize = "Freeport")]
    Freeport = 15
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, EnumString, Display, TryFromPrimitive)]
pub enum TransferSource {
    #[strum(serialize = "Orange Fool")]
    OrangeFool = 0,
    #[strum(serialize = "The Coward")]
    TheCoward = 1,
    #[strum(serialize = "Red Sentry")]
    RedSentry = 2,
    #[strum(serialize = "Blue Rusher")]
    BlueRusher = 3,
    #[strum(serialize = "Aqua Shielder")]
    AquaShielder = 4,
    #[strum(serialize = "The Shield Master")]
    TheShieldMaster = 5,
    #[strum(serialize = "Shield Helper")]
    ShieldHelper = 6,
    #[strum(serialize = "Yellow Hunter")]
    YellowHunter = 7,
    #[strum(serialize = "Red Sniper")]
    RedSniper = 8,
    #[strum(serialize = "The Lazer Enthusiast")]
    TheLazerEnthusiast = 9,
    #[strum(serialize = "Yellow Mine Guard")]
    YellowMineGuard = 10,
    #[strum(serialize = "bot - zombie")]
    OldZombieBot = 11,
    #[strum(serialize = "bot - zombie tank")]
    OldZombieTank = 12,
    #[strum(serialize = "bot - zombie hunter")]
    OldZombieHunter = 13,
    #[strum(serialize = "bot - zombie boss")]
    OldZombieBoss = 14,
    #[strum(serialize = "giant rubber ball")]
    GiantRubberBall = 15,
    #[strum(serialize = "block - iron mine")]
    IronMine = 16,
    #[strum(serialize = "block - flux node")]
    FluxNode = 17,
    #[strum(serialize = "block - vault")]
    Vault = 18,
    #[strum(serialize = "block - treasure diamond")]
    TreasureDiamond = 19,
    #[strum(serialize = "block - flux mine")]
    FluxMine = 20,
    #[strum(serialize = "bot - blue melee")]
    OldBlueMelee = 21,
    #[strum(serialize = "bot - orange fool")]
    OldOrangeFool = 22,
    #[strum(serialize = "bot - red sentry")]
    OldRedSentry = 23,
    #[strum(serialize = "bot - yellow rusher")]
    OldYellowRusher = 24,
    #[strum(serialize = "bot - red hunter")]
    OldRedHunter = 25,
    #[strum(serialize = "bot - green roamer")]
    OldGreenRoamer = 26,
    #[strum(serialize = "bot - blue shield")]
    OldBlueShield = 27,
}


#[derive(Serialize, Deserialize)]
pub struct ShipDataFile(pub IntMap<u64, PackedShipEntry>);