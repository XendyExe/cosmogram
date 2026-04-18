use std::collections::HashMap;
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonSummaryLog {
    pub count_ships: u32,
    pub count_logs: u32,
    pub items_held: HashMap<String, u32>,
    pub items_moved: HashMap<String, u32>,
    pub items_new: Vec<JsonSummarySource>
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonSummarySource {
    pub zone: String,
    pub src: String,
    pub item: u16,
    pub total: u32,
    pub grabbed: u32
}

#[derive(Debug, Deserialize)]
pub struct TransferJsonEntry {
    pub zone: String,
    pub src: String,
    pub dst: String,
    pub time: u32,
    pub item: u16,
    pub count: i32,
    pub serv: u8,
}
#[derive(Debug, Deserialize)]
pub struct ShipJsonEntry {
    pub items: HashMap<String, u32>,
    pub hex_code: String,
    pub name: String,
    pub color: u32
}

#[derive(Deserialize, DeepSizeOf)]
pub struct ItemSchema {
    pub id: u16,
    pub name: String,
    pub desc: String,
    pub image: String,
    pub rarity: i16,
    pub max_stack: u8,
    pub fab_recipe: Option<ItemFabRecipe>
}

#[derive(Deserialize, DeepSizeOf)]
pub struct ItemFabRecipe {
    pub count: u32,
    pub time: u32,
    pub input: Vec<FabRecipeItem>
}

#[derive(Deserialize, DeepSizeOf)]
pub struct FabRecipeItem {
    pub id: u16,
    pub count: u32,
}