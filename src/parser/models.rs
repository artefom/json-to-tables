use std::collections::HashMap;

use json::JsonValue;

pub type JsonPath = Vec<String>;

/// Path in json file without nested tables
pub type TJPath = Vec<JsonPath>;

/// Short for Nested Table Json Path
/// Path object that describes path in json that may contain nested lists
/// Does not store positions in lists
pub type NTJPath = Vec<TJPath>;

pub type TableRecord = HashMap<NTJPath, JsonValue>;