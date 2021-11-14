use std::collections::HashMap;

use serde_json::Value as JsonValue;

/// Path in json file without nested tables
pub type JsonPath = Vec<String>;

pub type TableRecord = HashMap<JsonPath, JsonValue>;

#[derive(Debug)]
pub struct TableLocation {
    pub table_path: Vec<JsonPath>,
    pub object_id: i32,
    pub parent_object_id: i32,
}