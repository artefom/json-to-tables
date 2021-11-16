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

impl TableLocation {
    pub fn parent_table_path(&self) -> Vec<JsonPath> {
        let mut rv = self.table_path.clone();
        rv.pop();
        rv
    }
}
