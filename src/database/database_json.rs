use std::string::String;
use std::vec::Vec;

use regex::Regex;
use serde_json::{Map, Value as JsonValue};

use crate::database::DatabaseSchema;
use crate::parser::{JsonPath, TableLocation, TableRecord};

use super::Database;

fn escape_id_prefix(s: &String) -> String {
    lazy_static! {
        static ref RE_UNDERSCORE: Regex = Regex::new("^(?P<w>(?:id)+)_").unwrap();
    }

    RE_UNDERSCORE.replace_all(s, "${w}id_").to_string()
}

fn escape_nested_key_element(s: &String) -> String {
    lazy_static! {
        static ref RE_JOIN: Regex = Regex::new("_(?P<w>(?:in)+)_").unwrap();
        static ref RE_EMPTY: Regex = Regex::new("^(?P<w>(?:empty)+)$").unwrap();
        static ref RE_LIST: Regex = Regex::new("^(?P<w>(?:list)+)$").unwrap();
        static ref RE_UNDERSCORE: Regex = Regex::new("^_").unwrap();
    }

    if s == "" {
        return String::from("empty");
    }

    let s = RE_JOIN.replace_all(s, "_${w}in_").to_string();
    let s = RE_EMPTY.replace_all(&s, "${w}empty").to_string();
    let s = RE_LIST.replace_all(&s, "${w}list").to_string();
    let s = RE_UNDERSCORE.replace_all(&s, "tech_").to_string();

    return s.clone();
}

fn json_path_to_str(nested_key: &JsonPath) -> String {
    let key;
    if nested_key.len() == 0 {
        key = String::from("list");
    } else {
        let mut nested_key_rev = nested_key.clone();
        nested_key_rev.reverse();
        let nested_keys_escaped = nested_key_rev
            .iter()
            .map(escape_nested_key_element)
            .collect::<Vec<String>>();
        key = nested_keys_escaped.join("_in_");
    }
    escape_id_prefix(&key)
}

fn escape_table_path_element(s: &String) -> String {
    lazy_static! {
        static ref RE_JOIN: Regex = Regex::new("_(?P<w>(?:lin)+)_").unwrap();
    }
    let s = RE_JOIN.replace_all(s, "_${w}lin_").to_string();
    return s.clone();
}

fn table_path_to_str(root_name: &String, nested_key: &Vec<JsonPath>) -> String {
    let mut table_path = nested_key.clone();
    table_path.reverse();
    table_path.push(vec![root_name.clone()]);

    if table_path.len() == 0 {
        return String::from("");
    } else {
        let converted_path = table_path
            .iter()
            .map(|p| json_path_to_str(p))
            .collect::<Vec<String>>();
        let nested_key_rev = converted_path.clone();
        let nested_keys_escaped = nested_key_rev
            .iter()
            .map(escape_table_path_element)
            .collect::<Vec<String>>();
        return nested_keys_escaped.join("_lin_");
    }
}

fn record_to_json(root_name: &String, loc: &TableLocation, rec: &TableRecord) -> serde_json::Value {
    let mut return_val = serde_json::Value::Object(Map::new());
    let obj = return_val.as_object_mut().unwrap();

    // Insert values
    for (path, val) in rec.iter() {
        obj.insert(json_path_to_str(path), val.clone());
    }

    // Insert object ids
    let table_name = table_path_to_str(root_name, &loc.table_path);
    {
        obj.insert(
            String::from("id_") + table_name.as_str(),
            serde_json::Value::from(loc.object_id),
        );
    }

    // Insert foreign
    let mut parent_path = loc.table_path.clone();
    parent_path.pop();
    let parent_table = table_path_to_str(root_name, &parent_path);
    {
        obj.insert(
            String::from("id_") + parent_table.as_str(),
            serde_json::Value::from(loc.parent_object_id),
        );
    }

    return_val
}

pub struct DatabaseJson<'a> {
    root_name: String,
    target: &'a mut JsonValue,
}

impl<'a> DatabaseJson<'a> {
    pub fn new(root_name: String, target: &'a mut JsonValue) -> DatabaseJson<'a> {
        DatabaseJson { root_name, target }
    }
}

impl<'a> Database for DatabaseJson<'a> {
    fn get_schema(&self) -> &DatabaseSchema {
        todo!()
    }

    fn get_schema_mut(&mut self) -> &mut DatabaseSchema {
        todo!()
    }

    fn write(&mut self, table: TableLocation, record: TableRecord) {
        let table_name = table_path_to_str(&self.root_name, &table.table_path);

        let obj = self.target.as_object_mut().unwrap();
        if !obj.contains_key(&table_name) {
            obj.insert(table_name.clone(), serde_json::Value::Array(Vec::new()));
        }

        obj[&table_name]
            .as_array_mut()
            .unwrap()
            .push(record_to_json(&self.root_name, &table, &record));
    }

    fn close(&mut self) {}
}
