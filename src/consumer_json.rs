use std::string::String;

use json;
use json::JsonValue;
use json::object::Object;

pub fn consume_to_json(root_obj: &mut JsonValue, table_name: &String, object: Object) {
    // Add table name to json object if it does not exist there
    if !root_obj.has_key(table_name) {
        root_obj.insert(table_name, JsonValue::new_array()).unwrap();
    }

    // Push object to array
    let arr = &mut root_obj[table_name];
    arr.push(object).unwrap();
}
