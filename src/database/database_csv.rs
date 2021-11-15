use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use serde::Serialize;
use serde_json::Value;

use crate::parser::{JsonPath, TableLocation, TableRecord};

use super::Database;

/// Add quotes around csv string and escape them
pub fn csv_field_quote(s: &String) -> String {
    String::from("\"") + &s.replace("\"", "\"\"") + "\""
}

/// Escape double quotes and commas in string,
/// making it valid csv record
pub fn csv_field_escape(s: &String) -> String {
    // Early return for empty strings
    if s.len() == 0 { return String::from("\"\""); }

    if s.contains('\"') || s.contains(',') || s.contains('\n') {
        csv_field_quote(s)
    } else {
        s.clone()
    }
}

#[derive(Serialize)]
pub struct Schema {
    #[serde(skip_serializing)]
    seen_cols: HashSet<JsonPath>,
    // Ordered mapping of json paths to string
    columns: Vec<JsonPath>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            seen_cols: HashSet::new(),
            columns: Vec::new(),
        }
    }

    fn add_all_columns(&mut self, rec: &TableRecord) {
        for k in rec.keys() {
            if !self.seen_cols.contains(k) {
                self.seen_cols.insert(k.clone());
                self.columns.push(k.clone());
            }
        }
    }

    fn value_to_str(v: &serde_json::Value) -> Option<String> {
        match v {
            Value::Null => { None }
            Value::Bool(v) => { Some(v.to_string()) }
            Value::Number(v) => { Some(v.to_string()) }
            Value::String(v) => { Some(csv_field_escape(v)) }
            Value::Array(_) => { panic!("Arrays are not allowed in record") }
            Value::Object(_) => { panic!("Objects are not allowed in record") }
        }
    }

    pub fn make_columns(&mut self, rec: TableRecord) -> Vec<Option<String>> {
        self.add_all_columns(&rec);
        self.columns.iter().map(|col| match rec.get(col) {
            Some(t) => { Schema::value_to_str(t) }
            None => None
        }).collect::<Vec<_>>()
    }
}

pub struct TableCsv {
    writer: BufWriter<File>,
    schema: Schema,
    schema_writer: BufWriter<File>,
}


impl TableCsv {
    pub fn new(file: File, schema_file: File) -> TableCsv {
        TableCsv {
            writer: BufWriter::new(file),
            schema: Schema::new(),
            schema_writer: BufWriter::new(schema_file),
        }
    }

    pub fn write(&mut self, rec: TableRecord) {
        // Convert record to set of strings
        let vals = self.schema.make_columns(rec);

        // Create buffered string for writing to file
        let line = vals.iter().map(|v| match v {
            Some(v) => {
                // Escape separator and quotes
                v.clone()
            }
            None => String::from("")
        }).collect::<Vec<_>>().join(",");

        let buf = line.as_bytes();


        // Write new line to csv file from fields
        self.writer.write(buf).unwrap();
        self.writer.write(b"\n").unwrap();
    }

    pub fn flush(&mut self) {
        self.writer.flush().unwrap();
        serde_json::to_writer_pretty(&mut self.schema_writer, &self.schema).unwrap();
        self.schema_writer.flush().unwrap();
    }
}

pub struct DatabaseCsv {
    name: String,
    path: PathBuf,
    tables: HashMap<Vec<JsonPath>, TableCsv>,
}

fn table_location_to_filename(root_name: &String, table_path: &Vec<JsonPath>) -> String {
    fn json_path_to_str(json_loc: &JsonPath) -> String {
        json_loc.join(".")
    }

    table_path.iter()
        .map(|name| json_path_to_str(name))
        .fold(root_name.clone(), |prev, next| prev + "-" + &next)
}

impl DatabaseCsv {
    pub fn new(name: String, path: PathBuf) -> DatabaseCsv {
        DatabaseCsv {
            tables: HashMap::new(),
            name,
            path,
        }
    }

    fn get_or_create_table_mut(&mut self, table_path: &Vec<JsonPath>) -> &mut TableCsv {
        if !self.tables.contains_key(table_path) {
            let table_name = table_location_to_filename(&self.name, table_path);

            let data_filename = table_name.clone() + ".csv";
            let schema_filename = table_name.clone() + ".json";

            let mut data_path = self.path.clone();
            data_path.push(data_filename);
            let mut schema_path = self.path.clone();
            schema_path.push(schema_filename);

            let data_file = File::create(data_path.as_path()).unwrap();
            let schema_file = File::create(schema_path.as_path()).unwrap();

            self.tables.insert(table_path.clone(), TableCsv::new(data_file, schema_file));
        }
        self.tables.get_mut(table_path).unwrap()
    }
}

impl Database for DatabaseCsv {
    fn write(&mut self, loc: TableLocation, record: TableRecord) {
        let table = self.get_or_create_table_mut(&loc.table_path);
        table.write(record);
    }

    fn flush(&mut self) {
        for table in self.tables.values_mut() {
            table.flush();
        }
    }
}
