use std::collections::HashMap;
use std::fs::{create_dir, File};
use std::io::{BufWriter, Write};
use std::mem::swap;
use std::path::PathBuf;

use serde_json::Value;

use crate::database::{ColumnSchema, DatabaseSchema, TableSchema};
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
    if s.len() == 0 {
        return String::from("\"\"");
    }

    if s.contains('\"') || s.contains(',') || s.contains('\n') {
        csv_field_quote(s)
    } else {
        s.clone()
    }
}

pub struct TableCsv {
    writer: BufWriter<File>,
    schema: Option<TableSchema>,
}

impl TableCsv {
    pub fn new(schema: TableSchema, file: File) -> TableCsv {
        TableCsv {
            writer: BufWriter::new(file),
            schema: Some(schema),
        }
    }

    fn value_to_str(v: &serde_json::Value) -> Option<String> {
        match v {
            Value::Null => None,
            Value::Bool(v) => Some(v.to_string()),
            Value::Number(v) => Some(v.to_string()),
            Value::String(v) => Some(csv_field_escape(v)),
            Value::Array(_) => {
                panic!("Arrays are not allowed in record")
            }
            Value::Object(_) => {
                panic!("Objects are not allowed in record")
            }
        }
    }

    pub fn make_columns(&mut self, loc: TableLocation, rec: TableRecord) -> Vec<Option<String>> {
        let schema = self.schema.as_mut().unwrap();
        schema.update(&rec);
        schema
            .columns
            .iter()
            .map(|col| match col {
                ColumnSchema::SourceColumn(col) => match rec.get(&col.source_path) {
                    Some(t) => TableCsv::value_to_str(t),
                    None => None,
                },
                ColumnSchema::PrimaryKey => Some(loc.object_id.to_string()),
                ColumnSchema::ForeignKey => Some(loc.parent_object_id.to_string()),
            })
            .collect::<Vec<_>>()
    }

    pub fn write(&mut self, loc: TableLocation, rec: TableRecord) {
        // Convert record to set of strings
        let vals = self.make_columns(loc, rec);

        // Create buffered string for writing to file
        let line = vals
            .iter()
            .map(|v| match v {
                Some(v) => {
                    // Escape separator and quotes
                    v.clone()
                }
                None => String::from(""),
            })
            .collect::<Vec<_>>()
            .join(",");

        let buf = line.as_bytes();

        // Write new line to csv file from fields
        self.writer.write(buf).unwrap();
        self.writer.write(b"\n").unwrap();
    }

    pub fn close(&mut self) {
        self.writer.flush().unwrap();
    }

    pub fn pop_schema(&mut self) -> Option<TableSchema> {
        let mut schema: Option<TableSchema> = None;
        swap(&mut self.schema, &mut schema);
        schema
    }
}

pub struct DatabaseCsv {
    schema: DatabaseSchema,
    path: PathBuf,
    tables: HashMap<Vec<JsonPath>, TableCsv>,
}

impl DatabaseCsv {
    pub fn new(schema: DatabaseSchema, path: PathBuf) -> DatabaseCsv {
        let mut data_path = path.clone();
        data_path.push("data");

        match create_dir(path.clone()) {
            Ok(_) => {
                create_dir(data_path).expect("could not create data directory");
            }
            Err(_) => {
                create_dir(data_path).expect("could not create data directory (already exists?)");
            }
        }

        DatabaseCsv {
            tables: HashMap::new(),
            path,
            schema,
        }
    }

    fn get_or_create_table_mut(&mut self, table_path: &Vec<JsonPath>) -> &mut TableCsv {
        if !self.tables.contains_key(table_path) {
            // Table schema can only be poped once, transferring ownership of the schema to the table
            // Consequent calls to pop table_schema for same table path should panic
            let table_schema = self.schema.borrow_table_schema(table_path).unwrap();
            let table_name = &table_schema.name;

            let data_filename = table_name.clone() + ".csv";

            let mut data_path = self.path.clone();
            data_path.push("data");
            data_path.push(data_filename);

            let data_file = File::create(data_path.as_path()).unwrap();

            self.tables
                .insert(table_path.clone(), TableCsv::new(table_schema, data_file));
        }
        self.tables.get_mut(table_path).unwrap()
    }
}

impl Database for DatabaseCsv {
    fn get_schema(&self) -> &DatabaseSchema {
        &self.schema
    }

    fn get_schema_mut(&mut self) -> &mut DatabaseSchema {
        &mut self.schema
    }

    fn write(&mut self, loc: TableLocation, record: TableRecord) {
        let table = self.get_or_create_table_mut(&loc.table_path);
        table.write(loc, record);
    }

    fn close(&mut self) {
        for (table_path, table) in self.tables.iter_mut() {
            table.close();
            self.schema.return_table_schema(
                table_path,
                table
                    .pop_schema()
                    .expect("Tried return non-existent schema"),
            );
        }

        self.schema.ensure_all_tables_returned();

        // Save schema o file
        let mut schema_filename = self.path.clone();
        schema_filename.push("schema.json");
        let schema_file = File::create(schema_filename.as_path()).unwrap();
        serde_json::to_writer_pretty(BufWriter::new(schema_file), &self.schema)
            .expect("Could not write schema file");
    }
}
