use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::string::String;

use serde::Serialize;

use crate::parser::{JsonPath, TableRecord};

#[derive(Serialize)]
pub struct SourceColumn {
    pub source_path: JsonPath,
}

#[derive(Serialize)]
pub enum ColumnSchema {
    SourceColumn(SourceColumn),
    PrimaryKey,
    ForeignKey,
}

#[derive(Serialize)]
pub struct TableSchema {
    #[serde(skip_serializing)]
    seen_cols: HashSet<JsonPath>,
    // Ordered mapping of json paths to string
    pub columns: Vec<ColumnSchema>,
    pub name: String,
    pub path: Vec<JsonPath>,
}

impl TableSchema {
    pub fn empty_with_ids(name: String, path: Vec<JsonPath>) -> TableSchema {
        let mut schema = TableSchema {
            seen_cols: HashSet::new(),
            columns: Vec::new(),
            name,
            path,
        };
        schema.add_column(ColumnSchema::PrimaryKey);
        schema.add_column(ColumnSchema::ForeignKey);

        schema
    }

    pub fn add_column(&mut self, col: ColumnSchema) {
        self.columns.push(col);
    }

    pub fn update(&mut self, rec: &TableRecord) {
        for k in rec.keys() {
            if !self.seen_cols.contains(k) {
                self.seen_cols.insert(k.clone());
                self.add_column(ColumnSchema::SourceColumn(SourceColumn {
                    source_path: k.clone(),
                }));
            }
        }
    }
}

#[derive(Serialize)]
pub struct DatabaseSchema {
    #[serde(skip_serializing)]
    table_path_to_id: HashMap<Vec<JsonPath>, usize>,
    // When table schema is borrowed for serializing, the value will be None
    tables: Vec<Option<TableSchema>>,
}

impl DatabaseSchema {
    pub fn empty() -> DatabaseSchema {
        DatabaseSchema {
            table_path_to_id: HashMap::new(),
            tables: Vec::new(),
        }
    }

    /// Get unique table name for specified json path
    pub fn borrow_table_schema(&mut self, path: &Vec<JsonPath>) -> Option<TableSchema> {
        let table_id = self.table_path_to_id.get(path);
        match table_id {
            Some(t_id) => {
                let mut schema: Option<TableSchema> = None;
                swap(self.tables.get_mut(*t_id).unwrap(), &mut schema);
                schema
            }
            None => {
                let table_id = self.tables.len();
                self.table_path_to_id.insert(path.clone(), table_id);
                let schema = TableSchema::empty_with_ids(
                    String::from("table_") + &table_id.to_string(),
                    path.clone(),
                );
                self.tables.push(None);
                Some(schema)
            }
        }
    }

    pub fn return_table_schema(&mut self, path: &Vec<JsonPath>, schema: TableSchema) {
        let table_id = self
            .table_path_to_id
            .get(path)
            .expect("Returned table that was not borrowed");
        swap(self.tables.get_mut(*table_id).unwrap(), &mut Some(schema));
    }

    pub fn ensure_all_tables_returned(&self) {
        for t in self.tables.iter() {
            match t {
                None => {
                    panic!("Table was not returned!")
                }
                Some(_) => {}
            }
        }
    }
}
