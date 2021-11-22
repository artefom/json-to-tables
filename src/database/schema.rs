use std::collections::HashMap;
use std::mem::swap;
use std::string::String;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::parser::{JsonPath, TableRecord};

#[derive(Deserialize, Serialize)]
pub struct SourceColumn {
    pub source_path: JsonPath,
    pub is_nullable: bool,
    pub is_null: bool,
    pub is_bool: bool,
    pub is_i64: bool,
    pub is_f64: bool,
    pub example_values: Vec<Value>,
}

#[derive(Deserialize, Serialize)]
pub enum ColumnSchema {
    SourceColumn(SourceColumn),
    PrimaryKey,
    ForeignKey,
}

#[derive(Deserialize, Serialize)]
pub struct TableSchema {
    #[serde(skip_deserializing,skip_serializing)]
    path_to_id: HashMap<JsonPath, usize>,
    // Ordered mapping of json paths to string
    pub columns: Vec<ColumnSchema>,
    pub name: String,
    pub path: Vec<JsonPath>,
}


impl TableSchema {
    pub fn empty_with_ids(name: String, path: Vec<JsonPath>) -> TableSchema {
        let mut schema = TableSchema {
            path_to_id: HashMap::new(),
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
        for (k, v) in rec.iter() {
            let col_id = self.path_to_id.get(k);
            let col_id = if col_id.is_none() {
                self.add_column(ColumnSchema::SourceColumn(SourceColumn {
                    source_path: k.clone(),
                    is_nullable: false,
                    is_null: true,
                    is_bool: true,
                    is_i64: true,
                    is_f64: true,
                    example_values: Vec::new(),
                }));
                self.path_to_id.insert(k.clone(), self.columns.len() - 1);
                self.columns.len() - 1
            } else {
                *col_id.unwrap()
            };
            // Update column status with value
            let col = &mut self.columns[col_id];
            match col {
                ColumnSchema::SourceColumn(ref mut _col) => {
                    _col.is_nullable = _col.is_nullable || v.is_null();
                    _col.is_null = _col.is_null && v.is_null();
                    _col.is_bool = _col.is_bool && v.is_boolean();
                    _col.is_i64 = _col.is_i64 && v.is_i64();
                    _col.is_f64 = _col.is_f64 && v.is_f64();

                    if _col.example_values.len() < 5 && !v.is_null() {
                        _col.example_values.push(v.clone());
                    }
                }
                ColumnSchema::PrimaryKey => {}
                ColumnSchema::ForeignKey => {}
            }
        }
    }
}

#[derive(Serialize)]
pub struct DatabaseSchema {
    #[serde(skip_deserializing,skip_serializing)]
    table_path_to_id: HashMap<Vec<JsonPath>, usize>,
    // When table schema is borrowed for serializing, the value will be None
    tables: Vec<Option<TableSchema>>,
}

impl<'de> Deserialize<'de> for DatabaseSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        let db: DatabaseSchema = Deserialize::deserialize(deserializer)?;
        Ok(db)
    }
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
