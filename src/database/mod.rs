pub use database_csv::DatabaseCsv;
pub use database_json::DatabaseJson;
pub use database_stdout::DatabaseStdout;
pub use schema::{ColumnSchema, DatabaseSchema, TableSchema};

use crate::parser::{TableLocation, TableRecord};

pub mod database_csv;
pub mod database_json;
pub mod database_stdout;
pub mod schema;

/// Used as sink for records
pub trait Database {
    fn get_schema(&self) -> &DatabaseSchema;
    fn get_schema_mut(&mut self) -> &mut DatabaseSchema;
    fn write(&mut self, table: TableLocation, record: TableRecord);
    fn close(&mut self);
}
