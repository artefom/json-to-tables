pub use database_json::DatabaseJson;
pub use database_stdout::DatabaseStdout;
pub use database_csv::DatabaseCsv;

use crate::parser::{TableLocation, TableRecord};

pub mod database_stdout;
pub mod database_json;
pub mod database_csv;

/// Used as sink for records
pub trait Database {
    fn write(&mut self, table: TableLocation, record: TableRecord);
    fn flush(&mut self);
}
