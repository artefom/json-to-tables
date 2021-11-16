pub use database_csv::DatabaseCsv;
pub use database_json::DatabaseJson;
pub use database_stdout::DatabaseStdout;

use crate::parser::{TableLocation, TableRecord};

pub mod database_csv;
pub mod database_json;
pub mod database_stdout;

/// Used as sink for records
pub trait Database {
    fn write(&mut self, table: TableLocation, record: TableRecord);
    fn flush(&mut self);
}
