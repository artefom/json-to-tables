pub use database_json::DatabaseJson;
pub use database_stdout::DatabaseStdout;

use crate::parser::{TableLocation, TableRecord};

pub mod database_stdout;
pub mod database_json;

/// Used as sink for records
pub trait Database {
    fn write(&mut self, table: TableLocation, record: TableRecord);
    fn flush(&mut self);
}
