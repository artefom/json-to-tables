use crate::parser::{TableLocation, TableRecord};

use super::Database;

pub struct TableStdout {}

pub struct DatabaseStdout {}

impl DatabaseStdout {
    pub fn new() -> DatabaseStdout {
        DatabaseStdout {}
    }
}

impl Database for DatabaseStdout {
    fn write(&mut self, table: TableLocation, record: TableRecord) {
        println!("{:?}: {:?}", table, record)
    }

    fn flush(&mut self) {}
}
