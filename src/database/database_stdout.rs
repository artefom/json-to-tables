use anyhow::Result;

use crate::database::DatabaseSchema;
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
    fn get_schema(&self) -> &DatabaseSchema {
        todo!()
    }

    fn get_schema_mut(&mut self) -> &mut DatabaseSchema {
        todo!()
    }

    fn write(&mut self, table: TableLocation, record: TableRecord) -> Result<()> {
        println!("{:?}: {:?}", table, record);
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
