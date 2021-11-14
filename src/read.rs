use std::io::BufRead;

use crate::database::Database;
use crate::parser::{NestedObjectHandler, TableLocation, TableRecord};
use crate::yajlish::Parser;

pub fn read_to_database<D: Database, B: BufRead>(mut database: D, mut reader: B) {
    let mut consumer = |loc: TableLocation, rec: TableRecord| { database.write(loc, rec) };
    let mut handler = NestedObjectHandler::new(&mut consumer);
    let mut parser = Parser::new(&mut handler);
    parser.parse(&mut reader).unwrap();
}

