use std::collections::HashMap;
use std::io::BufRead;

use crate::database::Database;
use crate::parser::{JsonPath, NestedObjectHandler, TableLocation, TableRecord};
use crate::yajlish::Parser;

pub fn read_to_db<D: Database, B: BufRead>(mut database: D, mut reader: B) {
    let mut consumer = |loc: TableLocation, rec: TableRecord| database.write(loc, rec);
    let mut handler = NestedObjectHandler::new(&mut consumer);
    let mut parser = Parser::new(&mut handler);
    parser.parse(&mut reader).unwrap();
}

/// Remaps ids from local parsers to global
/// local parsers can be potentially executed in parallel and they do yield ids in local space
///
/// Keeps track of latest ids in each table
/// Keeps track to which ids was initial data remapped
struct IdRemapper {
    // Keeps track latest ids for tables
    tables: HashMap<Vec<JsonPath>, i32>,

    // Keeps track of remapped ids from specific parsers
    remap_store: HashMap<usize, HashMap<Vec<JsonPath>, HashMap<i32, i32>>>,
}

impl IdRemapper {
    pub fn new() -> IdRemapper {
        IdRemapper {
            tables: HashMap::new(),
            remap_store: HashMap::new(),
        }
    }

    pub fn start_remapper(&mut self) -> usize {
        let remapper_id = self.remap_store.len();
        self.remap_store.insert(remapper_id, HashMap::new());
        remapper_id
    }

    pub fn finish_remapper(&mut self, remapper_id: usize) {
        self.remap_store.remove(&remapper_id);
    }

    /// Find object id in mappers for tables or if does not exist create it and return
    fn find_obj_id(
        &mut self,
        remapper_id: usize,
        table_path: &Vec<JsonPath>,
        object_id: i32,
    ) -> i32 {
        let remapper = self.remap_store.get_mut(&remapper_id).unwrap();
        let remapper_id_store = match remapper.get_mut(table_path) {
            Some(m) => m,
            None => {
                remapper.insert(table_path.clone(), HashMap::new());
                remapper.get_mut(table_path).unwrap()
            }
        };
        match remapper_id_store.get_mut(&object_id) {
            Some(i) => *i, // Object id found in id map
            None => {
                // No object id found in id map
                let current_id = match self.tables.get_mut(table_path) {
                    Some(i) => i,
                    None => {
                        self.tables.insert(table_path.clone(), 0);
                        self.tables.get_mut(table_path).unwrap()
                    }
                };
                // Clone current id before incrementing, increment and return cloned
                let insertion_id = current_id.clone();
                // Remember id to which we mapped the data
                remapper_id_store.insert(object_id, insertion_id);
                *current_id += 1;
                insertion_id
            }
        }
    }

    pub fn remap_ids(&mut self, remapper_id: usize, mut loc: TableLocation) -> TableLocation {
        loc.object_id = self.find_obj_id(remapper_id, &loc.table_path, loc.object_id);
        loc.parent_object_id =
            self.find_obj_id(remapper_id, &loc.parent_table_path(), loc.parent_object_id);
        loc
    }
}

pub fn read_to_db_many<D: Database, B: BufRead, C>(
    database: &mut D,
    readers: Vec<(C, B)>,
    callback_success: &mut dyn FnMut(C, usize),
) {
    let mut id_remapper = IdRemapper::new();

    for (args, mut reader) in readers {
        let remapper_id = id_remapper.start_remapper();
        let mut num_records: usize = 0;

        let mut consumer = |mut loc: TableLocation, rec: TableRecord| {
            loc = id_remapper.remap_ids(remapper_id, loc);
            num_records += 1;
            database.write(loc, rec)
        };

        let mut handler = NestedObjectHandler::new(&mut consumer);
        let mut parser = Parser::new(&mut handler);
        parser.parse(&mut reader).unwrap();

        id_remapper.finish_remapper(remapper_id);

        (callback_success)(args, num_records);
    }
}
