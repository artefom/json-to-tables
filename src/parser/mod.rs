use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem::swap;
use std::string::String;
use std::vec::Vec;

use serde_json::Value as JsonValue;

pub use models::{JsonPath, TableLocation, TableRecord};

use crate::yajlish::{Context, Handler, Status};

pub mod models;

/// Handles objects within list
#[derive(Debug)]
pub struct ObjectHandler {
    object_id: i32,
    path: JsonPath,
    // Current object id within list
    rec: TableRecord,
}

impl ObjectHandler {
    pub fn new() -> ObjectHandler {
        ObjectHandler {
            object_id: 0,
            path: JsonPath::new(),
            rec: TableRecord::new(),
        }
    }

    /// Optionally produces a record if it's creation is finished
    fn pop(&mut self) -> Option<(i32, TableRecord)> {
        // Do not produce elements if we are in the process of building object
        if self.path.len() != 0 { return None; }

        let mut new_rec = TableRecord::new();
        swap(&mut self.rec, &mut new_rec);
        let ret_value = Some((self.object_id, new_rec));
        self.object_id += 1;
        return ret_value;
    }

    fn handle_json_value(&mut self, val: JsonValue) {
        self.rec.insert(self.path.clone(), val);
    }

    fn handle_start_map(&mut self) {
        self.path.push(String::from(""));
    }

    fn handle_end_map(&mut self) {
        self.path.pop();
    }

    fn handle_map_key(&mut self, key: &str) {
        *self.path.last_mut().unwrap() = String::from(key);
    }
}

/// Tree supported by HashMap that allows traversing up and down by JsonPath
struct ObjectHandlerHashTree {
    // List of all nodes
    // Nodes contain link to all child ids and parent id
    arena: Vec<(ObjectHandler, usize, HashMap<JsonPath, usize>)>,
    // Defines current node
    current_id: usize,

    // Support field stored for fast access
    current_path: Vec<JsonPath>,
}

impl ObjectHandlerHashTree {
    pub fn new() -> ObjectHandlerHashTree {
        ObjectHandlerHashTree {
            arena: vec![(ObjectHandler::new(), 0, HashMap::new())],
            current_path: Vec::new(),
            current_id: 0,
        }
    }

    pub fn full_path(&mut self) -> &Vec<JsonPath> {
        &self.current_path
    }

    fn current_tup(&self) -> &(ObjectHandler, usize, HashMap<JsonPath, usize>) {
        &self.arena[self.current_id]
    }

    fn current_tup_mut(&mut self) -> &mut (ObjectHandler, usize, HashMap<JsonPath, usize>) {
        &mut self.arena[self.current_id]
    }

    fn parent_tup(&self) -> Option<&(ObjectHandler, usize, HashMap<JsonPath, usize>)> {
        let parent_id = self.current_tup().1;
        if parent_id != self.current_id {
            Some(&self.arena[parent_id])
        } else {
            None
        }
    }

    pub fn current(&self) -> &ObjectHandler {
        &self.current_tup().0
    }

    pub fn current_mut(&mut self) -> &mut ObjectHandler {
        &mut self.current_tup_mut().0
    }

    pub fn parent(&self) -> Option<&ObjectHandler> {
        match self.parent_tup() {
            Some(t) => {
                Some(&t.0)
            }
            None => {
                None
            }
        }
    }

    pub fn go_up(&mut self) {
        let cur_obj = self.current_tup();
        let parent_id = cur_obj.1;
        self.current_id = parent_id;
        self.current_path.pop();
    }

    pub fn go_down(&mut self, path: &JsonPath) {
        // Update current path
        self.current_path.push(path.clone());

        let current_itm = self.current_tup();

        match current_itm.2.get(path) {
            Some(down_id) => {
                self.current_id = down_id.clone();
            }
            None => {
                // Add link to child object to current object
                let new_id = self.arena.len().clone();
                self.current_tup_mut().2.insert(path.clone(), new_id);

                // Create and insert new object, set current id to new object
                self.arena.push((ObjectHandler::new(), self.current_id, HashMap::new()));
                self.current_id = new_id;
            }
        };
    }
}

pub struct NestedObjectHandler<'a> {
    // Path to current database being processed
    handler_stack: ObjectHandlerHashTree,

    // stack of object handlers
    consumer: &'a mut dyn FnMut(TableLocation, TableRecord),
}

impl<'a> Debug for NestedObjectHandler<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NestedObjectHandler")
    }
}


impl<'a> NestedObjectHandler<'a> {
    pub fn new(consumer: &'a mut dyn FnMut(TableLocation, TableRecord)) -> NestedObjectHandler<'a> {
        NestedObjectHandler {
            handler_stack: ObjectHandlerHashTree::new(),
            consumer,
        }
    }

    fn current_handler(&self) -> &ObjectHandler {
        self.handler_stack.current()
    }

    fn current_handler_mut(&mut self) -> &mut ObjectHandler {
        self.handler_stack.current_mut()
    }

    fn parent_handler(&self) -> Option<&ObjectHandler> {
        self.handler_stack.parent()
    }

    fn try_pop(&mut self) {
        match self.current_handler_mut().pop() {
            Some((rec_id, rec)) => {
                let table_location = TableLocation {
                    table_path: self.handler_stack.full_path().clone(),
                    object_id: rec_id,
                    parent_object_id: match self.parent_handler() {
                        Some(p) => {
                            p.object_id
                        }
                        None => { 0 }
                    },
                };
                (self.consumer)(table_location, rec);
            }
            None => {}
        }
    }
}


impl<'a> Handler for NestedObjectHandler<'a> {
    fn handle_json_value(&mut self, _ctx: &Context, val: JsonValue) -> Status {
        self.current_handler_mut().handle_json_value(val);
        self.try_pop();
        Status::Continue
    }

    fn handle_start_map(&mut self, _ctx: &Context) -> Status {
        self.current_handler_mut().handle_start_map();
        Status::Continue
    }

    fn handle_end_map(&mut self, _ctx: &Context) -> Status {
        self.current_handler_mut().handle_end_map();
        self.try_pop();
        Status::Continue
    }

    fn handle_map_key(&mut self, _ctx: &Context, key: &str) -> Status {
        self.current_handler_mut().handle_map_key(key);
        Status::Continue
    }

    fn handle_start_array(&mut self, _ctx: &Context) -> Status {
        let current_path = &self.current_handler().path.clone();
        self.handler_stack.go_down(current_path);
        Status::Continue
    }

    fn handle_end_array(&mut self, _ctx: &Context) -> Status {
        self.handler_stack.go_up();
        self.try_pop();
        Status::Continue
    }
}