use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem::{swap};
use std::string::String;
use std::vec::Vec;

use cached::SizedCache;
use json;
use json::object::Object;
use regex::Regex;
use yajlish::{Context, Handler, Status};

#[derive(Debug)]
struct ListStackElement {
    object: Object,
    object_id: i32,
    nested_key: Vec<String>,
}

pub struct NestedObjectHandler<'a> {
    root_name: String,

    // List stack
    list_stack: Vec<ListStackElement>,

    current_nested_key: Vec<String>,
    current_object_id: i32,
    current_object: Object,

    array_ids: HashMap<String, i32>,

    consumer: &'a mut dyn FnMut(&String, Object),
}

impl<'a> Debug for NestedObjectHandler<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NestedObjectHandler")
    }
}

// find_escapes = re.compile(fr'_((?:{word})+)_', re.IGNORECASE)
// return find_escapes.sub(fr'_\g<1>{word}_', s)

// static REGEX = Regex::new(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})");


fn escape_id_prefix(s: &String) -> String {
    lazy_static! {
        static ref RE_UNDERSCORE: Regex = Regex::new("^(?P<w>(?:id)+)_").unwrap();
    }

    RE_UNDERSCORE.replace_all(s, "${w}id_").to_string()
}

fn escape_nested_key_element(s: &String) -> String {
    lazy_static! {
        static ref RE_JOIN: Regex = Regex::new("_(?P<w>(?:in)+)_").unwrap();
        static ref RE_EMPTY: Regex = Regex::new("^(?P<w>(?:empty)+)$").unwrap();
        static ref RE_LIST: Regex = Regex::new("^(?P<w>(?:list)+)$").unwrap();
        static ref RE_UNDERSCORE: Regex = Regex::new("^_").unwrap();
    }

    if s == "" {
        return String::from("empty");
    }

    let s = RE_JOIN.replace_all(s, "_${w}in_").to_string();
    let s = RE_EMPTY.replace_all(&s, "${w}empty").to_string();
    let s = RE_LIST.replace_all(&s, "${w}list").to_string();
    let s = RE_UNDERSCORE.replace_all(&s, "tech_").to_string();

    return s.clone();
}

cached! {NESTED_KEY_TO_STR: SizedCache<Vec<String>,String> = SizedCache::with_size(10000);
fn nested_key_to_str(nested_key: &Vec<String>) -> String = {
    let key;
    if nested_key.len() == 0 {
        key = String::from("list");
    } else {
        let mut nested_key_rev = nested_key.clone();
        nested_key_rev.reverse();
        let nested_keys_escaped = nested_key_rev.iter().map(escape_nested_key_element).collect::<Vec<String>>();
        key = nested_keys_escaped.join("_in_");
    }
    escape_id_prefix(&key)
}}

fn escape_table_path_element(s: &String) -> String {
    lazy_static! {
        static ref RE_JOIN: Regex = Regex::new("_(?P<w>(?:lin)+)_").unwrap();
    }
    let s = RE_JOIN.replace_all(s, "_${w}lin_").to_string();
    return s.clone();
}

cached! {TABLE_PATH_TO_STR: SizedCache<Vec<String>,String> = SizedCache::with_size(10000);
fn table_path_to_str(nested_key: &Vec<String>) -> String = {
    if nested_key.len() == 0 {
        return String::from("");
    } else {
        let nested_key_rev = nested_key.clone();
        let nested_keys_escaped = nested_key_rev.iter().map(escape_table_path_element).collect::<Vec<String>>();
        return nested_keys_escaped.join("_lin_");
    }
}}

impl<'a> NestedObjectHandler<'a> {
    pub fn new(root_name: String, root_id: i32, consumer: &'a mut dyn FnMut(&String, Object)) -> NestedObjectHandler {
        NestedObjectHandler {
            root_name,
            list_stack: Vec::new(),
            current_nested_key: Vec::new(),
            current_object: Object::new(),
            current_object_id: root_id,
            array_ids: HashMap::new(),
            consumer,
        }
    }

    pub fn handle_json_value(&mut self, value: json::JsonValue) {
        let key = nested_key_to_str(&self.current_nested_key);
        self.current_object[key] = value;
    }

    pub fn at_list_or_document_root(&mut self) -> bool {
        self.current_nested_key.len() == 0
    }

    pub fn current_list_path(&mut self) -> String {
        let mut list_path = Vec::<String>::new();
        for ls in self.list_stack.iter() {
            list_path.push(nested_key_to_str(&ls.nested_key));
        }
        list_path.reverse();
        list_path.push(self.root_name.clone());
        table_path_to_str(&list_path)
    }

    pub fn parent_list_path(&mut self) -> String {
        let mut list_path = Vec::<String>::new();
        for ls in self.list_stack.iter() {
            list_path.push(nested_key_to_str(&ls.nested_key));
        }
        list_path.pop();
        list_path.reverse();
        list_path.push(self.root_name.clone());
        table_path_to_str(&list_path)
    }

    pub fn publish_object(&mut self) {
        let table_name = self.current_list_path();
        let parent_table_name = self.parent_list_path();


        // Set object ids
        let current_id = self.current_object_id;
        self.current_object[String::from("id_") + &table_name] = json::JsonValue::from(current_id);
        match self.list_stack.last() {
            Some(last_stack) => {
                self.current_object[String::from("id_") + &parent_table_name] =
                    json::JsonValue::from(last_stack.object_id);
            }
            None => {}
        };
        self.current_object_id += 1;

        // Pop object via swap
        let mut pop_object = Object::new();
        swap(&mut pop_object, &mut self.current_object);

        // Publishing object
        (self.consumer)(&table_name, pop_object);
    }
}


impl<'a> Handler for NestedObjectHandler<'a> {
    fn handle_null(&mut self, _ctx: &Context) -> Status {
        self.handle_json_value(json::JsonValue::Null);
        Status::Continue
    }

    fn handle_double(&mut self, _ctx: &Context, val: f64) -> Status {
        self.handle_json_value(json::JsonValue::from(val));

        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }

    fn handle_int(&mut self, _ctx: &Context, val: i64) -> Status {
        self.handle_json_value(json::JsonValue::from(val));

        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }

    fn handle_bool(&mut self, _ctx: &Context, val: bool) -> Status {
        self.handle_json_value(json::JsonValue::from(val));

        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }

    fn handle_string(&mut self, _ctx: &Context, val: &str) -> Status {
        let json_value_raw = json::parse(val);
        let json_value_parsed = json_value_raw.unwrap();
        let key_parsed = json_value_parsed.as_str().unwrap();
        self.handle_json_value(json::JsonValue::from(key_parsed));
        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }

    fn handle_start_map(&mut self, _ctx: &Context) -> Status {
        self.current_nested_key.push(String::new());
        Status::Continue
    }

    fn handle_end_map(&mut self, _ctx: &Context) -> Status {
        self.current_nested_key.pop();

        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }

    fn handle_map_key(&mut self, _ctx: &Context, key: &str) -> Status {
        let json_value_raw = json::parse(key);
        let json_value_parsed = json_value_raw.unwrap();
        let key_parsed = json_value_parsed.as_str().unwrap();
        let current_nested_key_size = self.current_nested_key.len();
        self.current_nested_key[current_nested_key_size - 1] = String::from(key_parsed);
        Status::Continue
    }

    fn handle_start_array(&mut self, _ctx: &Context) -> Status {
        // Push values to list stack
        let current_path = self.current_list_path();
        self.array_ids.insert(current_path, self.current_object_id);

        let mut current_object: Object = Object::new();

        let mut current_object_id: i32 = 0;
        let mut current_nested_key: Vec<String> = Vec::new();

        swap(&mut current_object, &mut self.current_object);
        swap(&mut current_object_id, &mut self.current_object_id);
        swap(&mut current_nested_key, &mut self.current_nested_key);

        let new_obj = ListStackElement {
            object: current_object,
            object_id: current_object_id,
            nested_key: current_nested_key,
        };

        self.list_stack.push(new_obj);

        // Get last array id from memory
        let list_path = self.current_list_path();
        if self.array_ids.contains_key(&list_path) {
            self.current_object_id = self.array_ids[&list_path];
        }
        // self.array_ids.contains_key();

        // Reset current values

        Status::Continue
    }

    fn handle_end_array(&mut self, _ctx: &Context) -> Status {
        // Update global offset
        let current_path = self.current_list_path();
        self.array_ids.insert(current_path, self.current_object_id);

        // Pop list stack
        let vals = self.list_stack.pop().unwrap();

        self.current_object = vals.object; // Assign by move
        self.current_object_id = vals.object_id; // Assign by copy
        self.current_nested_key = vals.nested_key; // Assign by move

        if self.at_list_or_document_root() {
            self.publish_object()
        }
        Status::Continue
    }
}