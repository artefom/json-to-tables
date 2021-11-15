extern crate json_to_tables;

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;

use rstest::*;
use serde::Serialize;
use serde_json::{Map, Value as JsonValue};

use json_to_tables::database::DatabaseJson;
use json_to_tables::read;

/// Convert input stream to tables in json format
pub fn read_to_json<B: BufRead>(root_name: String, input: B) -> JsonValue {
    let mut result: JsonValue = JsonValue::Object(Map::new());
    read::read_to_db(DatabaseJson::new(root_name, &mut result),
                     input);
    result
}


fn test_case_path(test_case: &String, expected: bool) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("resources");

    if expected {
        path.push(test_case.clone() + "-in-expected.json");
    } else {
        path.push(test_case.clone() + "-in.json");
    }

    path
}

fn read_test_case(test_case: &String, expected: bool) -> BufReader<File> {
    let path = test_case_path(test_case, expected);
    let file = File::open(path.as_path()).unwrap();
    BufReader::new(file)
}

fn write_actual(test_case: &String, obj: &JsonValue) {
    let path = test_case_path(test_case, true);
    let mut file = File::create(path.as_path()).unwrap();

    let buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, formatter);

    obj.serialize(&mut ser).unwrap();
    let obj_pretty = String::from_utf8(ser.into_inner()).unwrap();
    file.write_all(obj_pretty.as_bytes()).unwrap();
}

fn compare_expected(_expected: &String, _actual_json: &JsonValue) {
    todo!()
}

#[rstest]
#[case("bookstore")]
#[case("empty")]
#[case("empty-nested-list")]
#[case("empty_list")]
#[case("empty_object_in_list")]
#[case("integration")]
#[case("just-value")]
#[case("list-empty-dicts")]
#[case("mixed-types")]
#[case("pyramids")]
#[case("stations")]
#[case("xbus")]
fn test_json_to_tables(#[case] test_case: String) {
    println!("{}", test_case);

    let mut expected: String = String::new();
    let mut reader = read_test_case(&test_case, true);
    reader.read_to_string(&mut expected).unwrap();

    let actual = read_to_json(test_case.clone(),
                              &mut read_test_case(&test_case, false));

    if env!("REWRITE_EXPECTED") == "1" {
        write_actual(&test_case, &actual);
    } else {
        compare_expected(&expected, &actual)
    }
}
