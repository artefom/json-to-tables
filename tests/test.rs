extern crate json_to_tables;

use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

use json;
use json::JsonValue;
use json::object::Object;
use rstest::*;

use json_to_tables::{consumer_json, parser};

/// Convert input stream to tables in json format
pub fn to_json_tables<B: io::BufRead>(root_name: String, input: &mut B) -> JsonValue {
    let mut result = json::JsonValue::new_object();

    let mut consumer = |table_name: &String, object: Object|
        consumer_json::consume_to_json(&mut result, table_name, object);

    let mut handler = parser::NestedObjectHandler::new(root_name, 0, &mut consumer);
    let mut parser = yajlish::Parser::new(&mut handler);

    // Run parser
    parser.parse(input).unwrap();

    // Return resulting object
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
    file.write_all(obj.pretty(4).as_bytes()).unwrap();
}

fn compare_expected(expected: &String, actual_json: &JsonValue) {
    let expected_json = json::parse(expected).unwrap();

    assert_eq!(expected_json, *actual_json);
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

    let actual = to_json_tables(test_case.clone(),
                                &mut read_test_case(&test_case, false));

    if env!("REWRITE_EXPECTED") == "1" {
        write_actual(&test_case, &actual);
    } else {
        compare_expected(&expected, &actual)
    }
}