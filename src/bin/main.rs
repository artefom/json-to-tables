use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::exit;

use json::object::Object;
use structopt::StructOpt;

use json_to_tables;

#[derive(Debug, StructOpt)]
#[structopt(name = "Json-to-tables",
about = "Converts arbitrary jsons to line-saparated \
json tables with foreign keys")]
struct Cli {
    /// Output directory path
    output: std::path::PathBuf,

    /// Source .json files to convert to file tables structure
    #[structopt(parse(from_os_str))]
    files: Vec<std::path::PathBuf>,
}

fn path_to_str(p: &PathBuf) -> String {
    match p.clone().into_os_string().into_string() {
        Ok(s) => { s }
        Err(_) => { String::from("Unknown path") }
    }
}


fn main() {
    let opt = Cli::from_args();

    if opt.files.len() == 0 {
        println!("Must provide at least one file");
        exit(1)
    };

    let mut opened_files = HashMap::new();

    let mut consumer = |table_name: &String, object: Object|
        json_to_tables::consumer_files::consume_to_files(&mut opened_files,
                                                         &opt.output,
                                                         table_name,
                                                         object).unwrap();

    for path_buf in opt.files.iter() {
        let file = File::open(path_buf.clone().as_path()).unwrap();
        let mut reader = BufReader::new(file);

        let mut handler = json_to_tables::parser::NestedObjectHandler::new(
            String::from("root"), 0, &mut consumer);
        let mut parser = yajlish::Parser::new(&mut handler);

        match parser.parse(&mut reader) {
            Err(e) => {
                println!("Error parsing {}: {}", path_to_str(path_buf), e)
            }
            Ok(_) => { println!("Parsed {}", path_to_str(path_buf)) }
        };
    }
}

