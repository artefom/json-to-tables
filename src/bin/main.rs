use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::exit;

use glob::glob;
use structopt::StructOpt;

// use json_to_tables;
// use json_to_tables::parser::{TableLocation, TableRecord};
// use json_to_tables::yajlish::Parser;

#[derive(Debug, StructOpt)]
#[structopt(name = "Json-to-tables",
about = "Converts arbitrary jsons to line-separated \
json tables with foreign keys")]
struct Cli {
    /// Root name of converted database
    _name: String,

    /// Output directory path
    _output: std::path::PathBuf,

    /// Source .json files to convert to file tables structure
    files: Vec<String>,
}

fn _path_to_str(p: &PathBuf) -> String {
    match p.clone().into_os_string().into_string() {
        Ok(s) => { s }
        Err(_) => { String::from("Unknown path") }
    }
}


fn main() {
    println!("Hello");
    let opt = Cli::from_args();

    if opt.files.len() == 0 {
        println!("Must provide at least one file");
        exit(1)
    };

    // let mut opened_files = HashMap::new();

    //
    // // Define consumer to be used in parsing
    // // In this case, we want to consume to file directory structure
    // let mut consumer = |loc: TableLocation, rec: TableRecord|
    //     json_to_tables::consumer_files::consume_to_files(&mut opened_files,
    //                                                      &opt.output,
    //                                                      loc,
    //                                                      rec).unwrap();
    //

    for pattern in opt.files.iter() {
        for entry in glob(pattern).expect("Failed to read glob pattern") {
            match entry {
                Ok(path_buf) => {
                    let file = File::open(path_buf.clone().as_path()).unwrap();
                    let mut _reader = BufReader::new(file);
                    //
                    //                 let mut handler = json_to_tables::parser::NestedObjectHandler::new(0, &mut consumer);
                    //                 let mut parser = Parser::new(&mut handler);
                    //
                    //                 match parser.parse(&mut reader) {
                    //                     Err(e) => {
                    //                         println!("Error parsing {}: {}", path_to_str(&path_buf), e)
                    //                     }
                    //                     Ok(_) => { println!("Parsed {}", path_to_str(&path_buf)) }
                    //                 };
                }
                Err(e) => {
                    println!("{:?}", e);
                    exit(1)
                }
            }
        }
    }
}

