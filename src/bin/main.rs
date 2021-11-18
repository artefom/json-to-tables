use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::exit;

use glob::glob;
use structopt::StructOpt;

use json_to_tables::database::{Database, DatabaseCsv, DatabaseSchema};
use json_to_tables::read::read_to_db_many;

// use json_to_tables;
// use json_to_tables::parser::{TableLocation, TableRecord};
// use json_to_tables::yajlish::Parser;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Json-to-tables",
    about = "Converts arbitrary jsons to line-separated \
json tables with foreign keys"
)]
struct Cli {
    /// Output directory path
    output: std::path::PathBuf,

    /// Source .json files to convert to file tables structure
    files: Vec<String>,
}

fn _path_to_str(p: &PathBuf) -> String {
    match p.clone().into_os_string().into_string() {
        Ok(s) => s,
        Err(_) => String::from("Unknown path"),
    }
}

fn open_files(files: Vec<String>) -> Vec<(PathBuf, BufReader<File>)> {
    let mut all_files = Vec::<(PathBuf, BufReader<File>)>::new();

    for pattern in files.iter() {
        for entry in glob(pattern).expect("Failed to read glob pattern") {
            match entry {
                Ok(path_buf) => {
                    let file = File::open(path_buf.clone().as_path()).unwrap();
                    all_files.push((path_buf, BufReader::new(file)));
                }
                Err(e) => {
                    println!("{:?}", e);
                    exit(1)
                }
            }
        }
    }

    all_files
}

fn main() {
    let opt = Cli::from_args();

    if opt.files.len() == 0 {
        println!("Must provide at least one file");
        exit(1)
    };
    let db_schema = DatabaseSchema::empty();

    let mut db = DatabaseCsv::new(db_schema, opt.output);
    let all_files = open_files(opt.files);

    fn callback_success(path: PathBuf, num_records: usize) {
        println!(
            "Parsed {} - {} records",
            path.to_string_lossy(),
            num_records
        );
    }

    // Write data
    read_to_db_many(&mut db, all_files, &mut callback_success);

    db.close();
}
