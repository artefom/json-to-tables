use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::exit;

use glob::glob;
use structopt::StructOpt;

use json_to_tables::database::DatabaseCsv;
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
    /// Root name of converted database
    name: String,

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

fn open_files(files: Vec<String>) -> Vec<BufReader<File>> {
    let mut all_files = Vec::<BufReader<File>>::new();

    for pattern in files.iter() {
        for entry in glob(pattern).expect("Failed to read glob pattern") {
            match entry {
                Ok(path_buf) => {
                    let file = File::open(path_buf.clone().as_path()).unwrap();
                    all_files.push(BufReader::new(file));
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
    let db = DatabaseCsv::new(opt.name, opt.output);
    let all_files = open_files(opt.files);

    fn callback_success() {
        println!("Parsed some file");
    }

    read_to_db_many(db, all_files, &mut callback_success);
}
