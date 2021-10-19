use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Error, Write};
use std::path::{PathBuf};
use std::string::String;

use json;
use json::object::Object;

fn get_table_file<'a>(opened_files: &'a mut HashMap<String, BufWriter<File>>,
                      root_dir: &PathBuf,
                      table_name: &String) -> Result<&'a mut BufWriter<File>, Error> {
    if !opened_files.contains_key(table_name) {
        let mut path = root_dir.clone();
        path.push(table_name);
        path.set_extension("json");

        let file = File::create(path.as_path())?;
        let writer = BufWriter::new(file);
        opened_files.insert(table_name.clone(), writer);
    }

    return Ok(opened_files.get_mut(table_name).unwrap());
}

/// Writes flat objects to new-line separated json files into directory structure
pub fn consume_to_files(opened_files: &mut HashMap<String, BufWriter<File>>,
                        root_dir: &PathBuf, table_name: &String, object: Object) -> Result<(), Error> {
    let writer = get_table_file(opened_files, root_dir, table_name)?;

    writer.write(object.dump().as_bytes())?;
    writer.write("\n".as_bytes())?;

    Ok(())
}
