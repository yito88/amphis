use log::warn;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;

pub fn open_file(file_path: &str) -> Result<(File, bool), std::io::Error> {
    let mut is_created = false;
    let file = match OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&file_path)
    {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                warn!("a new index file is created");
                let f = File::create(&file_path)?;
                f.sync_all()?;
                is_created = true;
                f
            }
            _ => return Err(e),
        },
    };

    Ok((file, is_created))
}

pub fn get_table_id(path: &Path) -> Option<usize> {
    get_id(path, "sstable-")
}

pub fn get_tree_id(path: &Path) -> Option<usize> {
    get_id(path, "leaves-")
}

fn get_id(path: &Path, prefix: &str) -> Option<usize> {
    match path.file_stem().expect("cannot get the file name").to_str() {
        Some(file) if file.starts_with(prefix) => match file.strip_prefix(prefix) {
            Some(id_str) => match id_str.parse::<usize>() {
                Ok(id) => return Some(id),
                Err(_) => return None,
            },
            None => None,
        },
        _ => None,
    }
}
