use log::info;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;
use std::str::FromStr;

pub fn open_file(file_path: &str) -> Result<(File, bool), std::io::Error> {
    let mut is_created = false;
    let file = match OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(file_path)
    {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                let f = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(file_path)?;
                f.sync_all()?;
                info!("New file {} is created", file_path);
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
    let file = path
        .file_stem()
        .expect("cannot get the file name")
        .to_str()?;
    file.strip_prefix(prefix)
        .and_then(|id| usize::from_str(id).ok())
}
