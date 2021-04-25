use log::warn;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;

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

pub fn get_table_id(file_name: &str) -> Option<usize> {
    if file_name.starts_with("sstable-") {
        if let Some(suffix) = file_name.strip_prefix("sstable-") {
            if let Some(id_str) = suffix.strip_suffix(".amph") {
                match id_str.parse::<usize>() {
                    Ok(id) => return Some(id),
                    Err(_) => return None,
                }
            }
        }
    }
    None
}

pub fn get_tree_id(file_name: &str) -> Option<usize> {
    if file_name.starts_with("leaves-") {
        if let Some(suffix) = file_name.strip_prefix("leaves-") {
            if let Some(id_str) = suffix.strip_suffix(".amph") {
                match id_str.parse::<usize>() {
                    Ok(id) => return Some(id),
                    Err(_) => return None,
                }
            }
        }
    }
    None
}
