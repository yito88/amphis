use log::warn;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;

pub fn open_file(file_path: &str) -> Result<File, std::io::Error> {
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
                f
            }
            _ => return Err(e),
        },
    };

    Ok(file)
}
