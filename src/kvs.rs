use log::{debug, trace};
use std::path::Path;
use std::sync::{Arc, RwLock};

//use crate::amphis_error::CrudError;
use crate::config::Config;
use crate::file_utility;
use crate::flush_writer::FlushWriter;
use crate::fptree_manager::FPTreeManager;
use crate::sstable::sstable_manager::SstableManager;

pub struct KVS {
    name: String,
    config: Config,
    fptree_manager: Arc<FPTreeManager>,
    sstable_manager: Arc<SstableManager>,
    flush_writer: Arc<RwLock<FlushWriter>>,
}

impl KVS {
    pub fn new(name: &str, config: Config) -> Result<Self, std::io::Error> {
        let path = config.get_data_dir_path(name);
        let mut flush_writer = FlushWriter::new(name, config.clone(), 0);
        let sstable_manager = SstableManager::new(name, config.clone());
        if Path::new(&path).exists() {
            // find the next table ID
            let mut next_table_id = 0;
            for entry in std::fs::read_dir(path.clone())? {
                if let Some(table_id) = file_utility::get_table_id(&entry?.path()) {
                    if next_table_id <= table_id {
                        next_table_id = (table_id / 2 + 1) * 2;
                    }
                }
            }
            debug!("next table ID: {}", next_table_id);
            // flush the exsting trees
            flush_writer = FlushWriter::new(name, config.clone(), next_table_id);
            for entry in std::fs::read_dir(path)? {
                if let Some(fptree_id) = file_utility::get_tree_id(&entry?.path()) {
                    trace!("found FPTree ID: {}", fptree_id);
                    let (table_id, filter, index) =
                        flush_writer.flush_with_file(name, fptree_id)?;
                    sstable_manager.register(table_id, filter, index)?;
                    let leaf_file = config.get_leaf_file_path(name, fptree_id);
                    std::fs::remove_file(leaf_file)?;
                }
            }
        }
        Ok(KVS {
            name: name.to_string(),
            config: config.clone(),
            fptree_manager: Arc::new(FPTreeManager::new(name, config.clone())?),
            sstable_manager: Arc::new(sstable_manager),
            flush_writer: Arc::new(RwLock::new(flush_writer)),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );

        self.fptree_manager.put(key, value)?;

        // TODO: make a flush process async
        if self.fptree_manager.need_flush() {
            if let Some(first_leaf) = self.fptree_manager.prepare_flush()? {
                let (table_id, filter, index) =
                    self.flush_writer.write().unwrap().flush(first_leaf)?;
                self.sstable_manager.register(table_id, filter, index)?;
                self.fptree_manager.swith_fptree()?;
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        // TODO: concurrenct read
        let result = match self.fptree_manager.get(key)? {
            Some(r) => Some(r),
            None => self.sstable_manager.get(key)?,
        };

        match result {
            Some(v) => {
                if v.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(v))
                }
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree_manager.delete(key)
    }
}
