use log::{debug, info, trace};
use std::path::Path;
use std::sync::{Arc, RwLock};

//use crate::amphis_error::CrudError;
use crate::config::Config;
use crate::flush_writer::FlushWriter;
use crate::fptree_manager::FPTreeManager;
use crate::sstable_manager::SstableManager;
use crate::util::file_util;

pub struct KVS {
    fptree_manager: Arc<FPTreeManager>,
    sstable_manager: Arc<SstableManager>,
    flush_writer: Arc<RwLock<FlushWriter>>,
}

impl KVS {
    pub fn new(name: &str, config: Config) -> Result<Self, std::io::Error> {
        let path = config.get_leaf_dir_path(name);
        let mut flush_writer = FlushWriter::new(name, config.clone(), 0);
        let (sstable_manager, next_table_id) = SstableManager::new(name, config.clone())?;
        if Path::new(&path).exists() {
            // flush the exsting trees
            flush_writer = FlushWriter::new(name, config.clone(), next_table_id);
            for entry in std::fs::read_dir(path)? {
                if let Some(fptree_id) = file_util::get_tree_id(&entry?.path()) {
                    debug!("found FPTree ID: {}", fptree_id);
                    let (table_id, filter, index) =
                        flush_writer.flush_with_file(name, fptree_id)?;
                    sstable_manager.register(table_id, filter, index)?;
                    let leaf_file = config.get_leaf_file_path(name, fptree_id);
                    std::fs::remove_file(leaf_file)?;
                }
            }
        }
        info!("Amphis has started: table {}", name);
        Ok(KVS {
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
