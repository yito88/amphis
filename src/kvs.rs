//use crate::amphis_error::CrudError;
use crate::flush_writer::FlushWriter;
use crate::fptree::fptree::FPTree;
use log::trace;
use std::sync::{Arc, RwLock};

use crate::config::Config;

pub struct KVS {
    name: String,
    config: Config,
    fptree: Arc<RwLock<FPTree>>,
    new_fptree: Option<Arc<RwLock<FPTree>>>,
    fptree_id: usize,
    fptree_written: Arc<()>,
    flush_writer: Arc<FlushWriter>,
}

impl KVS {
    pub fn new(name: &str, config: Config) -> Result<KVS, std::io::Error> {
        // TODO: recovery
        let fptree = Arc::new(RwLock::new(FPTree::new(name, 0, &config)?));
        Ok(KVS {
            name: name.to_string(),
            config: config.clone(),
            fptree: fptree.clone(),
            new_fptree: None,
            fptree_id: 0,
            fptree_written: Arc::new(()),
            flush_writer: Arc::new(FlushWriter::new(config.clone(), fptree.clone(), 0)),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );

        match &self.new_fptree {
            Some(new_fptree) => new_fptree.read().unwrap().put(key, value)?,
            None => {
                let _written = self.fptree_written.clone();
                self.fptree.read().unwrap().put(key, value)?
            }
        };

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        // TODO: concurrenct read
        if let Some(new_fptree) = &self.new_fptree {
            match new_fptree.read().unwrap().get(key)? {
                Some(val) => Ok(Some(val)),
                None => self.fptree.read().unwrap().get(key),
            }
        } else {
            self.fptree.read().unwrap().get(key)
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        if let Some(new_fptree) = &self.new_fptree {
            new_fptree.read().unwrap().delete(key)?;
        }
        let _written = self.fptree_written.clone();
        self.fptree.read().unwrap().delete(key)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        let new_id = self.fptree_id + 1;
        self.allocate_new_fptree(new_id)?;
        // check if other threads write data to FPTree
        if Arc::strong_count(&self.fptree_written) != 1 {
            return Ok(());
        }

        //std::fs::remove_file(config.get_leaf_file_path(TABLE_NAME, self.fptree_id)).unwrap()?;
        self.fptree_id = new_id;

        Ok(())
    }

    fn allocate_new_fptree(&mut self, new_id: usize) -> Result<(), std::io::Error> {
        self.new_fptree = Some(Arc::new(RwLock::new(FPTree::new(
            &self.name,
            new_id,
            &self.config,
        )?)));

        Ok(())
    }
}
