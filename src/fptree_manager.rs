use log::info;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::fptree::{FPTree, Leaf};

pub struct FPTreeManager {
    name: String,
    config: Config,
    fptree_ptr: Arc<RwLock<Arc<RwLock<FPTree>>>>,
    new_fptree_ptr: Arc<RwLock<Option<Arc<RwLock<FPTree>>>>>,
    fptree_id: Arc<RwLock<usize>>,
    fptree_written: Arc<()>,
}

impl FPTreeManager {
    pub fn new(name: &str, config: Config) -> Result<Self, std::io::Error> {
        let fptree_id = 0;
        Ok(FPTreeManager {
            name: name.to_string(),
            config: config.clone(),
            fptree_ptr: Arc::new(RwLock::new(Arc::new(RwLock::new(FPTree::new(
                name, fptree_id, &config,
            )?)))),
            new_fptree_ptr: Arc::new(RwLock::new(None)),
            fptree_id: Arc::new(RwLock::new(fptree_id)),
            fptree_written: Arc::new(()),
        })
    }

    pub fn need_flush(&self) -> bool {
        // Flush has been already started when the new FPTree exists
        self.new_fptree_ptr.read().unwrap().is_none()
            && self
                .fptree_ptr
                .read()
                .unwrap()
                .read()
                .unwrap()
                .get_root_split_count()
                >= self.config.get_root_split_threshold()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        let locked_new = self.new_fptree_ptr.read().unwrap();
        match &*locked_new {
            Some(n) => n.read().unwrap().put(key, value),
            None => {
                let _written = self.fptree_written.clone();
                self.fptree_ptr
                    .read()
                    .unwrap()
                    .read()
                    .unwrap()
                    .put(key, value)
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut result = None;
        // TODO: concurrenct read
        let locked_new = self.new_fptree_ptr.read().unwrap();
        if let Some(n) = &*locked_new {
            result = n.read().unwrap().get(key)?;
        }

        if result.is_none() {
            result = self.fptree_ptr.read().unwrap().read().unwrap().get(key)?;
        }

        Ok(result)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), std::io::Error> {
        let locked_new = self.new_fptree_ptr.read().unwrap();
        match &*locked_new {
            Some(n) => n.read().unwrap().delete(key),
            None => {
                let _written = self.fptree_written.clone();
                self.fptree_ptr.read().unwrap().read().unwrap().delete(key)
            }
        }
    }

    /// Check the triggered flush before starting flush and set the new FPTree
    pub fn prepare_flush(&self) -> Result<Option<Arc<RwLock<Leaf>>>, std::io::Error> {
        let locked_fptree_id = self.fptree_id.write().unwrap();

        // re-check since another thread might have already flushed
        if !self.need_flush() {
            return Ok(None);
        }

        let mut locked_new = self.new_fptree_ptr.write().unwrap();
        if (*locked_new).is_some() {
            // The flush is in progress
            return Ok(None);
        }

        *locked_new = Some(Arc::new(RwLock::new(FPTree::new(
            &self.name,
            *locked_fptree_id + 1,
            &self.config,
        )?)));

        // check if other threads write data to the current FPTree
        if Arc::strong_count(&self.fptree_written) != 1 {
            return Ok(None);
        }

        let first_leaf = self
            .fptree_ptr
            .read()
            .unwrap()
            .read()
            .unwrap()
            .get_first_leaf();
        Ok(Some(first_leaf))
    }

    pub fn switch_fptree(&self) -> Result<(), std::io::Error> {
        let mut locked_fptree_id = self.fptree_id.write().unwrap();
        let mut locked_new = self.new_fptree_ptr.write().unwrap();
        match &*locked_new {
            Some(n) => {
                *self.fptree_ptr.write().unwrap() = n.clone();
                let deleted_id = *locked_fptree_id;
                *locked_fptree_id += 1;
                *locked_new = None;

                let leaf_file = self.config.get_leaf_file_path(&self.name, deleted_id);
                std::fs::remove_file(leaf_file)?;
            }
            None => unreachable!("No new FPTree when flushing"),
        }

        info!("Completed flushing FPTree {}", *locked_fptree_id - 1);

        Ok(())
    }
}
