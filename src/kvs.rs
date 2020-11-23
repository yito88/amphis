//use crate::amphis_error::CrudError;
use crate::fptree::fptree::FPTree;
use log::trace;
use std::sync::Arc;
use std::sync::RwLock;

use crate::config::Config;

pub struct KVS {
    fptree: Arc<RwLock<FPTree>>,
}

impl KVS {
    pub fn new(config: &Config) -> Result<Self, std::io::Error> {
        Ok(KVS {
            fptree: Arc::new(RwLock::new(FPTree::new(&config)?)),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );

        self.fptree.read().unwrap().put(key, value)?;
        //let locked_fptree = self.fptree.read().unwrap();
        //if let Some(split_key) = locked_fptree.put(key, value)? {
        //    drop(locked_fptree);
        //    self.fptree.write().unwrap().insert(&split_key);
        //};

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree.read().unwrap().get(key)
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree.read().unwrap().delete(key)
    }
}
