//use crate::amphis_error::CrudError;
use crate::fptree::fptree::FPTree;
use log::trace;
use std::cell::RefCell;
use std::rc::Rc;

use crate::config::Config;

pub struct KVS {
    fptree: Rc<RefCell<FPTree>>,
}

impl KVS {
    pub fn new(config: &Config) -> Result<Self, std::io::Error> {
        Ok(KVS {
            fptree: Rc::new(RefCell::new(FPTree::new(&config)?)),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );
        self.fptree.borrow_mut().put(key, value)?;

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree.borrow().get(key)
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree.borrow_mut().delete(key)
    }
}
