use crate::amphis_error::CrudError;
use crate::fptree::fptree::FPTree;
use std::cell::RefCell;
use std::rc::Rc;

pub struct KVS {
    fptree: Rc<RefCell<FPTree>>,
}

impl KVS {
    pub fn new() -> Self {
        KVS {
            fptree: Rc::new(RefCell::new(FPTree::new())),
        }
    }

    pub fn insert(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), CrudError> {
        println!(
            "Inserting K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );
        self.fptree.borrow_mut().insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, CrudError> {
        println!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );
        match self.fptree.borrow().get(key) {
            Ok(v) => Ok(v),
            Err(_) => Err(CrudError::TimedOut),
        }
    }
}
