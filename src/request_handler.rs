use crate::amphis_error::CrudError;
use crate::fptree::fptree::FPTree;
use std::cell::RefCell;
use std::rc::Rc;

pub struct RequestHandler {
    mutation_handler: MutationHandler,
    select_handler: SelectHandler,
}

pub struct SelectHandler {
    fptree: Rc<RefCell<FPTree>>,
}

pub struct MutationHandler {
    fptree: Rc<RefCell<FPTree>>,
}

impl RequestHandler {
    pub fn new() -> Self {
        let fptree = Rc::new(RefCell::new(FPTree::new()));

        RequestHandler {
            mutation_handler: MutationHandler::new(Rc::clone(&fptree)),
            select_handler: SelectHandler::new(Rc::clone(&fptree)),
        }
    }

    pub fn insert(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), CrudError> {
        self.mutation_handler.insert(key, value)
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, CrudError> {
        self.select_handler.get(key)
    }
}

impl SelectHandler {
    pub fn new(fptree: Rc<RefCell<FPTree>>) -> Self {
        SelectHandler { fptree: fptree }
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

impl MutationHandler {
    pub fn new(fptree: Rc<RefCell<FPTree>>) -> Self {
        MutationHandler { fptree: fptree }
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
}
