use std::cell::RefCell;
use std::rc::Rc;

pub trait Node {
    fn get_next(&self) -> Option<Rc<RefCell<dyn Node>>>;
    fn get_child(&self, key: &Vec<u8>) -> Option<Rc<RefCell<dyn Node>>>;
    fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>)
        -> Result<Option<Vec<u8>>, std::io::Error>;
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error>;
    fn split(&mut self) -> Result<Vec<u8>, std::io::Error>;
}
