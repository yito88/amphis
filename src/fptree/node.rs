use std::cell::RefCell;
use std::rc::Rc;

pub trait Node {
    fn is_inner(&self) -> bool;
    fn is_leaf(&self) -> bool;
    fn need_split(&self) -> bool;
    fn get_next(&self) -> Option<Rc<RefCell<dyn Node>>>;
    fn get_child(&self, key: &Vec<u8>) -> Option<Rc<RefCell<dyn Node>>>;
    fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Option<Vec<u8>>;
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error>;
    fn split(&mut self) -> Vec<u8>;
}
