use std::cell::RefCell;
use std::rc::Rc;

use super::inner::Inner;
use super::leaf::Leaf;
use super::node::Node;

pub struct FPTree {
    root: Rc<RefCell<dyn Node>>,
}

impl FPTree {
    pub fn new() -> Self {
        FPTree {
            root: Rc::new(RefCell::new(Leaf::new())),
        }
    }

    // TODO: error handling
    pub fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>) {
        let opt_split_key = self.root.borrow_mut().insert(key, value);

        if let Some(split_key) = opt_split_key {
            //println!("root split!");
            let new_child = self.root.borrow().get_next().unwrap();

            let mut new_root = Inner::new();
            new_root.add_key(&split_key);
            new_root.add_child(&Rc::clone(&self.root));
            new_root.add_child(&Rc::clone(&new_child));
            self.root = Rc::new(RefCell::new(new_root));
        }
    }

    // TODO: error handling
    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.root.borrow().get(key)
    }
}
