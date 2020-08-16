use log::trace;
use std::cell::RefCell;
use std::rc::Rc;

use super::node::Node;

// TODO: parameterize them
const FANOUT: usize = 3;

pub struct Inner {
    keys: Vec<Vec<u8>>,
    children: Vec<Rc<RefCell<dyn Node>>>,
    next: Option<Rc<RefCell<dyn Node>>>,
}

impl std::fmt::Display for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "keys: {:?}", self.keys)
    }
}

impl Node for Inner {
    fn is_inner(&self) -> bool {
        true
    }

    fn is_leaf(&self) -> bool {
        false
    }

    fn need_split(&self) -> bool {
        self.keys.len() > FANOUT
    }

    fn get_next(&self) -> Option<Rc<RefCell<dyn Node>>> {
        match &self.next {
            Some(rc) => Some(Rc::clone(&rc)),
            None => None,
        }
    }

    fn get_child(&self, key: &Vec<u8>) -> Option<Rc<RefCell<dyn Node>>> {
        trace!("check an inner - {}", self);
        let child_idx = match self.keys.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        if child_idx == self.children.len() {
            self.get_next()
        } else {
            Some(Rc::clone(&self.children[child_idx]))
        }
    }

    //TODO: error handling
    fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Option<Vec<u8>> {
        let mut ret: Option<Vec<u8>> = None;
        let child = self.get_child(key).unwrap();
        let opt_split_key = child.borrow_mut().insert(key, value);

        if let Some(split_key) = opt_split_key {
            let new_child = child.borrow().get_next().unwrap();

            match self.keys.binary_search(&split_key) {
                Ok(_) => panic!("should not reach here"),
                Err(i) => {
                    self.keys.insert(i, split_key.clone());
                    if i + 1 >= self.children.len() {
                        self.children.push(Rc::clone(&new_child));
                    } else {
                        self.children.insert(i + 1, Rc::clone(&new_child));
                    }
                }
            }

            if self.need_split() {
                ret = Some(self.split());
            }
        }

        ret
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        match self.get_child(key) {
            Some(c) => c.borrow().get(key),
            None => Ok(None),
        }
    }

    fn split(&mut self) -> Vec<u8> {
        let new_keys = self.keys.split_off((FANOUT + 1) / 2);
        let split_key = new_keys.first().unwrap().clone();
        let new_children = self.children.split_off((FANOUT + 1) / 2 + 1);

        let mut new_inner = Inner::new();
        for i in 1..new_keys.len() {
            new_inner.add_key(&new_keys[i]);
        }
        for i in 0..new_children.len() {
            new_inner.add_child(&new_children[i]);
        }
        if let Some(next) = self.get_next() {
            new_inner.next = Some(Rc::clone(&next));
        }
        trace!("split existing inner: {}", self);
        trace!("new inner: {}", new_inner);
        trace!("split_key: {:?}", split_key.clone());
        self.next = Some(Rc::new(RefCell::new(new_inner)));

        split_key
    }
}

impl Inner {
    pub fn new() -> Self {
        Inner {
            keys: Vec::with_capacity(FANOUT),
            next: None,
            children: Vec::with_capacity(FANOUT),
        }
    }

    pub fn add_key(&mut self, key: &Vec<u8>) {
        self.keys.push(key.clone());
    }

    pub fn add_child(&mut self, child: &Rc<RefCell<dyn Node>>) {
        self.children.push(Rc::clone(child));
    }
}

#[cfg(test)]
mod tests {
    use crate::fptree::inner::Inner;
    use crate::fptree::leaf::Leaf;
    use crate::fptree::node::Node;
    use std::cell::RefCell;
    use std::rc::Rc;
    const FANOUT: usize = 3;

    #[test]
    fn test_need_split() {
        let mut inner = Inner::new();
        assert_eq!(inner.need_split(), false);

        for i in 0..(FANOUT + 1) {
            inner.add_key(&vec![i as u8]);
        }
        assert_eq!(inner.need_split(), true);
    }

    #[test]
    fn test_get_next() {
        let mut inner = Inner::new();
        let not_exists = match inner.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);

        let new_inner: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Inner::new()));
        inner.next = Some(Rc::clone(&new_inner));

        let next = inner.get_next().unwrap();
        assert!(Rc::ptr_eq(&next, &new_inner));
    }

    #[test]
    fn test_get_child() {
        let mut inner = Inner::new();
        inner.add_key(&vec![10 as u8]);

        let mut new_child1 = Inner::new();
        new_child1.add_key(&vec![1 as u8]);
        let rc_new_child1: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(new_child1));
        inner.add_child(&rc_new_child1);

        let mut new_child2 = Inner::new();
        new_child2.add_key(&vec![11 as u8]);
        let rc_new_child2: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(new_child2));
        inner.add_child(&rc_new_child2);

        let child1 = inner.get_child(&vec![0u8]).unwrap();
        assert!(Rc::ptr_eq(&child1, &rc_new_child1));
        let child2 = inner.get_child(&vec![11u8]).unwrap();
        assert!(Rc::ptr_eq(&child2, &rc_new_child2));
    }

    #[test]
    fn test_insert() {
        let mut inner = Inner::new();
        let child1: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child2: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let k1 = "key1".as_bytes().to_vec();
        let k2 = "key2".as_bytes().to_vec();
        inner.keys = vec![k1, k2];
        inner.children = vec![Rc::clone(&child1), Rc::clone(&child2)];

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();
        inner.insert(&k, &v);

        let not_exists = match inner.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);
        assert_eq!(inner.keys.len(), 2);
        assert_eq!(child1.borrow().get(&k).unwrap().unwrap(), v);
        assert_eq!(child2.borrow().get(&k).unwrap(), None);
    }

    #[test]
    fn test_get() {
        let mut inner = Inner::new();
        let child1: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child2: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let k1 = "key1".as_bytes().to_vec();
        let k2 = "key2".as_bytes().to_vec();
        inner.keys = vec![k1, k2];
        inner.children = vec![Rc::clone(&child1), Rc::clone(&child2)];

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();
        inner.insert(&k, &v);

        let result = inner.get(&k);

        assert_eq!(result.unwrap().unwrap(), v);
    }

    #[test]
    fn test_split() {
        let mut inner = Inner::new();
        let child1: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child2: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child3: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child4: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let child5: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        let k1 = "key1".as_bytes().to_vec();
        let k2 = "key2".as_bytes().to_vec();
        let k3 = "key3".as_bytes().to_vec();
        let k4 = "key4".as_bytes().to_vec();
        inner.keys = vec![k1.clone(), k2.clone(), k3.clone(), k4.clone()];
        inner.children = vec![
            Rc::clone(&child1),
            Rc::clone(&child2),
            Rc::clone(&child3),
            Rc::clone(&child4),
            Rc::clone(&child5),
        ];

        let split_key = inner.split();

        assert_eq!(split_key, k3);
        assert_eq!(inner.keys, vec!(k1.clone(), k2.clone()));
        if let Some(next) = inner.get_next() {
            assert!(Rc::ptr_eq(
                &next.borrow().get_child(&split_key).unwrap(),
                &child4
            ));
            assert!(Rc::ptr_eq(&next.borrow().get_child(&k4).unwrap(), &child5));
        } else {
            panic!("the next inner should exist");
        }
    }
}