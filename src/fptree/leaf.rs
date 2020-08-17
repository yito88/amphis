use log::trace;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::rc::Rc;

use super::leaf_manager::LeafHeader;
use super::leaf_manager::LeafManager;
use super::leaf_manager::NUM_SLOT;
use super::node::Node;

pub struct Leaf {
    leaf_manager: Rc<RefCell<LeafManager>>,
    header: LeafHeader,
    id: usize,
    next: Option<Rc<RefCell<dyn Node>>>,
}

impl std::fmt::Display for Leaf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "id {}, header {}", self.id, self.header)
    }
}

impl Node for Leaf {
    fn get_next(&self) -> Option<Rc<RefCell<dyn Node>>> {
        match &self.next {
            Some(rc) => Some(Rc::clone(&rc)),
            None => None,
        }
    }

    fn get_child(&self, _key: &Vec<u8>) -> Option<Rc<RefCell<dyn Node>>> {
        None
    }

    // TODO: error handling
    fn insert(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut ret: Option<Vec<u8>> = None;
        let data_size = key.len() + value.len();
        if self.header.need_split(data_size) {
            let split_key = self.split()?;
            let new_leaf = self.get_next().unwrap();
            if split_key < *key {
                new_leaf.borrow_mut().insert(key, value)?;
            }
            ret = Some(split_key);
        }

        if let Some(slot) = self.header.get_empty_slot() {
            let tail_offset = self.leaf_manager.borrow_mut().write_data(
                self.id,
                self.header.get_tail_offset(),
                key,
                value,
            )?;

            self.header.set_slot(slot);
            self.header.set_tail_offset(tail_offset);
            self.header.set_fingerprint(slot, calc_key_hash(key));
            self.header
                .set_kv_info(slot, tail_offset, key.len(), value.len());
            // TODO: CRc
            self.leaf_manager
                .borrow_mut()
                .update_header(self.id, &self.header)?;
        }

        trace!("Leaf: {}", self);
        Ok(ret)
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!("Read from Leaf: {}", self);
        let hash = calc_key_hash(key);
        for (slot, fp) in self.header.get_fingerprints().iter().enumerate() {
            if self.header.is_slot_set(slot) && *fp == hash {
                let (data_offset, key_size, value_size) = self.header.get_kv_info(slot);
                let (actual_key, value) = self.leaf_manager.borrow().read_data(
                    self.id,
                    data_offset,
                    key_size,
                    value_size,
                )?;
                if actual_key == *key {
                    return Ok(Some(value.clone()));
                }
            }
        }
        Ok(None)
    }

    fn split(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut new_leaf = Leaf::new(Rc::clone(&self.leaf_manager))?;

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(NUM_SLOT);
        for slot in 0..NUM_SLOT {
            if self.header.is_slot_set(slot) {
                let (data_offset, key_size, _value_size) = self.header.get_kv_info(slot);
                let key = self
                    .leaf_manager
                    .borrow()
                    .read_key(self.id, data_offset, key_size)?;
                keys.push(key.clone());
            }
        }
        keys.sort();
        let new_first = keys.len() / 2;
        let split_key = keys[new_first].clone();

        for k in keys.split_off(new_first) {
            let hash = calc_key_hash(&k);
            let mut removed_slot: Option<usize> = None;
            for (slot, fp) in self.header.get_fingerprints().iter().enumerate() {
                if *fp == hash {
                    let (data_offset, key_size, value_size) = self.header.get_kv_info(slot);
                    let (actual_key, value) = self.leaf_manager.borrow().read_data(
                        self.id,
                        data_offset,
                        key_size,
                        value_size,
                    )?;
                    if actual_key == k {
                        removed_slot = Some(slot);
                        new_leaf.insert(&k, &value.clone())?;
                        break;
                    }
                }
            }
            if let Some(slot) = removed_slot {
                self.header.unset_slot(slot);
            }
        }
        trace!("split existing leaf: {}", self);
        trace!("new leaf: {}", new_leaf);
        trace!("split_key: {:?}", split_key.clone());
        self.header.set_next(new_leaf.id);
        self.next = Some(Rc::new(RefCell::new(new_leaf)));

        Ok(split_key)
    }
}

impl Leaf {
    pub fn new(leaf_manager: Rc<RefCell<LeafManager>>) -> Result<Self, std::io::Error> {
        let id = leaf_manager.borrow_mut().get_free_leaf()?;

        Ok(Leaf {
            leaf_manager: leaf_manager,
            header: LeafHeader::new(),
            id: id,
            next: None,
        })
    }
}

fn calc_key_hash(key: &Vec<u8>) -> u8 {
    let mut hasher = DefaultHasher::new();
    for b in key {
        hasher.write_u8(*b);
    }

    hasher.finish() as u8
}

/*
#[cfg(test)]
mod tests {
    use crate::fptree::leaf::Leaf;
    use crate::fptree::node::Node;
    use std::cell::RefCell;
    use std::rc::Rc;
    const NUM_SLOT: usize = 32;

    #[test]
    fn test_need_split() {
        let mut leaf = Leaf::new();
        assert_eq!(leaf.need_split(), false);

        for i in 0..NUM_SLOT {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v);
        }
        assert_eq!(leaf.need_split(), true);
    }

    #[test]
    fn test_get_next() {
        let mut leaf = Leaf::new();
        let not_exists = match leaf.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);

        let new_leaf: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(Leaf::new()));
        leaf.next = Some(Rc::clone(&new_leaf));

        let next = leaf.get_next().unwrap();

        assert!(Rc::ptr_eq(&next, &new_leaf));
    }

    #[test]
    fn test_insert_first() {
        let mut leaf = Leaf::new();
        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();

        leaf.insert(&k, &v);

        assert_eq!(leaf.bitmap[0], 1);
        assert_eq!(leaf.fingerprints[0], 192);
        assert_eq!(leaf.data[0], (k, v));
    }

    #[test]
    fn test_insert_any_slot() {
        let mut leaf = Leaf::new();
        for i in 0..16 {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v);
        }

        leaf.bitmap[1] = 0xFF ^ (1 << 5);
        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();

        leaf.insert(&k, &v);

        assert_eq!(leaf.bitmap[1], 0xFF);
        assert_eq!(leaf.fingerprints[13], 192);
        assert_eq!(leaf.data[13], (k, v));
    }

    #[test]
    fn test_get() {
        let mut leaf = Leaf::new();
        for i in 0..NUM_SLOT {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v);
        }

        for i in 0..NUM_SLOT {
            let k = vec![i as u8];
            let v = vec![i as u8];
            assert_eq!(leaf.get(&k).unwrap().unwrap(), v);
        }

        let k = vec![100 as u8];
        assert_eq!(leaf.get(&k).unwrap(), None);
    }

    #[test]
    fn test_split() {
        let mut leaf = Leaf::new();
        for i in 0..NUM_SLOT {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v);
        }

        let split_key = leaf.split();

        assert_eq!(split_key, vec!((NUM_SLOT / 2) as u8));
        assert_eq!(leaf.bitmap, vec!(0xFF, 0xFF, 0, 0));
        let exists = match leaf.get_next() {
            Some(_) => true,
            None => false,
        };
        assert!(exists);
    }
}
*/
