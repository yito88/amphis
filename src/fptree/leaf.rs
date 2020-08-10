use log::trace;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::rc::Rc;

use super::node::Node;

// TODO: parameterize them
const NUM_SLOT: usize = 32;

pub struct Leaf {
    bitmap: Vec<u8>,
    next: Option<Rc<RefCell<dyn Node>>>,
    fingerprints: Vec<u8>,
    data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Display for Leaf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bitmap: {:?}, fingerprints: {:?}, data {:?}",
            self.bitmap, self.fingerprints, self.data
        )
    }
}

impl Node for Leaf {
    fn is_inner(&self) -> bool {
        false
    }

    fn is_leaf(&self) -> bool {
        true
    }

    fn need_split(&self) -> bool {
        self.bitmap.iter().all(|&x| x == 0xFF)
    }

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
    fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Option<Vec<u8>> {
        let mut ret: Option<Vec<u8>> = None;
        if self.need_split() {
            let split_key = self.split();
            let new_leaf = self.get_next().unwrap();
            if split_key < *key {
                new_leaf.borrow_mut().insert(key, value);
            }
            ret = Some(split_key);
        }

        if let Some(slot) = self.get_empty_slot() {
            self.set_slot(slot);
            self.fingerprints[slot] = self.calc_key_hash(key);
            if slot < self.data.len() {
                self.data[slot] = (key.clone(), value.clone());
            } else {
                // append a new data
                self.data.insert(slot, (key.clone(), value.clone()));
            }
        }

        trace!("Leaf: {}", self);
        ret
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!("Read from Leaf: {}", self);
        let hash = self.calc_key_hash(key);
        for (i, fp) in self.fingerprints.iter().enumerate() {
            if self.is_slot_set(i) && *fp == hash {
                if self.data[i].0 == *key {
                    return Ok(Some(self.data[i].1.clone()));
                }
            }
        }

        Ok(None)
    }

    // TODO: error handling
    fn split(&mut self) -> Vec<u8> {
        let mut new_leaf = Leaf::new();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(NUM_SLOT);

        for kv in &self.data {
            keys.push(kv.0.clone());
        }
        keys.sort();
        let split_key = keys[NUM_SLOT / 2].clone();

        for k in keys.split_off(NUM_SLOT / 2) {
            let hash = self.calc_key_hash(&k);
            let mut removed_idx: Vec<usize> = Vec::new();
            for (i, fp) in self.fingerprints.iter().enumerate() {
                if *fp == hash {
                    if self.data[i].0 == k {
                        removed_idx.push(i);
                        new_leaf.insert(&k, &self.data[i].1);
                    }
                }
            }
            for idx in removed_idx {
                self.unset_slot(idx);
            }
        }
        trace!("split existing leaf: {}", self);
        trace!("new leaf: {}", new_leaf);
        trace!("split_key: {:?}", split_key.clone());
        self.next = Some(Rc::new(RefCell::new(new_leaf)));

        split_key
    }
}

impl Leaf {
    pub fn new() -> Self {
        Leaf {
            bitmap: vec![0; NUM_SLOT / 8],
            next: None,
            fingerprints: vec![0; NUM_SLOT],
            data: Vec::with_capacity(NUM_SLOT),
        }
    }

    fn calc_key_hash(&self, key: &Vec<u8>) -> u8 {
        let mut hasher = DefaultHasher::new();
        for b in key {
            hasher.write_u8(*b);
        }

        hasher.finish() as u8
    }

    fn get_empty_slot(&self) -> Option<usize> {
        for (i, slots) in self.bitmap.iter().enumerate() {
            if *slots == 0xFF {
                continue;
            }

            for offset in 0..8 {
                if slots & (1 << offset) == 0 {
                    return Some(i * 8 + offset);
                }
            }
        }
        None
    }

    fn is_slot_set(&self, slot: usize) -> bool {
        let idx = slot / 8;
        let offset = slot % 8;

        self.bitmap[idx] & (1 << offset) != 0
    }

    fn set_slot(&mut self, slot: usize) {
        let idx = slot / 8;
        let offset = slot % 8;
        self.bitmap[idx] |= 1 << offset;
    }

    fn unset_slot(&mut self, slot: usize) {
        let idx = slot / 8;
        let offset = slot % 8;
        self.bitmap[idx] &= 0xFF ^ (1 << offset);
    }
}

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
