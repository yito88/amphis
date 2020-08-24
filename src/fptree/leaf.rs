use log::trace;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::rc::Rc;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::fptree::leaf_manager::MockLeafManager as LeafManager;
    } else {
        use crate::fptree::leaf_manager::LeafManager;
    }
}
use super::leaf_manager::LeafHeader;
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

    fn insert(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut ret: Option<Vec<u8>> = None;

        self.invalidate_data(key, false)?;

        let data_size = key.len() + value.len();
        if self.header.need_split(data_size) {
            let split_key = self.split()?;
            let new_leaf = self.get_next().unwrap();
            if split_key < *key {
                // TODO: when the new leaf is split
                new_leaf.borrow_mut().insert(key, value)?;
                return Ok(Some(split_key));
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
            self.header.set_fingerprint(slot, self.calc_key_hash(key));
            self.header
                .set_kv_info(slot, tail_offset, key.len(), value.len());
            self.leaf_manager
                .borrow_mut()
                .commit_header(self.id, &self.header)?;
        }

        trace!("Leaf: {}", self);
        Ok(ret)
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!("Read from Leaf: {}", self);
        for slot in self.get_existing_slots(key) {
            let (data_offset, key_size, value_size) = self.header.get_kv_info(slot);
            let (actual_key, value) =
                self.leaf_manager
                    .borrow()
                    .read_data(self.id, data_offset, key_size, value_size)?;
            if actual_key == *key {
                return Ok(Some(value.clone()));
            }
        }

        Ok(None)
    }

    fn delete(&mut self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        self.invalidate_data(key, true)
    }

    fn split(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut new_leaf = Leaf::new(Rc::clone(&self.leaf_manager))?;

        let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>, usize)> = Vec::with_capacity(NUM_SLOT);
        for slot in 0..NUM_SLOT {
            if self.header.is_slot_set(slot) {
                let (data_offset, key_size, value_size) = self.header.get_kv_info(slot);
                let (key, value) = self.leaf_manager.borrow().read_data(
                    self.id,
                    data_offset,
                    key_size,
                    value_size,
                )?;
                kv_pairs.push((key.clone(), value.clone(), slot));
            }
        }
        kv_pairs.sort_by_key(|p| p.0.clone());
        let new_first = kv_pairs.len() / 2;
        let split_key = kv_pairs[new_first].0.clone();

        for (k, v, slot) in kv_pairs.split_off(new_first) {
            new_leaf.insert(&k, &v)?;
            self.header.unset_slot(slot);
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
        let (id, header) = leaf_manager.borrow_mut().get_free_leaf()?;

        Ok(Leaf {
            leaf_manager,
            header,
            id,
            next: None,
        })
    }

    fn calc_key_hash(&self, key: &Vec<u8>) -> u8 {
        let mut hasher = DefaultHasher::new();
        for b in key {
            hasher.write_u8(*b);
        }

        hasher.finish() as u8
    }

    fn get_existing_slots(&self, key: &Vec<u8>) -> Vec<usize> {
        let mut slots = Vec::new();
        let hash = self.calc_key_hash(key);
        for (slot, fp) in self.header.get_fingerprints().iter().enumerate() {
            if self.header.is_slot_set(slot) && *fp == hash {
                slots.push(slot);
            }
        }

        slots
    }

    fn invalidate_data(&mut self, key: &Vec<u8>, is_committed: bool) -> Result<(), std::io::Error> {
        for slot in self.get_existing_slots(key) {
            let (data_offset, key_size, value_size) = self.header.get_kv_info(slot);
            let (actual_key, _value) =
                self.leaf_manager
                    .borrow()
                    .read_data(self.id, data_offset, key_size, value_size)?;
            if actual_key == *key {
                self.header.unset_slot(slot);
                if is_committed {
                    self.leaf_manager
                        .borrow_mut()
                        .commit_header(self.id, &self.header)?;
                }
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const LEAF_SIZE: usize = 256 * 1024;

    fn make_new_leaf(id: usize) -> Leaf {
        let mut mock_leaf_manager = LeafManager::default();
        mock_leaf_manager
            .expect_get_free_leaf()
            .returning(move || Ok((id, LeafHeader::new())));
        mock_leaf_manager
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(offset - 1));
        mock_leaf_manager
            .expect_read_data()
            .returning(|_, offset, _, _| {
                let kv = vec![(LEAF_SIZE - offset - 1) as u8];
                Ok((kv.clone(), kv.clone()))
            });
        mock_leaf_manager
            .expect_commit_header()
            .returning(move |_, _| Ok(()));

        Leaf::new(Rc::new(RefCell::new(mock_leaf_manager))).unwrap()
    }

    #[test]
    fn test_get_next() {
        let mut leaf = make_new_leaf(0);
        let not_exists = match leaf.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);

        let new_leaf: Rc<RefCell<dyn Node>> = Rc::new(RefCell::new(make_new_leaf(1)));
        leaf.next = Some(Rc::clone(&new_leaf));

        let next = leaf.get_next().unwrap();

        assert!(Rc::ptr_eq(&next, &new_leaf));
    }

    #[test]
    fn test_insert_first() {
        let mut leaf = make_new_leaf(0);
        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();

        leaf.insert(&k, &v).unwrap();

        let expected_offset = LEAF_SIZE - 1;
        assert!(leaf.header.is_slot_set(0));
        assert_eq!(leaf.header.get_fingerprints()[0], 192);
        assert_eq!(leaf.header.get_tail_offset(), expected_offset);
        assert_eq!(
            leaf.header.get_kv_info(0),
            (expected_offset, k.len(), v.len())
        );
    }

    #[test]
    fn test_insert_any_slot() {
        let mut leaf = make_new_leaf(0);
        let any_slot = NUM_SLOT / 2;
        for i in 0..any_slot {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        assert!((0..any_slot).all(|i| { leaf.header.is_slot_set(i) }));
        assert!(!leaf.header.is_slot_set(any_slot));

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();

        leaf.insert(&k, &v).unwrap();

        let expected_offset = LEAF_SIZE - any_slot - 1;
        assert!(leaf.header.is_slot_set(any_slot));
        assert_eq!(leaf.header.get_fingerprints()[any_slot], 192);
        assert_eq!(leaf.header.get_tail_offset(), expected_offset);
        assert_eq!(
            leaf.header.get_kv_info(any_slot),
            (expected_offset, k.len(), v.len())
        );
    }

    #[test]
    fn test_get() {
        let mut leaf = make_new_leaf(0);
        for i in 0..5 {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        let k = vec![3u8];
        let v = vec![3u8];
        assert_eq!(leaf.get(&k).unwrap().unwrap(), v);

        let k = vec![8 as u8];
        assert_eq!(leaf.get(&k).unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let mut leaf = make_new_leaf(0);
        for i in 0..5 {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        let k = vec![3u8];
        leaf.delete(&k).unwrap();

        assert!(!leaf.header.is_slot_set(3));
        assert_eq!(leaf.header.get_kv_info(3), (LEAF_SIZE - 4, 1, 1));
    }

    #[test]
    fn test_update() {
        let mut leaf = make_new_leaf(0);
        for i in 0..5 {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        let k = vec![3u8];
        let v2 = vec![5u8];
        leaf.insert(&k, &v2).unwrap();

        let expected_offset = LEAF_SIZE - 6;
        assert!(leaf.header.is_slot_set(3));
        assert_eq!(leaf.header.get_tail_offset(), expected_offset);
        assert_eq!(
            leaf.header.get_kv_info(3),
            (expected_offset, k.len(), v2.len())
        );
    }

    #[test]
    fn test_split() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .borrow_mut()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(offset - 1));

        for i in 0..NUM_SLOT {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        let split_key = leaf.split().unwrap();

        assert_eq!(split_key, vec!((NUM_SLOT / 2) as u8));
        assert!((0..(NUM_SLOT / 2)).all(|i| { leaf.header.is_slot_set(i) }));
        assert!(((NUM_SLOT / 2)..NUM_SLOT).all(|i| { !leaf.header.is_slot_set(i) }));
        let exists = match leaf.get_next() {
            Some(_) => true,
            None => false,
        };
        assert!(exists);
    }
}
