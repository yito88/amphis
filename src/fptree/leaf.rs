use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::{Arc, RwLock};

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::fptree::leaf_manager::MockLeafManager as LeafManager;
    } else {
        use crate::fptree::leaf_manager::LeafManager;
    }
}
use super::leaf_manager::{LeafHeader, INITIAL_TAIL_OFFSET, NUM_SLOT};
use super::node::Node;

pub struct Leaf {
    leaf_manager: Arc<RwLock<LeafManager>>,
    header: LeafHeader,
    id: usize,
    page_id: usize,
    next: Option<Arc<RwLock<Leaf>>>,
    is_root: bool,
}

impl Node for Leaf {
    fn is_root(&self) -> bool {
        self.is_root
    }

    fn set_root(&mut self, is_root: bool) {
        self.is_root = is_root;
    }

    fn is_leaf(&self) -> bool {
        true
    }

    fn get_next(&self) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
        match &self.next {
            Some(arc) => Some(arc.clone()),
            None => None,
        }
    }

    fn get_child(&self, _key: &Vec<u8>) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
        None
    }

    fn may_need_split(&self) -> bool {
        self.header.need_split()
    }

    fn insert(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut ret: Option<Vec<u8>> = None;

        self.invalidate_data(key)?;

        if self.header.need_split() {
            let split_key = self.split()?;
            let new_leaf = self.get_next().expect("no next leaf");
            if split_key < *key {
                self.commit()?;

                // TODO: when the new leaf is split
                new_leaf.write().unwrap().insert(key, value)?;
                return Ok(Some(split_key));
            } else {
                new_leaf.read().unwrap().commit()?;
            }

            ret = Some(split_key);
        }

        let slot = self.header.get_empty_slot().expect("no empty slot");
        loop {
            let offset = self.header.get_tail_offset();
            let tail_offset =
                self.leaf_manager
                    .read()
                    .unwrap()
                    .write_data(self.page_id, offset, key, value)?;
            match tail_offset {
                Some(tail_offset) => {
                    self.update_header_for_write(slot, tail_offset, key, value);
                    break;
                }
                None => {
                    // not enough space to write
                    self.append_new_page()?;
                }
            }
        }
        self.commit()?;

        trace!("Leaf: {}, key {:?}", self, key);
        Ok(ret)
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!("Read from Leaf: {}", self);
        for slot in self.get_existing_slots(key) {
            let (page_id, data_offset, key_size, value_size) = self.header.get_kv_info(slot);
            let (actual_key, value) = self.leaf_manager.read().unwrap().read_data(
                page_id,
                data_offset,
                key_size,
                value_size,
            )?;
            if actual_key == *key {
                return Ok(Some(value.clone()));
            }
        }

        Ok(None)
    }

    fn split(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut new_leaf = Leaf::new(self.leaf_manager.clone())?;

        let mut kv_pairs = self.get_kv_pairs()?;
        kv_pairs.sort();
        let new_first = kv_pairs.len() / 2;
        let split_key = kv_pairs[new_first].0.clone();

        for (k, v, slot) in kv_pairs.split_off(new_first) {
            new_leaf.insert(&k, &v)?;
            self.header.unset_slot(slot);
        }

        if let Some(n) = &self.next {
            new_leaf
                .header
                .set_next(self.header.get_next().expect("no next"));
            new_leaf.next = Some(n.clone());
        }

        trace!("new leaf: {}", new_leaf);

        self.header.set_next(new_leaf.id);
        self.next = Some(Arc::new(RwLock::new(new_leaf)));

        trace!("split existing leaf: {}", self);
        trace!("split_key: {:?}", split_key.clone());

        Ok(split_key)
    }

    fn commit(&self) -> Result<(), std::io::Error> {
        self.leaf_manager
            .read()
            .unwrap()
            .commit_header(self.id, &self.header)
    }
}

impl Leaf {
    pub fn new(leaf_manager: Arc<RwLock<LeafManager>>) -> Result<Self, std::io::Error> {
        let (id, header) = leaf_manager.write().unwrap().allocate_leaf()?;

        Ok(Leaf {
            leaf_manager,
            header,
            id,
            page_id: id,
            next: None,
            is_root: false,
        })
    }

    pub fn get_leaf_manager(&self) -> Arc<RwLock<LeafManager>> {
        self.leaf_manager.clone()
    }

    pub fn get_kv_pairs(&self) -> Result<Vec<(Vec<u8>, Vec<u8>, usize)>, std::io::Error> {
        let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>, usize)> = Vec::with_capacity(NUM_SLOT);

        for slot in 0..NUM_SLOT {
            if self.header.is_slot_set(slot) {
                let (page_id, data_offset, key_size, value_size) = self.header.get_kv_info(slot);

                let (key, value) = self.leaf_manager.read().unwrap().read_data(
                    page_id,
                    data_offset,
                    key_size,
                    value_size,
                )?;
                kv_pairs.push((key, value, slot));
            }
        }

        Ok(kv_pairs)
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

    fn invalidate_data(&mut self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        for slot in self.get_existing_slots(key) {
            let (page_id, data_offset, key_size, value_size) = self.header.get_kv_info(slot);
            let (actual_key, _value) = self.leaf_manager.read().unwrap().read_data(
                page_id,
                data_offset,
                key_size,
                value_size,
            )?;
            if actual_key == *key {
                self.header.unset_slot(slot);
                break;
            }
        }

        Ok(())
    }

    fn update_header_for_write(
        &mut self,
        slot: usize,
        tail_offset: usize,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) {
        let offset = self.header.get_tail_offset();
        self.header.set_slot(slot);
        self.header.set_fingerprint(slot, self.calc_key_hash(key));
        self.header
            .set_kv_info(slot, self.page_id, offset, key.len(), value.len());
        self.header.set_tail_offset(tail_offset);
    }

    fn append_new_page(&mut self) -> Result<(), std::io::Error> {
        let new_page_id = self
            .leaf_manager
            .write()
            .unwrap()
            .allocate_ext_page(self.id)?;
        self.page_id = new_page_id;
        self.header.set_tail_offset(INITIAL_TAIL_OFFSET);
        self.header.set_ext(new_page_id);

        trace!("append a new leaf page {}", new_page_id);
        Ok(())
    }
}

impl std::fmt::Display for Leaf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "id {}, header {}", self.id, self.header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const DATA_UNIT: usize = 4 * 1024;
    const LEAF_SIZE: usize = 1024 * 1024;

    fn make_new_leaf(id: usize) -> Leaf {
        let mut mock_leaf_manager = LeafManager::default();
        mock_leaf_manager
            .expect_allocate_leaf()
            .returning(move || Ok((id, LeafHeader::new())));
        mock_leaf_manager
            .expect_commit_header()
            .returning(move |_, _| Ok(()));

        Leaf::new(Arc::new(RwLock::new(mock_leaf_manager))).unwrap()
    }

    #[test]
    fn test_get_next() {
        let mut leaf = make_new_leaf(0);
        let not_exists = match leaf.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);

        let new_leaf: Arc<RwLock<Leaf>> = Arc::new(RwLock::new(make_new_leaf(1)));
        leaf.next = Some(new_leaf.clone());

        let exists = match leaf.get_next() {
            Some(_) => true,
            None => false,
        };
        assert!(exists);
    }

    #[test]
    fn test_insert_first() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(Some(offset + DATA_UNIT)));

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();

        leaf.insert(&k, &v).unwrap();

        let expected_tail_offset = DATA_UNIT * 2;
        assert!(leaf.header.is_slot_set(0));
        assert_eq!(leaf.header.get_fingerprints()[0], 192);
        assert_eq!(leaf.header.get_tail_offset(), expected_tail_offset);
        assert_eq!(
            leaf.header.get_kv_info(0),
            (leaf.id, expected_tail_offset - DATA_UNIT, k.len(), v.len())
        );
    }

    #[test]
    fn test_extended_page() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, k, v| {
                if (offset + k.len() + v.len()) <= LEAF_SIZE {
                    Ok(Some(offset + k.len() + v.len()))
                } else {
                    Ok(None)
                }
            });
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_allocate_ext_page()
            .returning(|id| Ok(id + 1));

        let k0 = vec![0; 256 * 1024];
        let v0 = vec![0; 256 * 1024];
        leaf.insert(&k0, &v0).unwrap();

        let k1 = vec![1; 256 * 1024];
        let v1 = vec![1; 256 * 1024];
        leaf.insert(&k1, &v1).unwrap();

        let expected_tail_offset = DATA_UNIT;
        assert_eq!(
            leaf.header.get_kv_info(1),
            (leaf.id + 1, expected_tail_offset, k1.len(), v1.len())
        );
        assert_eq!(leaf.header.get_ext().expect("no ext page"), leaf.id + 1);
    }

    #[test]
    fn test_insert_any_slot() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(Some(offset + DATA_UNIT)));

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

        let expected_tail_offset = DATA_UNIT + (any_slot + 1) * DATA_UNIT;
        assert!(leaf.header.is_slot_set(any_slot));
        assert_eq!(leaf.header.get_fingerprints()[any_slot], 192);
        assert_eq!(leaf.header.get_tail_offset(), expected_tail_offset);
        assert_eq!(
            leaf.header.get_kv_info(any_slot),
            (leaf.id, (any_slot + 1) * DATA_UNIT, k.len(), v.len())
        );
    }

    #[test]
    fn test_get() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(Some(offset + DATA_UNIT)));
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_read_data()
            .returning(|_, offset, _, _| {
                let kv = vec![(offset / DATA_UNIT - 1) as u8];
                Ok((kv.clone(), kv.clone()))
            });

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
    fn test_update() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(Some(offset + DATA_UNIT)));
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_read_data()
            .returning(|_, offset, _, _| {
                let kv = vec![(offset / DATA_UNIT - 1) as u8];
                Ok((kv.clone(), kv.clone()))
            });

        for i in 0..5 {
            let k = vec![i as u8];
            let v = vec![i as u8];
            leaf.insert(&k, &v).unwrap();
        }

        let k = vec![3u8];
        let v2 = vec![5u8];
        leaf.insert(&k, &v2).unwrap();

        let expected_tail_offset = DATA_UNIT + DATA_UNIT * 6;
        assert!(leaf.header.is_slot_set(3));
        assert_eq!(leaf.header.get_tail_offset(), expected_tail_offset);
        assert_eq!(
            leaf.header.get_kv_info(3),
            (leaf.id, expected_tail_offset - DATA_UNIT, k.len(), v2.len())
        );
    }

    #[test]
    fn test_split() {
        let mut leaf = make_new_leaf(0);
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_write_data()
            .returning(|_, offset, _, _| Ok(Some(offset + DATA_UNIT)));
        leaf.leaf_manager
            .write()
            .unwrap()
            .expect_read_data()
            .returning(|_, offset, _, _| {
                let kv = vec![(offset / DATA_UNIT - 1) as u8];
                Ok((kv.clone(), kv.clone()))
            });

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
