use std::collections::BTreeMap;

// TODO: parameterize
const LEAST_OFFSET: usize = 1 << 18;

pub struct SparseIndex {
    prev_offset: usize,
    index: BTreeMap<Vec<u8>, usize>,
}

impl SparseIndex {
    pub fn new() -> Self {
        SparseIndex {
            prev_offset: usize::MAX,
            index: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &Vec<u8>, offset: usize) {
        if self.prev_offset == usize::MAX || offset - self.prev_offset >= LEAST_OFFSET {
            self.prev_offset = offset;
            self.index.insert(key.clone(), offset);
        }
    }

    // the offset should be always returned since the minimum key is inserted
    pub fn get(&self, key: &Vec<u8>) -> usize {
        match self.index.get(key) {
            Some(offset) => *offset,
            None => *self.index.range(..key.clone()).last().unwrap().1,
        }
    }
}
