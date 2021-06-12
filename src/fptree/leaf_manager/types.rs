use serde::{Deserialize, Serialize};

use crate::util::data_util;

pub const NUM_SLOT: usize = 32;
pub const NUM_ALLOCATION: usize = 16;
pub const LEAF_SIZE: usize = 1024 * 1024;

const INVALID_LEAF_ID: u32 = u32::MAX;
pub const INITIAL_TAIL_OFFSET: usize = data_util::DATA_ALIGNMENT;
pub const END_TAIL_OFFSET: usize = LEAF_SIZE - data_util::DATA_ALIGNMENT;

// for header format
pub(super) const HEADER_MAGIC: u32 = 0x1234;
pub(super) const LEN_HEADER_MAGIC: usize = 4;
const LEN_BITMAP: usize = NUM_SLOT / 8;
const LEN_NEXT: usize = 4;
const LEN_EXT: usize = 4;
const LEN_TAIL_OFFSET: usize = 4;
const LEN_FINGERPRINTS: usize = NUM_SLOT;
const LEN_KV_INFO: usize = NUM_SLOT * std::mem::size_of::<KVInfo>();
pub const LEAF_HEADER_SIZE: usize = LEN_HEADER_MAGIC
    + LEN_BITMAP
    + LEN_NEXT
    + LEN_EXT
    + LEN_TAIL_OFFSET
    + LEN_FINGERPRINTS
    + LEN_KV_INFO
    + data_util::LEN_CRC;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LeafHeader {
    magic: u32,
    bitmap: [u8; NUM_SLOT / 8],
    next: u32,
    ext: u32,
    tail_offset: u32,
    fingerprints: [u8; NUM_SLOT],
    kv_info: [KVInfo; NUM_SLOT],
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
struct KVInfo {
    page_id: u32,
    offset: u32,
    key_size: u32,
    value_size: u32,
}

impl LeafHeader {
    pub fn new() -> Self {
        LeafHeader {
            magic: HEADER_MAGIC,
            bitmap: [0u8; NUM_SLOT / 8],
            next: INVALID_LEAF_ID,
            ext: INVALID_LEAF_ID,
            fingerprints: [0u8; NUM_SLOT],
            kv_info: [KVInfo::new(); NUM_SLOT],
            tail_offset: INITIAL_TAIL_OFFSET as u32,
        }
    }

    pub fn need_split(&self) -> bool {
        self.bitmap.iter().all(|&x| x == 0xFF)
    }

    pub fn get_empty_slot(&self) -> Option<usize> {
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

    pub fn is_slot_set(&self, slot: usize) -> bool {
        let idx = slot / 8;
        let offset = slot % 8;

        self.bitmap[idx] & (1 << offset) != 0
    }

    pub fn set_slot(&mut self, slot: usize) {
        let idx = slot / 8;
        let offset = slot % 8;

        self.bitmap[idx] |= 1 << offset;
    }

    pub fn unset_slot(&mut self, slot: usize) {
        let idx = slot / 8;
        let offset = slot % 8;

        self.bitmap[idx] &= 0xFF ^ (1 << offset);
    }

    pub fn get_next(&self) -> Option<usize> {
        if self.next == INVALID_LEAF_ID {
            None
        } else {
            Some(self.next as _)
        }
    }

    pub fn set_next(&mut self, next_id: usize) {
        self.next = next_id as u32;
    }

    pub fn get_ext(&self) -> Option<usize> {
        if self.ext == INVALID_LEAF_ID {
            None
        } else {
            Some(self.ext as _)
        }
    }

    pub fn set_ext(&mut self, ext_id: usize) {
        self.ext = ext_id as u32;
    }

    pub fn get_tail_offset(&self) -> usize {
        self.tail_offset as usize
    }

    pub fn set_tail_offset(&mut self, data_offset: usize) {
        self.tail_offset = data_offset as u32;
    }

    pub fn get_fingerprints(&self) -> &[u8] {
        &self.fingerprints
    }

    pub fn set_fingerprint(&mut self, slot: usize, hash: u8) {
        self.fingerprints[slot] = hash;
    }

    pub fn get_kv_info(&self, slot: usize) -> (usize, usize, usize, usize) {
        self.kv_info[slot].get()
    }

    pub fn set_kv_info(
        &mut self,
        slot: usize,
        page_id: usize,
        data_offset: usize,
        key_size: usize,
        value_size: usize,
    ) {
        self.kv_info[slot].set(page_id, data_offset, key_size, value_size);
    }
}

impl KVInfo {
    fn new() -> Self {
        KVInfo {
            page_id: 0,
            offset: 0,
            key_size: 0,
            value_size: 0,
        }
    }

    fn get(&self) -> (usize, usize, usize, usize) {
        (
            self.page_id as usize,
            self.offset as usize,
            self.key_size as usize,
            self.value_size as usize,
        )
    }

    fn set(&mut self, page_id: usize, offset: usize, key_size: usize, value_size: usize) {
        self.page_id = page_id as u32;
        self.offset = offset as u32;
        self.key_size = key_size as u32;
        self.value_size = value_size as u32;
    }
}

impl std::fmt::Display for LeafHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bitmap: {:?}, next: {}, ext: {}, fingerprints: {:?}, kv_info: {:?}",
            self.bitmap, self.next, self.ext, self.fingerprints, self.kv_info
        )
    }
}

impl std::fmt::Display for KVInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "[offset: {}, key_size: {}, value_size {}]",
            self.offset, self.key_size, self.value_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header() -> LeafHeader {
        LeafHeader {
            magic: HEADER_MAGIC,
            bitmap: [0u8; NUM_SLOT / 8],
            next: INVALID_LEAF_ID,
            ext: INVALID_LEAF_ID,
            fingerprints: [0u8; NUM_SLOT],
            kv_info: [KVInfo::new(); NUM_SLOT],
            tail_offset: INITIAL_TAIL_OFFSET as u32,
        }
    }

    #[test]
    fn test_need_split() {
        let mut header = make_header();
        assert!(!header.need_split());
        for i in 0..NUM_SLOT {
            header.set_slot(i);
        }
        assert!(header.need_split());
    }

    #[test]
    fn test_slot() {
        let mut header = make_header();
        assert!(!header.is_slot_set(3));

        for i in 0..4 {
            header.set_slot(i);
        }
        assert_eq!(header.get_empty_slot().unwrap(), 4);
        assert!(header.is_slot_set(3));

        header.unset_slot(2);
        assert_eq!(header.get_empty_slot().unwrap(), 2);
    }
}
