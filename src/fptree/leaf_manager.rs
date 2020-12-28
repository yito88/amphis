use log::{trace, warn};
use memmap::{MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;

use crate::config::Config;
use crate::data_utility;

#[cfg(test)]
use mockall::automock;

// TODO: parameterize them
pub const NUM_SLOT: usize = 32;
const NUM_ALLOCATION: usize = 16;
const LEAF_SIZE: usize = 256 * 1024;
const RECLAMATION_THRESHOLD: usize = LEAF_SIZE / 2;
const RECLAMATION_RATE: f32 = 0.5;

const LEN_BITMAP: usize = NUM_SLOT / 8;
const LEN_NEXT: usize = 4;
const LEN_TAIL_OFFSET: usize = 4;
const LEN_FINGERPRINTS: usize = NUM_SLOT;
const LEN_KV_INFO: usize = NUM_SLOT * std::mem::size_of::<KVInfo>();
const LEAF_HEADER_SIZE: usize = LEN_BITMAP
    + LEN_NEXT
    + LEN_TAIL_OFFSET
    + LEN_FINGERPRINTS
    + LEN_KV_INFO
    + data_utility::LEN_CRC;
const DATA_OFFSET: usize = 4 * 1024;

pub struct LeafManager {
    leaves_file: File,
    free_leaves: VecDeque<usize>,
    header_mmap: Vec<MmapMut>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LeafHeader {
    bitmap: [u8; NUM_SLOT / 8],
    next: u32,
    tail_offset: u32,
    fingerprints: [u8; NUM_SLOT],
    kv_info: [KVInfo; NUM_SLOT],
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
struct KVInfo {
    offset: u32,
    key_size: u32,
    value_size: u32,
}

#[cfg_attr(test, automock)]
impl LeafManager {
    pub fn new(config: &Config) -> Result<Self, std::io::Error> {
        match std::fs::create_dir_all(&config.data_dir) {
            Ok(_) => (),
            Err(e) => panic!("{} - {}", &config.data_dir, e),
        }

        let leaf_file_path = config.get_leaf_file_path();
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&leaf_file_path)
        {
            Ok(f) => f,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    warn!("a new leaf file is created");
                    let f = File::create(&leaf_file_path)?;
                    f.sync_all()?;
                    f
                }
                _ => return Err(e),
            },
        };

        // TODO: recovery
        Ok(LeafManager {
            leaves_file: file,
            free_leaves: VecDeque::new(),
            header_mmap: Vec::new(),
        })
    }

    pub fn get_free_leaf(&mut self) -> Result<(usize, LeafHeader), std::io::Error> {
        if self.free_leaves.is_empty() {
            self.allocate_new_leaves()?;
        }

        let new_id = self.free_leaves.pop_front().unwrap();
        self.header_mmap.push(self.mmap_header(new_id)?);

        trace!("New leaf is allocated: {}", new_id);
        Ok((new_id, LeafHeader::new()))
    }

    fn allocate_new_leaves(&mut self) -> Result<(), std::io::Error> {
        trace!("New leaf group is allocated");
        let file_size = self.leaves_file.metadata()?.len() as usize;
        let start_id = file_size / LEAF_SIZE;
        let end_id = start_id + NUM_ALLOCATION;

        let new_size = file_size + NUM_ALLOCATION * LEAF_SIZE;
        self.leaves_file.set_len(new_size as u64)?;

        for id in start_id..end_id {
            self.free_leaves.push_back(id);
        }

        Ok(())
    }

    fn mmap_header(&self, id: usize) -> Result<MmapMut, std::io::Error> {
        // TODO: protect the header when write failure (tail header)
        let offset = id * LEAF_SIZE;
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(LEAF_HEADER_SIZE)
                .map_mut(&self.leaves_file)?
        };

        Ok(mmap)
    }

    pub fn commit_header(&mut self, id: usize, header: &LeafHeader) -> Result<(), std::io::Error> {
        let mmap = &mut self.header_mmap[id];
        let mut encoded: Vec<u8> = match bincode::serialize(header) {
            Ok(b) => b,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to serialize a leaf header",
                ))
            }
        };
        encoded.extend(&data_utility::calc_crc(&encoded).to_le_bytes());
        mmap.copy_from_slice(&encoded);
        mmap.flush()?;

        Ok(())
    }

    pub fn read_data(
        &self,
        id: usize,
        offset: usize,
        key_size: usize,
        value_size: usize,
    ) -> Result<(Vec<u8>, Vec<u8>), std::io::Error> {
        let data_offset = id * LEAF_SIZE + offset;
        let data_size = key_size + value_size + data_utility::LEN_REDUNDANCY * 2;
        let mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map(&self.leaves_file)?
        };
        let bound_offset = key_size + data_utility::LEN_REDUNDANCY;
        data_utility::check_kv_crc(&mmap[..bound_offset])?;
        data_utility::check_kv_crc(&mmap[bound_offset..])?;

        let (key_start, key_end) = data_utility::get_key_offset(key_size);
        let (value_start, value_end) = data_utility::get_value_offset(key_size, value_size);

        Ok((
            mmap[key_start..key_end].to_vec(),
            mmap[value_start..value_end].to_vec(),
        ))
    }

    pub fn write_data(
        &mut self,
        id: usize,
        offset: usize,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<usize, std::io::Error> {
        let data_size = key.len() + value.len() + data_utility::LEN_REDUNDANCY * 2;
        let data_offset = id * LEAF_SIZE + offset;
        let mut mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map_mut(&self.leaves_file)?
        };

        let data = data_utility::format_data_with_crc(&key, &value);
        mmap.copy_from_slice(&data);
        mmap.flush()?;

        Ok(offset + data_size)
    }
}

impl LeafHeader {
    pub fn new() -> Self {
        LeafHeader {
            bitmap: [0u8; NUM_SLOT / 8],
            next: 0u32,
            fingerprints: [0u8; NUM_SLOT],
            kv_info: [KVInfo::new(); NUM_SLOT],
            tail_offset: DATA_OFFSET as u32,
        }
    }

    pub fn need_split(&self, data_size: usize) -> bool {
        let no_space = LEAF_SIZE < self.tail_offset as usize + data_size;
        let is_slot_full = self.bitmap.iter().all(|&x| x == 0xFF);

        no_space || is_slot_full
    }

    pub fn need_reclamation(&self) -> bool {
        if (self.tail_offset as usize) < RECLAMATION_THRESHOLD {
            return false;
        }

        let mut valid_size = 0;
        for slot in 0..NUM_SLOT {
            if self.is_slot_set(slot) {
                let (_, key_size, value_size) = self.kv_info[slot].get();
                valid_size += key_size + value_size + data_utility::LEN_REDUNDANCY * 2;
            }
        }

        valid_size < (self.tail_offset as f32 * RECLAMATION_RATE) as usize
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

    pub fn get_next(&mut self) -> usize {
        self.next as usize
    }

    pub fn set_next(&mut self, next_id: usize) {
        self.next = next_id as u32;
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

    pub fn get_kv_info(&self, slot: usize) -> (usize, usize, usize) {
        self.kv_info[slot].get()
    }

    pub fn set_kv_info(
        &mut self,
        slot: usize,
        data_offset: usize,
        key_size: usize,
        value_size: usize,
    ) {
        self.kv_info[slot].set(data_offset, key_size, value_size);
    }
}

impl KVInfo {
    fn new() -> Self {
        KVInfo {
            offset: 0,
            key_size: 0,
            value_size: 0,
        }
    }

    fn get(&self) -> (usize, usize, usize) {
        (
            self.offset as usize,
            self.key_size as usize,
            self.value_size as usize,
        )
    }

    fn set(&mut self, offset: usize, key_size: usize, value_size: usize) {
        self.offset = offset as u32;
        self.key_size = key_size as u32;
        self.value_size = value_size as u32;
    }
}

impl std::fmt::Display for LeafHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bitmap: {:?}, next: {}, fingerprints: {:?}, kv_info: {:?}",
            self.bitmap, self.next, self.fingerprints, self.kv_info
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
            bitmap: [0u8; NUM_SLOT / 8],
            next: 0u32,
            fingerprints: [0u8; NUM_SLOT],
            kv_info: [KVInfo::new(); NUM_SLOT],
            tail_offset: DATA_OFFSET as u32,
        }
    }

    #[test]
    fn test_need_split() {
        let mut header = make_header();
        assert!(!header.need_split(100));

        header.set_tail_offset(LEAF_SIZE - 50);
        assert!(header.need_split(100));
        assert!(!header.need_split(10));

        for i in 0..NUM_SLOT {
            header.set_slot(i);
        }
        assert!(header.need_split(0));
    }

    #[test]
    fn test_need_reclamation() {
        let mut header = make_header();
        assert!(!header.need_reclamation());

        let mut offset = 0;
        let key_size = 2 * 1024;
        let value_size = 4 * 1024;
        for i in 0..32 {
            offset += key_size + value_size + data_utility::LEN_REDUNDANCY * 2;
            if i % 3 == 0 {
                header.set_slot(i);
            }
            header.set_kv_info(i, offset, key_size, value_size);
        }
        header.set_tail_offset(offset);

        assert!(header.need_reclamation());
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
