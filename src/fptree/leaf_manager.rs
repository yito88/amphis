use log::{debug, trace, warn};
use memmap::{MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::File;
use std::io::ErrorKind;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::util::data_util;
use crate::util::file_util;

#[cfg(test)]
use mockall::automock;

// TODO: parameterize them
pub const NUM_SLOT: usize = 32;
const NUM_ALLOCATION: usize = 16;
const LEAF_SIZE: usize = 1024 * 1024;

// for header format
const HEADER_MAGIC: u32 = 0x1234;
const INVALID_LEAF_ID: u32 = u32::MAX;
pub const INITIAL_TAIL_OFFSET: usize = data_util::DATA_ALIGNMENT;
const END_TAIL_OFFSET: usize = LEAF_SIZE - data_util::DATA_ALIGNMENT;
const LEN_HEADER_MAGIC: usize = 4;
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

pub struct LeafManager {
    leaves_file: File,
    free_leaves: VecDeque<usize>,
    header_mmap: HashMap<usize, Arc<RwLock<MmapMut>>>,
}

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

#[cfg_attr(test, automock)]
impl LeafManager {
    pub fn new(name: &str, id: usize, config: &Config) -> Result<Self, std::io::Error> {
        let data_dir = config.get_data_dir_path(name);
        match std::fs::create_dir_all(&data_dir) {
            Ok(_) => (),
            Err(e) => panic!("{} - {}", &data_dir, e),
        }

        let file_path = config.get_leaf_file_path(name, id);
        let (file, is_created) = file_util::open_file(&file_path)?;
        let mut manager = LeafManager {
            leaves_file: file,
            free_leaves: VecDeque::new(),
            header_mmap: HashMap::new(),
        };

        if !is_created {
            debug!("recovering headers for FPTree {}", id);
            manager.recover_state()?;
        }

        Ok(manager)
    }

    pub fn allocate_leaf(&mut self) -> Result<(usize, LeafHeader), std::io::Error> {
        if self.free_leaves.is_empty() {
            self.allocate_new_leaves()?;
        }

        let new_id = self.free_leaves.pop_front().unwrap();
        self.header_mmap
            .insert(new_id, Arc::new(RwLock::new(self.mmap_header(new_id)?)));

        trace!("New leaf is allocated: {}", new_id);
        Ok((new_id, LeafHeader::new()))
    }

    pub fn allocate_ext_page(&mut self, id: usize) -> Result<usize, std::io::Error> {
        if self.free_leaves.is_empty() {
            self.allocate_new_leaves()?;
        }
        let new_id = self.free_leaves.pop_front().unwrap();
        self.header_mmap
            .insert(new_id, Arc::new(RwLock::new(self.mmap_header(new_id)?)));

        // update the last leaf to be appended
        let mut last_id = id;
        let mut last_header = self
            .get_header(id)
            .expect("the appended header doesn't exist");
        while let Some(ext) = last_header.get_ext() {
            last_id = ext;
            last_header = self
                .get_header(ext)
                .expect("the appended header doesn't exist");
        }
        last_header.set_ext(new_id);
        self.commit_header(last_id, &&last_header)?;

        Ok(new_id)
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

    pub fn get_header(&self, id: usize) -> Option<LeafHeader> {
        match self.header_mmap.get(&id) {
            Some(mmap) => {
                let header: LeafHeader =
                    bincode::deserialize(mmap.read().unwrap().as_ref()).unwrap();
                Some(header)
            }
            None => None,
        }
    }

    pub fn commit_header(&self, id: usize, header: &LeafHeader) -> Result<(), std::io::Error> {
        let mut mmap = self.header_mmap.get(&id).unwrap().write().unwrap();
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
        encoded.extend(&data_util::calc_crc(&encoded).to_le_bytes());
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
        let data_size = data_util::get_data_size(key_size, value_size);
        let mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map(&self.leaves_file)?
        };
        let bound_offset = data_util::get_bound_offset(key_size);
        data_util::check_slot_crc(&mmap[..bound_offset])?;
        data_util::check_slot_crc(&mmap[bound_offset..])?;
        let (key_start, key_end) = data_util::get_key_offset(key_size);
        if value_size == 0 {
            Ok((mmap[key_start..key_end].to_vec(), Vec::new()))
        } else {
            let (value_start, value_end) = data_util::get_value_offset(key_size, value_size);
            Ok((
                mmap[key_start..key_end].to_vec(),
                mmap[value_start..value_end].to_vec(),
            ))
        }
    }

    pub fn write_data(
        &self,
        id: usize,
        offset: usize,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<Option<usize>, std::io::Error> {
        let data_size = data_util::get_data_size(key.len(), value.len());
        let aligned_tail = offset + data_util::round_up_size(data_size);
        if aligned_tail > END_TAIL_OFFSET {
            return Ok(None);
        }
        let data_offset = id * LEAF_SIZE + offset;
        let mut mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map_mut(&self.leaves_file)?
        };

        let data = data_util::format_data_with_crc(&key, &value);
        mmap.copy_from_slice(&data);
        mmap.flush()?;

        Ok(Some(aligned_tail))
    }

    pub fn get_leaf_id_chain(&self) -> Vec<usize> {
        let mut leaf_id_chain = Vec::new();
        let first_mmap = self
            .header_mmap
            .get(&0)
            .expect("no header for the first leaf");
        let mut header: LeafHeader =
            bincode::deserialize(first_mmap.read().unwrap().as_ref()).unwrap();
        leaf_id_chain.push(0);
        while let Some(next) = header.get_next() {
            leaf_id_chain.push(next);
            let next_mmap = self
                .header_mmap
                .get(&next)
                .expect("no header for the first leaf");
            header = bincode::deserialize(next_mmap.read().unwrap().as_ref()).unwrap();
        }

        leaf_id_chain
    }

    fn recover_state(&mut self) -> Result<(), std::io::Error> {
        let file_size = self.leaves_file.metadata()?.len() as usize;
        for id in 0..(file_size / LEAF_SIZE) {
            let mmap = self.mmap_header(id)?;

            // validate the header
            let magic = u32::from_le_bytes(mmap[0..LEN_HEADER_MAGIC].try_into().unwrap());
            if magic != HEADER_MAGIC {
                self.free_leaves.push_back(id);
                continue;
            }

            match data_util::check_header_crc(&mmap) {
                Ok(_) => {
                    self.header_mmap.insert(id, Arc::new(RwLock::new(mmap)));
                }
                Err(_) => {
                    // TODO: check another header field
                    warn!("header's CRC check failed");
                    self.free_leaves.push_back(id);
                }
            }
        }

        Ok(())
    }
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
