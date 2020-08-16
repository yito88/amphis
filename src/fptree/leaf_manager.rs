use crc::{crc32, Hasher32};
use log::{trace, warn};
use memmap::{MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;

use crate::config::Config;

// TODO: parameterize them
const NUM_ALLOCATION: usize = 16;
const LEAF_SIZE: usize = 256 * 1024;

const NUM_SLOT: usize = 32;

const LEN_BITMAP: usize = NUM_SLOT / 8;
const LEN_NEXT: usize = 4;
const LEN_TAIL_OFFSET: usize = 4;
const LEN_FINGERPRINTS: usize = NUM_SLOT;
const LEN_KV_INFO: usize = NUM_SLOT * std::mem::size_of::<KVInfo>();
const LEAF_HEADER_SIZE: usize =
    LEN_BITMAP + LEN_NEXT + LEN_TAIL_OFFSET + LEN_FINGERPRINTS + LEN_KV_INFO;
const LEN_SIZE: usize = 4;
const LEN_CRC: usize = 4;
const LEN_REDUNDANCY: usize = LEN_SIZE + LEN_CRC;

pub struct LeafManager {
    leaves_file: File,
    leaves_in_tree: Vec<usize>,
    free_leaves: VecDeque<usize>,
    header_mmap: Vec<MmapMut>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LeafHeader {
    pub bitmap: [u8; NUM_SLOT / 8],
    pub next: u32,
    pub tail_offset: u32,
    pub fingerprints: [u8; NUM_SLOT],
    pub kv_info: [KVInfo; NUM_SLOT],
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
pub struct KVInfo {
    pub offset: u32,
    pub key_size: u32,
    pub value_size: u32,
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
                    File::create(&leaf_file_path)?
                }
                _ => return Err(e),
            },
        };

        // TODO: recovery
        Ok(LeafManager {
            leaves_file: file,
            leaves_in_tree: Vec::new(),
            free_leaves: VecDeque::new(),
            header_mmap: Vec::new(),
        })
    }

    pub fn get_free_leaf(&mut self) -> Result<usize, std::io::Error> {
        if self.free_leaves.is_empty() {
            self.allocate_new_leaves()?;
        }

        let new_offset = self.free_leaves.pop_front().unwrap();
        self.leaves_in_tree.push(new_offset);

        let index = self.leaves_in_tree.len() - 1;
        self.header_mmap.push(self.mmap_header(index)?);

        trace!("New leaf is allocated: {}", index);
        Ok(index)
    }

    fn allocate_new_leaves(&mut self) -> Result<(), std::io::Error> {
        trace!("New leaf group is allocated");
        let file_size = self.leaves_file.metadata()?.len() as usize;
        let new_size = file_size + NUM_ALLOCATION * LEAF_SIZE;
        self.leaves_file.set_len(new_size as u64)?;

        for offset in (file_size..new_size).step_by(LEAF_SIZE) {
            self.free_leaves.push_back(offset);
        }

        Ok(())
    }

    fn mmap_header(&self, id: usize) -> Result<MmapMut, std::io::Error> {
        let offset = self.leaves_in_tree[id];
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(LEAF_HEADER_SIZE)
                .map_mut(&self.leaves_file)?
        };

        Ok(mmap)
    }

    pub fn update_header(&mut self, id: usize, header: &LeafHeader) -> Result<(), std::io::Error> {
        let mmap = &mut self.header_mmap[id];
        let encoded: Vec<u8> = match bincode::serialize(header) {
            Ok(b) => b,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to serialize a leaf header",
                ))
            }
        };
        mmap.copy_from_slice(&encoded);
        mmap.flush()?;

        Ok(())
    }

    pub fn read_key(
        &self,
        id: usize,
        offset: usize,
        key_size: usize,
    ) -> Result<Vec<u8>, std::io::Error> {
        let data_offset = self.leaves_in_tree[id] + offset;
        let data_size = key_size + LEN_REDUNDANCY;
        let mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map(&self.leaves_file)?
        };

        self.check_crc(&mmap[0..])?;

        Ok(mmap[0..key_size].to_vec())
    }

    pub fn read_data(
        &self,
        id: usize,
        offset: usize,
        key_size: usize,
        value_size: usize,
    ) -> Result<(Vec<u8>, Vec<u8>), std::io::Error> {
        let data_offset = self.leaves_in_tree[id] + offset;
        let data_size = key_size + value_size + LEN_REDUNDANCY * 2;
        let mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map(&self.leaves_file)?
        };

        let value_start = key_size + LEN_REDUNDANCY;
        let value_end = value_start + value_size;

        self.check_crc(&mmap[0..value_start])?;
        self.check_crc(&mmap[value_start..])?;

        Ok((
            mmap[0..key_size].to_vec(),
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
        let data_size = key.len() + value.len() + LEN_REDUNDANCY * 2;
        let data_offset = self.leaves_in_tree[id] + offset - data_size;
        let mut mmap = unsafe {
            MmapOptions::new()
                .offset(data_offset as u64)
                .len(data_size)
                .map_mut(&self.leaves_file)?
        };

        let mut data: Vec<u8> = Vec::with_capacity(data_size);
        data.extend(key);
        data.extend(&(key.len() as u32).to_be_bytes());
        data.extend(&self.calc_crc(key).to_be_bytes());

        data.extend(value);
        data.extend(&(value.len() as u32).to_be_bytes());
        data.extend(&self.calc_crc(value).to_be_bytes());

        mmap.copy_from_slice(&data);
        mmap.flush()?;

        Ok(offset - data_size)
    }

    fn calc_crc(&self, data: &Vec<u8>) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(data);
        digest.write(&(data.len() as u32).to_be_bytes());

        digest.sum32()
    }

    fn check_crc(&self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let len = bytes.len();
        let crc = u32::from_be_bytes(bytes[(len - LEN_CRC)..len].try_into().unwrap());
        let size = u32::from_be_bytes(
            bytes[(len - LEN_REDUNDANCY)..(len - LEN_CRC)]
                .try_into()
                .unwrap(),
        );
        let data = bytes[0..size as usize].to_vec();

        if self.calc_crc(&data) == crc {
            Ok(())
        } else {
            // TODO: replace with an amphis error
            Err(std::io::Error::new(ErrorKind::Other, "CRC check failed!"))
        }
    }
}

impl LeafHeader {
    pub fn new() -> Self {
        LeafHeader {
            bitmap: [0; NUM_SLOT / 8],
            next: 0u32,
            fingerprints: [0u8; NUM_SLOT],
            kv_info: [KVInfo {
                offset: 0,
                key_size: 0,
                value_size: 0,
            }; NUM_SLOT],
            tail_offset: LEAF_SIZE as u32,
        }
    }

    pub fn need_split(&self, data_size: usize) -> bool {
        let no_space = self.tail_offset < (LEAF_HEADER_SIZE + data_size) as u32;
        let is_slot_full = self.bitmap.iter().all(|&x| x == 0xFF);

        no_space || is_slot_full
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

    pub fn set_next(&mut self, next_id: usize) {
        self.next = next_id as u32;
    }

    pub fn set_tail_offset(&mut self, data_offset: usize) {
        self.tail_offset = data_offset as u32;
    }

    pub fn set_fingerprint(&mut self, slot: usize, hash: u8) {
        self.fingerprints[slot] = hash;
    }

    pub fn get_kv_info(&self, slot: usize) -> (usize, usize, usize) {
        let data_offset = self.kv_info[slot].offset as usize;
        let key_size = self.kv_info[slot].key_size as usize;
        let value_size = self.kv_info[slot].value_size as usize;

        (data_offset, key_size, value_size)
    }

    pub fn set_kv_info(
        &mut self,
        slot: usize,
        data_offset: usize,
        key_size: usize,
        value_size: usize,
    ) {
        self.kv_info[slot].offset = data_offset as u32;
        self.kv_info[slot].key_size = key_size as u32;
        self.kv_info[slot].value_size = value_size as u32;
    }
}
