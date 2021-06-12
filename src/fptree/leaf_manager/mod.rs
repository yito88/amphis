mod types;

use log::{debug, trace, warn};
use memmap::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::File;
use std::io::ErrorKind;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::util::data_util;
use crate::util::file_util;

pub use types::{
    LeafHeader, END_TAIL_OFFSET, INITIAL_TAIL_OFFSET, LEAF_HEADER_SIZE, LEAF_SIZE, NUM_ALLOCATION,
    NUM_SLOT,
};
use types::{HEADER_MAGIC, LEN_HEADER_MAGIC};

#[cfg(test)]
use mockall::automock;

pub struct LeafManager {
    leaves_file: File,
    free_leaves: VecDeque<usize>,
    header_mmap: HashMap<usize, Arc<RwLock<MmapMut>>>,
}

#[cfg_attr(test, automock)]
impl LeafManager {
    pub fn new(name: &str, id: usize, config: &Config) -> Result<Self, std::io::Error> {
        let data_dir = config.get_leaf_dir_path(name);
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
        let mut header = self.get_header(0).expect("no header for the first leaf");
        leaf_id_chain.push(0);

        while let Some(next) = header.get_next() {
            leaf_id_chain.push(next);
            header = self.get_header(next).expect("the header should exist");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_page() {
        let config = Config::new_for_testing();
        let mut manager =
            LeafManager::new("test", 0, &config).expect("cannot create a leaf manager");
        let (id, header) = manager.allocate_leaf().expect("page allocation failed");
        assert_eq!(id, 0);
        assert_eq!(header.get_next(), None);
        assert_eq!(header.get_ext(), None);
        assert_eq!(manager.free_leaves.len(), NUM_ALLOCATION - 1);

        // commit the header for the next allocation
        manager.commit_header(id, &header).expect("commit failed");

        // allocate an extension page
        let ext_id = manager
            .allocate_ext_page(id)
            .expect("extension page allocation failed");
        // load the root header
        let mut header = manager.get_header(id).expect("no header");
        assert_eq!(header.get_next(), None);
        assert_eq!(header.get_ext(), Some(ext_id));
        assert_eq!(manager.free_leaves.len(), NUM_ALLOCATION - 2);

        // allocate new pages
        let (next_id, next_header) = manager.allocate_leaf().expect("page allocation failed");
        header.set_next(next_id);
        manager.commit_header(id, &header).expect("commit failed");
        manager
            .commit_header(next_id, &next_header)
            .expect("commit failed");

        let leaf_id_chain = manager.get_leaf_id_chain();
        assert_eq!(leaf_id_chain, vec![id, next_id]);
    }

    #[test]
    fn test_read_write_data() {
        let config = Config::new_for_testing();
        let mut manager =
            LeafManager::new("test", 0, &config).expect("cannot create a leaf manager");
        let (id, _) = manager.allocate_leaf().expect("page allocation failed");

        let key = vec![0u8];
        let value = vec![0u8, 0u8];
        manager
            .write_data(id, 4096, &key, &value)
            .expect("write failed");
        let (ret_key, ret_value) = manager
            .read_data(id, 4096, key.len(), value.len())
            .expect("read failed");
        assert_eq!(ret_key, key);
        assert_eq!(ret_value, value);

        // read/write a tombstone
        let key = vec![0u8];
        let value = vec![];
        manager
            .write_data(id, 4096, &key, &value)
            .expect("write failed");
        let (ret_key, ret_value) = manager
            .read_data(id, 8192, key.len(), value.len())
            .expect("read failed");
        assert_eq!(ret_key, key);
        assert!(ret_value.is_empty());
    }
}
