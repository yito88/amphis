use bloomfilter::Bloom;
use log::trace;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::{Arc, RwLock};

use super::sparse_index::SparseIndex;
use crate::config::Config;
use crate::data_utility;

const READ_BUFFER_SIZE: usize = 1 << 16;

pub struct SstableManager {
    name: String,
    config: Config,
    filters: Arc<RwLock<BTreeMap<usize, Bloom<Vec<u8>>>>>,
    indexes: Arc<RwLock<BTreeMap<usize, SparseIndex>>>,
}

impl SstableManager {
    pub fn new(name: &str, config: Config) -> Self {
        // TODO: recovery
        SstableManager {
            name: name.to_string(),
            config,
            filters: Arc::new(RwLock::new(BTreeMap::new())),
            indexes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn register_table(&self, table_id: usize, filter: Bloom<Vec<u8>>, index: SparseIndex) {
        self.filters.write().unwrap().insert(table_id, filter);
        self.indexes.write().unwrap().insert(table_id, index);
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        for (table_id, filter) in self.filters.read().unwrap().iter().rev() {
            if !filter.check(key) {
                continue;
            }

            trace!("Read from SSTable {} with {:?}", table_id, key);
            let indexes = self.indexes.read().unwrap();
            let index = indexes.get(&table_id).unwrap();
            let offset = index.get(key);
            match self.get_from_table(key, *table_id, offset)? {
                Some(r) => return Ok(Some(r)),
                None => continue,
            }
        }

        Ok(None)
    }

    fn get_from_table(
        &self,
        key: &Vec<u8>,
        table_id: usize,
        offset: usize,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.config.get_table_file_path(&self.name, table_id);
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);
        reader.seek(SeekFrom::Start(offset as u64))?;

        loop {
            let cur_key;
            match self.read_data(&mut reader)? {
                Some(k) => cur_key = k,
                None => return Ok(None),
            }
            let value = self.read_data(&mut reader)?;

            if cur_key == *key {
                return Ok(value);
            }
        }
    }

    fn read_data(&self, reader: &mut BufReader<File>) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut size_buf = [0_u8; data_utility::LEN_SIZE];
        let len = reader.read(&mut size_buf)?;
        if len == 0 {
            return Ok(None);
        }
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut data = vec![0_u8; size];
        reader.read(&mut data)?;

        let mut crc_buf = [0_u8; data_utility::LEN_CRC];
        reader.read(&mut crc_buf)?;
        let crc = u32::from_le_bytes(crc_buf);

        data_utility::check_crc(data.as_slice(), crc)?;

        Ok(Some(data))
    }
}
