use bloomfilter::Bloom;
use log::trace;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::ErrorKind;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::io::{BufWriter, Write};
use std::sync::{Arc, RwLock};

use super::sparse_index::SparseIndex;
use crate::config::Config;
use crate::data_utility;
use crate::file_utility;

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

    pub fn register(
        &self,
        table_id: usize,
        filter: Bloom<Vec<u8>>,
        index: SparseIndex,
    ) -> Result<(), std::io::Error> {
        self.write_filter(table_id, &filter)?;
        self.write_index(table_id, &index)?;

        self.filters.write().unwrap().insert(table_id, filter);
        self.indexes.write().unwrap().insert(table_id, index);

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        for (table_id, filter) in self.filters.read().unwrap().iter().rev() {
            trace!(
                "Check the bloom filter of SSTable {} with {:?}",
                table_id,
                key
            );
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

    fn write_filter(&self, table_id: usize, filter: &Bloom<Vec<u8>>) -> Result<(), std::io::Error> {
        let file_path = self.config.get_filter_file_path(&self.name);
        let (file, _) = file_utility::open_file(&file_path)?;
        let mut writer = BufWriter::new(&file);

        let mut encoded: Vec<u8> = Vec::new();
        encoded.extend(&(table_id as u32).to_le_bytes());
        encoded.extend(&(filter.number_of_bits() / 8).to_le_bytes());
        encoded.extend(filter.bitmap());
        encoded.extend(&filter.number_of_hash_functions().to_le_bytes());
        let [k1, k2] = filter.sip_keys();
        encoded.extend(&k1.0.to_le_bytes());
        encoded.extend(&k1.1.to_le_bytes());
        encoded.extend(&k2.0.to_le_bytes());
        encoded.extend(&k2.1.to_le_bytes());

        let mut data: Vec<u8> = Vec::new();
        data.extend(&(encoded.len() as u32).to_le_bytes());
        data.extend(&encoded);
        data.extend(&data_utility::calc_crc(&encoded).to_le_bytes());

        writer.write(&data)?;

        Ok(())
    }

    fn write_index(&self, table_id: usize, index: &SparseIndex) -> Result<(), std::io::Error> {
        let file_path = self.config.get_index_file_path(&self.name);
        let (file, _) = file_utility::open_file(&file_path)?;
        let mut writer = BufWriter::new(&file);

        let mut encoded: Vec<u8> = Vec::new();
        encoded.extend(&(table_id as u32).to_le_bytes());
        encoded.extend(match bincode::serialize(&index) {
            Ok(b) => b,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to serialize a sparse index",
                ))
            }
        });

        let mut data: Vec<u8> = Vec::new();
        data.extend(&(encoded.len() as u32).to_le_bytes());
        data.extend(&encoded);
        data.extend(&data_utility::calc_crc(&encoded).to_le_bytes());

        writer.write(&data)?;

        Ok(())
    }
}
