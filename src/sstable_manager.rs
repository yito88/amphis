use bloomfilter::Bloom;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::ErrorKind;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

use super::sparse_index::SparseIndex;
use crate::config::Config;
use crate::util::data_util;
use crate::util::file_util;

const READ_BUFFER_SIZE: usize = 1 << 16;

pub struct SstableManager {
    name: String,
    config: Config,
    filters: Arc<RwLock<BTreeMap<usize, Bloom<Vec<u8>>>>>,
    indexes: Arc<RwLock<BTreeMap<usize, SparseIndex>>>,
}

#[derive(Serialize, Deserialize)]
struct BloomElements {
    table_id: usize,
    bitmap_bits: u64,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
    bitmap: Vec<u8>,
}

type TableInfo = (usize, Bloom<Vec<u8>>);

impl SstableManager {
    pub fn new(name: &str, config: Config) -> Result<(Self, usize), std::io::Error> {
        let path = config.get_table_dir_path(name);
        let manager = SstableManager {
            name: name.to_string(),
            config,
            filters: Arc::new(RwLock::new(BTreeMap::new())),
            indexes: Arc::new(RwLock::new(BTreeMap::new())),
        };

        // recovery the current state
        let mut next_table_id = 0;
        if Path::new(&path).exists() {
            // find the next table ID
            for entry in std::fs::read_dir(path.clone())? {
                if let Some(table_id) = file_util::get_table_id(&entry?.path()) {
                    if next_table_id <= table_id {
                        next_table_id = (table_id / 2 + 1) * 2;
                    }
                }
            }
            debug!("next table ID: {}", next_table_id);

            // load the metadata
            manager.load_filters()?;
            manager.load_indexes()?;
        }

        Ok((manager, next_table_id))
    }

    pub fn register(
        &self,
        table_id: usize,
        filter: Bloom<Vec<u8>>,
        index: SparseIndex,
    ) -> Result<(), std::io::Error> {
        self.write_filter(table_id, &filter)?;
        self.write_index(&index)?;

        self.filters.write().unwrap().insert(table_id, filter);
        self.indexes.write().unwrap().insert(table_id, index);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        for (table_id, filter) in self.filters.read().unwrap().iter().rev() {
            trace!(
                "Check the bloom filter of SSTable {} with {:?}",
                table_id,
                key
            );
            if !filter.check(&key.to_vec()) {
                continue;
            }

            trace!("Read from SSTable {} with {:?}", table_id, key);
            let indexes = self.indexes.read().unwrap();
            let index = indexes.get(table_id).unwrap();
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
        key: &[u8],
        table_id: usize,
        offset: usize,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.config.get_table_file_path(&self.name, table_id);
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);
        reader.seek(SeekFrom::Start(offset as u64))?;

        loop {
            let cur_key = match self.read_data(&mut reader)? {
                Some(k) => k,
                None => return Ok(None),
            };
            let value = self.read_data(&mut reader)?;

            if cur_key == *key {
                return Ok(value);
            }
        }
    }

    fn read_data(&self, reader: &mut BufReader<File>) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut size_buf = [0_u8; data_util::LEN_SIZE];
        let len = reader.read(&mut size_buf)?;
        if len == 0 {
            return Ok(None);
        }
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut data = vec![0_u8; size];
        reader.read_exact(&mut data)?;

        let mut crc_buf = [0_u8; data_util::LEN_CRC];
        reader.read_exact(&mut crc_buf)?;
        let crc = u32::from_le_bytes(crc_buf);

        data_util::check_crc(data.as_slice(), crc)?;

        Ok(Some(data))
    }

    fn write_filter(&self, table_id: usize, filter: &Bloom<Vec<u8>>) -> Result<(), std::io::Error> {
        let file_path = self.config.get_filter_file_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut writer = BufWriter::new(&file);

        let elements = BloomElements {
            table_id,
            bitmap_bits: filter.number_of_bits(),
            k_num: filter.number_of_hash_functions(),
            sip_keys: filter.sip_keys(),
            bitmap: filter.bitmap(),
        };
        let encoded = bincode::serialize(&elements).expect("serializing the filter failed");
        let mut data: Vec<u8> = Vec::new();
        data.extend(&(encoded.len() as u32).to_le_bytes());
        data.extend(&encoded);
        data.extend(&data_util::calc_crc(&encoded).to_le_bytes());

        writer.write_all(&data)?;

        Ok(())
    }

    fn load_filters(&self) -> Result<(), std::io::Error> {
        let file_path = self.config.get_filter_file_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

        while let Some((id, filter)) = self.read_filter(&mut reader)? {
            self.filters.write().unwrap().insert(id, filter);
        }

        Ok(())
    }

    fn read_filter(
        &self,
        reader: &mut BufReader<File>,
    ) -> Result<Option<TableInfo>, std::io::Error> {
        let mut size_buf = [0_u8; data_util::LEN_SIZE];
        let len = reader.read(&mut size_buf)?;
        if len == 0 {
            return Ok(None);
        }
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut data = vec![0_u8; size];
        reader.read_exact(&mut data)?;

        let mut crc_buf = [0_u8; data_util::LEN_CRC];
        reader.read_exact(&mut crc_buf)?;
        let crc = u32::from_le_bytes(crc_buf);

        data_util::check_crc(data.as_slice(), crc)?;

        let elements: BloomElements = match bincode::deserialize(&data) {
            Ok(e) => e,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to deserialize bloom elements",
                ))
            }
        };
        let filter = Bloom::from_existing(
            &elements.bitmap,
            elements.bitmap_bits,
            elements.k_num,
            elements.sip_keys,
        );
        Ok(Some((elements.table_id, filter)))
    }

    fn write_index(&self, index: &SparseIndex) -> Result<(), std::io::Error> {
        let file_path = self.config.get_index_file_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut writer = BufWriter::new(&file);

        let encoded = match bincode::serialize(&index) {
            Ok(b) => b,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to serialize a sparse index",
                ))
            }
        };

        let mut data: Vec<u8> = Vec::new();
        data.extend(&(encoded.len() as u32).to_le_bytes());
        data.extend(&encoded);
        data.extend(&data_util::calc_crc(&encoded).to_le_bytes());

        writer.write_all(&data)?;

        Ok(())
    }

    fn load_indexes(&self) -> Result<(), std::io::Error> {
        let file_path = self.config.get_index_file_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

        while let Some(index) = self.read_index(&mut reader)? {
            self.indexes
                .write()
                .unwrap()
                .insert(index.get_table_id(), index);
        }

        Ok(())
    }

    fn read_index(
        &self,
        reader: &mut BufReader<File>,
    ) -> Result<Option<SparseIndex>, std::io::Error> {
        let mut size_buf = [0_u8; data_util::LEN_SIZE];
        let len = reader.read(&mut size_buf)?;
        if len == 0 {
            return Ok(None);
        }
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut data = vec![0_u8; size];
        reader.read_exact(&mut data)?;

        let mut crc_buf = [0_u8; data_util::LEN_CRC];
        reader.read_exact(&mut crc_buf)?;
        let crc = u32::from_le_bytes(crc_buf);

        data_util::check_crc(data.as_slice(), crc)?;

        let index: SparseIndex = match bincode::deserialize(&data) {
            Ok(i) => i,
            // TODO: replace with an amphis error
            Err(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "failed to deserialize a sparse index",
                ))
            }
        };
        Ok(Some(index))
    }
}
