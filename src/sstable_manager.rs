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
    tables: Arc<RwLock<Vec<BTreeMap<TableId, TableInfo>>>>,
}

pub type TableId = usize;

#[derive(Serialize, Deserialize)]
pub struct TableInfo {
    pub id: TableId,
    pub size: usize,
    pub level: usize,
    pub filter: Bloom<Vec<u8>>,
    pub index: SparseIndex,
}

impl SstableManager {
    pub fn new(name: &str, config: Config) -> Result<(Self, usize), std::io::Error> {
        let path = config.get_table_dir_path(name);
        let manager = SstableManager {
            name: name.to_string(),
            config,
            tables: Arc::new(RwLock::new(Vec::new())),
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

            manager.load_table_info()?;
        }

        Ok((manager, next_table_id))
    }

    pub fn register(&self, table_info: TableInfo) -> Result<(), std::io::Error> {
        self.write_table_info(&table_info)?;

        // Register the new table to Level 0
        let mut tables = self.tables.write().unwrap();
        match tables.get_mut(0) {
            Some(level_zero) => {
                level_zero.insert(table_info.id, table_info);
            }
            None => {
                let mut level_zero = BTreeMap::new();
                level_zero.insert(table_info.id, table_info);
                tables.push(level_zero);
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        for leveled_tables in self.tables.read().unwrap().iter() {
            for (table_id, table_info) in leveled_tables.iter().rev() {
                trace!(
                    "Check the bloom filter of SSTable {} with {:?}",
                    table_id,
                    key
                );
                if !table_info.filter.check(&key.to_vec()) {
                    continue;
                }

                trace!("Read from SSTable {} with {:?}", table_id, key);
                let offset = table_info.index.get(key);
                match self.get_from_table(key, *table_id, offset)? {
                    Some(r) => return Ok(Some(r)),
                    None => continue,
                }
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

    fn write_table_info(&self, table_info: &TableInfo) -> Result<(), std::io::Error> {
        let file_path = self.config.get_metadata_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut writer = BufWriter::new(&file);

        let encoded = bincode::serialize(table_info).expect("serializing the table info failed");
        let mut data: Vec<u8> = Vec::new();
        data.extend(&(encoded.len() as u32).to_le_bytes());
        data.extend(&encoded);
        data.extend(&data_util::calc_crc(&encoded).to_le_bytes());

        writer.write_all(&data)?;

        Ok(())
    }

    fn load_table_info(&self) -> Result<(), std::io::Error> {
        let file_path = self.config.get_metadata_path(&self.name);
        let (file, _) = file_util::open_file(&file_path)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

        while let Some(table_info) = self.read_table_info(&mut reader)? {
            debug!("load table info for ID: {}", table_info.id);
            let mut tables = self.tables.write().unwrap();
            match tables.get_mut(table_info.level) {
                Some(tables) => {
                    tables.insert(table_info.id, table_info);
                }
                None => {
                    while tables.len() < table_info.level {
                        tables.push(BTreeMap::new());
                    }
                    let mut leveled_tables = BTreeMap::new();
                    leveled_tables.insert(table_info.id, table_info);
                    tables.push(leveled_tables);
                }
            }
        }

        Ok(())
    }

    fn read_table_info(
        &self,
        reader: &mut BufReader<File>,
    ) -> Result<Option<TableInfo>, std::io::Error> {
        match self.read_data(reader)? {
            Some(bytes) => {
                let table_info = bincode::deserialize(&bytes).map_err(|_| {
                    std::io::Error::new(ErrorKind::Other, "failed to deserialize the table info")
                })?;
                Ok(Some(table_info))
            }
            None => Ok(None),
        }
    }
}
