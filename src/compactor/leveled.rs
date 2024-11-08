use bloomfilter::Bloom;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

use super::CompactionContext;
use crate::config::Config;
use crate::sparse_index::SparseIndex;
use crate::sstable_manager::{read_data, LeveledTables, TableId, TableInfo, Tables};
use crate::util::data_util;

const WRITE_BUFFER_SIZE: usize = 1 << 18;
const READ_BUFFER_SIZE: usize = 1 << 16;

pub struct LeveledCompactor {
    name: String,
    /// Configurations
    config: Config,
    /// Shared table info.
    tables: Arc<RwLock<LeveledTables>>,
}

impl LeveledCompactor {
    pub fn new(name: String, config: Config, tables: Arc<RwLock<LeveledTables>>) -> Self {
        Self {
            name,
            config,
            tables,
        }
    }
}

impl CompactionContext for LeveledCompactor {
    fn compact(
        &self,
        level: usize,
        deleted_tables: Vec<Vec<TableId>>,
        new_tables: LeveledTables,
    ) -> Result<(Vec<Vec<TableId>>, LeveledTables), std::io::Error> {
        let mut target_tables = match new_tables.get(level) {
            Some(tables) => tables,
            None => return Ok((deleted_tables, new_tables)),
        };
        let current_tables = match self.tables.read().unwrap().get(level) {
            Some(table) => table,
            None => return Ok((deleted_tables, new_tables)),
        };
        let head_key = target_tables
            .first_key_value()
            .map(|(_, table)| table.range.0)
            .expect("Smallest table should exist");
        let tail_key = target_tables
            .last_key_value()
            .map(|(_, table)| table.range.1)
            .expect("Largest table should exist");

        let new_table_id = current_tables
            .last_key_value()
            .map(|(_, table)| table.id + 1)
            .expect("Largest table should exist");
        let table_file_path = self.config.get_table_file_path(&self.name, new_table_id);
        let table_file = File::create(table_file_path)?;
        let mut writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, &table_file);

        let mut current_tables = current_tables
            .iter()
            .filter(|(_, table)| table.range.0 <= tail_key && head_key <= table.range.1);
        let deleted_table_ids = current_tables.map(|table| table.0).collect();

        let mut offset = 0;
        let mut index = SparseIndex::new();
        let mut filter = Bloom::new_for_fp_rate(
            self.config.get_filter_items_count(),
            self.config.get_filter_fp_rate(),
        );
        let mut key_range = (None, None);

        let next_table_reader = |tables_iter: &mut dyn Iterator<Item = (&TableId, &TableInfo)>| -> Option<BufReader<File>> {
            let table = tables_iter.next();
            table.map(|(id, _)| {
                let path = self.config.get_table_file_path(&self.name, *id);
                let file = File::open(path).expect("File should exist");
                BufReader::with_capacity(READ_BUFFER_SIZE, file)
            })
        };

        // TODO: hard to read the big loop. Need refactoring
        let mut target_tables = target_tables.iter();
        let mut target_reader = next_table_reader(&mut target_tables);
        let mut current_reader = next_table_reader(&mut current_tables);

        let mut target_pair = match target_reader {
            Some(reader) => read_data(&mut reader)?.zip(read_data(&mut reader)?),
            None => None,
        };
        let mut current_pair = match current_reader {
            Some(reader) => read_data(&mut reader)?.zip(read_data(&mut reader)?),
            None => None,
        };
        loop {
            let (key, value) = match (target_pair, current_pair) {
                (Some((target_key, target_val)), Some((current_key, current_val))) => {
                    if target_key <= current_key {
                        let reader = target_reader.expect("Reader should exist");
                        target_pair = read_data(&mut reader)?.zip(read_data(&mut reader)?);
                        (target_key, target_val)
                    } else {
                        let reader = current_reader.expect("Reader should exist");
                        current_pair = read_data(&mut reader)?.zip(read_data(&mut reader)?);
                        if current_pair.is_none() {}
                        (current_key, current_val)
                    }
                }
                (Some((target_key, target_val)), None) => {
                    // check the next table
                    current_reader = next_table_reader(&mut current_tables);
                    match current_reader {
                        Some(reader) => {
                            current_pair = read_data(&mut reader)?.zip(read_data(&mut reader)?);
                            // compare the target and the current again
                            continue;
                        }
                        None => (target_key, target_val),
                    }
                }
                (None, Some((current_key, current_val))) => {
                    // check the next table
                    target_reader = next_table_reader(&mut target_tables);
                    match target_reader {
                        Some(reader) => {
                            target_pair = read_data(&mut reader)?.zip(read_data(&mut reader)?);
                            // compare the target and the current again
                            continue;
                        }
                        None => (current_key, current_val),
                    }
                }
                (None, None) => break,
            };
            if key_range.0.is_none() {
                key_range.0 = Some(key.clone());
            }
            filter.set(&key);
            index.insert(&key, offset);
            offset += data_util::get_data_size(key.len(), value.len());
            writer.write_all(&data_util::format_data_with_crc(&key, &value))?;
            key_range.1 = Some(key);
        }

        table_file.sync_all()?;
        let range = (
            key_range.0.expect("Head key should exist"),
            key_range.1.expect("Tail key should exist"),
        );

        let table_info = TableInfo {
            id: new_table_id,
            size: table_file.metadata()?.len() as _,
            level,
            range,
            filter,
            index,
        };

        let mut new_tables = new_tables;
        match new_tables.get_mut(level) {
            Some(tables) => {
                tables.insert(new_table_id, table_info);
            }
            None => {
                let mut tables = BTreeMap::new();
                tables.insert(new_table_id, table_info);
                new_tables.push(tables);
            }
        }

        Ok(new_tables)
    }
}
