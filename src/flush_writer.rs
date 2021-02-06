use bloomfilter::Bloom;
use log::trace;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::data_utility;
use crate::fptree::leaf::Leaf;
use crate::sstable::sparse_index::SparseIndex;

// TODO: parameterize
const ITEMS_COUNT: usize = 1 << 13;
const WRITE_BUFFER: usize = 1 << 18;

pub struct FlushWriter {
    name: String,
    config: Config,
    table_id: usize,
}

impl FlushWriter {
    pub fn new(name: &str, config: Config, table_id: usize) -> Self {
        FlushWriter {
            name: name.to_string(),
            config,
            table_id,
        }
    }

    pub fn flush(
        &mut self,
        first_leaf: Arc<RwLock<Leaf>>,
    ) -> Result<(usize, Bloom<Vec<u8>>, SparseIndex), std::io::Error> {
        let mut locked_leaves = Vec::new();
        locked_leaves.push(first_leaf.clone());
        loop {
            let leaf = match locked_leaves
                .last()
                .unwrap()
                .read()
                .unwrap()
                .get_next_leaf()
            {
                Some(next) => next,
                None => break,
            };
            trace!(
                "starting to flush a leaf to SSTable {} - {}",
                self.table_id,
                leaf.read().unwrap().header
            );

            locked_leaves.push(leaf);
        }

        let mut filter = Bloom::new(self.config.get_bloom_filter_size(), ITEMS_COUNT);
        let mut index = SparseIndex::new();
        let mut offset = 0;
        let table_file = self.create_new_table()?;
        let mut writer = BufWriter::with_capacity(WRITE_BUFFER, &table_file);
        for locked_leaf in locked_leaves {
            let kv_pairs = locked_leaf.read().unwrap().get_sorted_kv_pairs()?;
            // it is enough to sort only kv_pairs since all leaves are ordered
            for (key, value, _) in kv_pairs {
                writer.write(&data_utility::format_data_with_crc(&key, &value))?;
                filter.set(&key);
                index.insert(&key, offset);

                offset += data_utility::get_data_size(key.len(), value.len());
            }
        }
        table_file.sync_all()?;

        let id = self.table_id;
        // odd ID used by compactions
        self.table_id += 2;

        Ok((id, filter, index))
    }

    fn create_new_table(&mut self) -> Result<File, std::io::Error> {
        let table_file_path = self.config.get_table_file_path(&self.name, self.table_id);
        let table_file = File::create(&table_file_path)?;

        Ok(table_file)
    }
}
