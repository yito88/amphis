use bloomfilter::Bloom;
use crossbeam_channel::Receiver;
use log::{debug, trace};
use mockall_double::double;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use crate::config::Config;
use crate::fptree::leaf_manager::NUM_SLOT;
use crate::fptree::Leaf;
use crate::fptree_manager::FPTreeManager;
use crate::sparse_index::SparseIndex;
use crate::sstable_manager::{SstableManager, TableId, TableInfo};
use crate::util::data_util;

#[double]
use crate::fptree::leaf_manager::LeafManager;

const WRITE_BUFFER_SIZE: usize = 1 << 18;

#[derive(Debug, Clone)]
pub enum FlushSignal {
    TryFlush,
    Shutdown,
}

pub fn spawn_flush_writer(
    mut flush_writer: FlushWriter,
    receiver: Receiver<FlushSignal>,
    fptree_manager: Arc<FPTreeManager>,
    sstable_manager: Arc<SstableManager>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        for signal in receiver {
            match signal {
                FlushSignal::TryFlush => {
                    // TODO: error handling
                    if let Some(first_leaf) = fptree_manager.prepare_flush().unwrap() {
                        let table_info = flush_writer.flush(first_leaf).unwrap();
                        sstable_manager.register(table_info).unwrap();
                        fptree_manager.switch_fptree().unwrap();
                    }
                }
                FlushSignal::Shutdown => break,
            }
        }
    })
}

pub struct FlushWriter {
    name: String,
    config: Config,
    table_id: TableId,
}

impl FlushWriter {
    pub fn new(name: &str, config: Config, table_id: usize) -> Self {
        FlushWriter {
            name: name.to_string(),
            config,
            table_id,
        }
    }

    /// flush the current tree
    pub fn flush(&mut self, first_leaf: Arc<RwLock<Leaf>>) -> Result<TableInfo, std::io::Error> {
        debug!(
            "Starting flush FPTree of {} to SSTable ID {}",
            self.name, self.table_id
        );
        let leaf_manager = first_leaf.read().unwrap().get_leaf_manager();
        let id_list = leaf_manager.read().unwrap().get_leaf_id_chain();
        trace!("leaf ID list: {:?}", id_list);

        self.flush_kv(leaf_manager, id_list)
    }

    /// flush all leaves in a leaf file
    pub fn flush_with_file(
        &mut self,
        name: &str,
        fptree_id: usize,
    ) -> Result<TableInfo, std::io::Error> {
        let leaf_manager = LeafManager::new(name, fptree_id, &self.config)?;
        let id_list = leaf_manager.get_leaf_id_chain();
        debug!("leaf ID list: {:?}", id_list);

        self.flush_kv(Arc::new(RwLock::new(leaf_manager)), id_list)
    }

    fn create_new_table(&mut self) -> Result<(TableId, File), std::io::Error> {
        let id = self.table_id;
        let table_file_path = self.config.get_table_file_path(&self.name, id);
        let table_file = File::create(table_file_path)?;

        // odd ID used by compactions
        self.table_id += 2;

        Ok((id, table_file))
    }

    fn flush_kv(
        &mut self,
        leaf_manager: Arc<RwLock<LeafManager>>,
        id_list: Vec<usize>,
    ) -> Result<TableInfo, std::io::Error> {
        let mut offset = 0;
        let (table_id, table_file) = self.create_new_table()?;
        let mut index = SparseIndex::new();
        let mut filter = Bloom::new_for_fp_rate(
            self.config.get_filter_items_count(),
            self.config.get_filter_fp_rate(),
        );
        let mut writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, &table_file);
        for id in id_list {
            let header = leaf_manager
                .read()
                .unwrap()
                .get_header(id)
                .expect("The header doesn't exist");
            let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(NUM_SLOT);
            for slot in 0..NUM_SLOT {
                if header.is_slot_set(slot) {
                    let (page_id, data_offset, key_size, value_size) = header.get_kv_info(slot);
                    let (key, value) = leaf_manager.read().unwrap().read_data(
                        page_id,
                        data_offset,
                        key_size,
                        value_size,
                    )?;
                    kv_pairs.push((key, value));
                }
            }
            // it is enough to sort only kv_pairs since all leaves are ordered
            kv_pairs.sort();
            for (key, value) in kv_pairs {
                filter.set(&key);
                index.insert(&key, offset);
                offset += data_util::get_data_size(key.len(), value.len());
                writer.write_all(&data_util::format_data_with_crc(&key, &value))?;
            }
        }
        table_file.sync_all()?;

        Ok(TableInfo {
            id: table_id,
            size: table_file.metadata()?.len() as _,
            level: 0,
            filter,
            index,
        })
    }
}
