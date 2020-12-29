use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::Arc;
use std::sync::RwLock;

use crate::config::Config;
use crate::data_utility;
use crate::fptree::fptree::FPTree;

pub struct FlushWriter {
    config: Config,
    fptree: Arc<RwLock<FPTree>>,
    table_index: usize,
}

impl FlushWriter {
    pub fn new(config: Config, fptree: Arc<RwLock<FPTree>>, table_index: usize) -> Self {
        FlushWriter {
            config,
            fptree,
            table_index,
        }
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        let first_leaf = self.fptree.read().unwrap().get_first_leaf();
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

            locked_leaves.push(leaf);
        }

        let table_file = self.create_new_table()?;
        let mut writer = BufWriter::new(&table_file);
        for locked_leaf in locked_leaves {
            let kv_pairs = locked_leaf.read().unwrap().get_sorted_kv_pairs()?;
            // it is enough to sort only kv_pairs since all leaves are ordered
            for pair in kv_pairs {
                writer.write(&data_utility::format_data_with_crc(&pair.0, &pair.1))?;
            }
        }
        table_file.sync_all()?;

        Ok(())
    }

    fn create_new_table(&mut self) -> Result<File, std::io::Error> {
        let table_file_path = format!(
            "{}{}{}",
            "amphis_table",
            self.table_index.to_string(),
            ".amph"
        );
        let table_file = File::create(&table_file_path)?;
        self.table_index += 1;

        Ok(table_file)
    }
}
