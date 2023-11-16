use crossbeam_channel::Sender;
use log::{debug, error, info, trace};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

//use crate::amphis_error::CrudError;
use crate::config::Config;
use crate::flush_writer::{spawn_flush_writer, FlushSignal, FlushWriter};
use crate::fptree_manager::FPTreeManager;
use crate::sstable_manager::SstableManager;
use crate::util::file_util;

pub struct KVS {
    fptree_manager: Arc<FPTreeManager>,
    sstable_manager: Arc<SstableManager>,
    flush_writer_handle: Option<JoinHandle<()>>,
    sender: Sender<FlushSignal>,
}

impl KVS {
    pub fn new(name: &str, config: Config) -> Result<Self, std::io::Error> {
        let path = config.get_leaf_dir_path(name);
        let (tx, rx) = crossbeam_channel::unbounded::<FlushSignal>();

        let (sstable_manager, next_table_id) = SstableManager::new(name, config.clone())?;
        let sstable_manager = Arc::new(sstable_manager);

        let mut flush_writer = FlushWriter::new(name, config.clone(), next_table_id);
        if Path::new(&path).exists() {
            // flush the exsting trees
            for entry in std::fs::read_dir(path)? {
                if let Some(fptree_id) = file_util::get_tree_id(&entry?.path()) {
                    debug!("found FPTree ID: {}", fptree_id);
                    let table_info = flush_writer.flush_with_file(name, fptree_id)?;
                    sstable_manager.register(table_info)?;
                    let leaf_file = config.get_leaf_file_path(name, fptree_id);
                    std::fs::remove_file(leaf_file)?;
                }
            }
        }

        let fptree_manager = Arc::new(FPTreeManager::new(name, config.clone())?);

        let flush_writer_handle = spawn_flush_writer(
            flush_writer,
            rx,
            fptree_manager.clone(),
            sstable_manager.clone(),
        );
        info!("Amphis KVS has started: table {}", name);
        Ok(KVS {
            fptree_manager,
            sstable_manager,
            flush_writer_handle: Some(flush_writer_handle),
            sender: tx,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.to_vec()).unwrap(),
            String::from_utf8(value.to_vec()).unwrap()
        );

        self.fptree_manager.put(key, value)?;

        if self.fptree_manager.need_flush() {
            let _ = self.sender.send(FlushSignal::TryFlush);
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.to_vec()).unwrap()
        );

        // TODO: concurrenct read
        let result = match self.fptree_manager.get(key)? {
            Some(r) => Some(r),
            None => self.sstable_manager.get(key)?,
        };

        match result {
            Some(v) => {
                if v.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(v))
                }
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.to_vec()).unwrap()
        );

        self.fptree_manager.delete(key)
    }
}

impl Drop for KVS {
    fn drop(&mut self) {
        info!("Wait for the flushing for shutting down...");
        let _ = self.sender.send(FlushSignal::Shutdown);
        if let Some(handle) = self.flush_writer_handle.take() {
            if let Err(e) = handle.join() {
                error!("FlushWrite failed to shut down: {e:?}");
            }
        }
        info!("Shutdown gracefully");
    }
}
