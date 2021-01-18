use log::trace;
use std::sync::Arc;

//use crate::amphis_error::CrudError;
use crate::config::Config;
use crate::fptree::fptree_manager::FPTreeManager;
use crate::sstable::sstable_manager::SstableManager;

pub struct KVS {
    name: String,
    config: Config,
    fptree_manager: Arc<FPTreeManager>,
    sstable_manager: Arc<SstableManager>,
}

impl KVS {
    pub fn new(name: &str, config: Config) -> Result<KVS, std::io::Error> {
        // TODO: recovery (Flush all exsting FPTrees)
        Ok(KVS {
            name: name.to_string(),
            config: config.clone(),
            fptree_manager: Arc::new(FPTreeManager::new(name, config.clone())?),
            sstable_manager: Arc::new(SstableManager::new(name, config.clone())),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );

        self.fptree_manager.put(key, value)?;

        // TODO: make a flush process async
        if self.fptree_manager.need_flush() {
            if let Some((table_id, filter, index)) = self.fptree_manager.flush()? {
                self.sstable_manager.register_table(table_id, filter, index);
                self.fptree_manager.post_flush()?;
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        // TODO: concurrenct read
        let result = match self.fptree_manager.get(key)? {
            Some(r) => Some(r),
            None => self.sstable_manager.get(key)?,
        };

        Ok(result)
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        self.fptree_manager.delete(key)
    }
}
