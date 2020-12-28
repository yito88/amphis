//use crate::amphis_error::CrudError;
use crate::flush_writer::FlushWriter;
use crate::fptree::fptree::FPTree;
use log::trace;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::Weak;

use crate::config::Config;

pub struct KVS {
    config: Config,
    fptree: Arc<RwLock<FPTree>>,
    new_fptree: Option<Arc<RwLock<FPTree>>>,
    fptree_written: Arc<()>,
    flush_writer: Arc<FlushWriter>,
}

#[derive(Clone, Debug)]
struct CurrentTreeWritten {
    arc: Arc<()>,
    latches: Vec<Weak<()>>,
}

impl KVS {
    pub fn new(config: Config) -> Result<KVS, std::io::Error> {
        let fptree = Arc::new(RwLock::new(FPTree::new(&config)?));
        Ok(KVS {
            config: config.clone(),
            fptree: fptree.clone(),
            new_fptree: None,
            fptree_written: Arc::new(()),
            flush_writer: Arc::new(FlushWriter::new(config.clone(), fptree.clone(), 0)),
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Put K: {}, V: {}",
            String::from_utf8(key.clone()).unwrap(),
            String::from_utf8(value.clone()).unwrap()
        );

        match &self.new_fptree {
            Some(new_fptree) => new_fptree.read().unwrap().put(key, value)?,
            None => {
                let _written = self.fptree_written.clone();
                self.fptree.read().unwrap().put(key, value)?
            }
        };

        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        trace!(
            "Getting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        // TODO: concurrenct read
        if let Some(new_fptree) = &self.new_fptree {
            match new_fptree.read().unwrap().get(key)? {
                Some(val) => Ok(Some(val)),
                None => self.fptree.read().unwrap().get(key),
            }
        } else {
            self.fptree.read().unwrap().get(key)
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        trace!(
            "Deleting from K: {}",
            String::from_utf8(key.clone()).unwrap()
        );

        if let Some(new_fptree) = &self.new_fptree {
            new_fptree.read().unwrap().delete(key)?;
        }
        let _written = self.fptree_written.clone();
        self.fptree.read().unwrap().delete(key)
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.allocate_new_fptree()?;
        // check if other threads write data to FPTree
        if Arc::strong_count(&self.fptree_written) != 1 {
            return Ok(());
        }

        Ok(())
    }

    fn allocate_new_fptree(&mut self) -> Result<(), std::io::Error> {
        self.new_fptree = Some(Arc::new(RwLock::new(FPTree::new(&self.config)?)));

        Ok(())
    }
}
