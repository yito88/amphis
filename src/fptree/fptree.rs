use log::trace;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;

use super::inner::Inner;
use super::leaf::Leaf;
cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::fptree::leaf_manager::MockLeafManager as LeafManager;
    } else {
        use crate::fptree::leaf_manager::LeafManager;
    }
}
use super::node::Node;
use crate::config::Config;

pub struct FPTree {
    root_ptr: Arc<RwLock<Arc<RwLock<dyn Node + Send + Sync>>>>,
    first_leaf: Arc<RwLock<Leaf>>,
    mutex: Arc<Mutex<usize>>,
}

impl FPTree {
    pub fn new(name: &str, id: usize, config: &Config) -> Result<Self, std::io::Error> {
        let leaf_manager = Arc::new(RwLock::new(LeafManager::new(name, id, config)?));
        // TODO: recovery
        let first_leaf = Arc::new(RwLock::new(Leaf::new(leaf_manager).unwrap()));
        first_leaf.write().unwrap().set_root(true);

        Ok(FPTree {
            root_ptr: Arc::new(RwLock::new(first_leaf.clone())),
            mutex: Arc::new(Mutex::new(0)),
            first_leaf: first_leaf,
        })
    }

    pub fn get_first_leaf(&self) -> Arc<RwLock<Leaf>> {
        self.first_leaf.clone()
    }

    fn split_root(
        &self,
        key: &Vec<u8>,
        mut root_lock: RwLockWriteGuard<Arc<RwLock<dyn Node + Send + Sync>>>,
        locked_new_child: Arc<RwLock<dyn Node + Send + Sync>>,
    ) {
        trace!("Root split: {:?}", key);
        let mut new_root = Inner::new();
        new_root.set_root(true);
        new_root.add_key(key);
        new_root.add_child(root_lock.clone());
        new_root.add_child(locked_new_child.clone());
        *root_lock = Arc::new(RwLock::new(new_root));
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        // Lock the pointer to the root since it might be updated
        let root_lock = self.root_ptr.write().unwrap();

        // Phase1: Acquire locks of nodes atomically
        let lock = self.mutex.lock().unwrap();
        let mut nodes = Vec::new();
        nodes.push(root_lock.clone());
        loop {
            let index = nodes.len() - 1;
            if nodes[index].read().unwrap().is_leaf() {
                break;
            }

            let child = nodes[index].read().unwrap().get_child(key).unwrap();
            nodes.push(child.clone());
        }

        let mut locked_nodes = Vec::new();
        let mut is_root_locked = true;
        for i in 0..nodes.len() {
            let locked_node = nodes[i].write().unwrap();
            if !locked_node.may_need_split(key, value) {
                is_root_locked = false;
                locked_nodes.clear();
            }
            locked_nodes.push(locked_node);
        }
        drop(lock);

        // Phase2: Insert split keys and a value
        let mut inserted = value.clone();
        if is_root_locked {
            while let Some(mut locked_node) = locked_nodes.pop() {
                if let Some(split_key) = locked_node.insert(key, &inserted)? {
                    inserted = split_key.clone();
                    if locked_node.is_root() {
                        locked_node.set_root(false);
                        let new_child = locked_node.get_next().unwrap();
                        self.split_root(&inserted, root_lock, new_child);
                        return Ok(Some(inserted));
                    }
                } else {
                    break;
                }
            }
        } else {
            drop(root_lock);
            while let Some(mut locked_node) = locked_nodes.pop() {
                if let Some(split_key) = locked_node.insert(key, &inserted)? {
                    inserted = split_key.clone();
                } else {
                    break;
                }
            }
        }

        Ok(None)
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut node = self.root_ptr.read().unwrap().clone();
        loop {
            let n = node.clone();
            let node_guard = n.read().unwrap();
            if node_guard.is_leaf() {
                return node_guard.get(key);
            }

            node = node_guard.get_child(key).unwrap().clone();
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), std::io::Error> {
        let mut node = self.root_ptr.read().unwrap().clone();
        loop {
            let n = node.clone();
            let mut node_guard = n.write().unwrap();
            if node_guard.is_leaf() {
                return node_guard.delete(key);
            }

            node = node_guard.get_child(key).unwrap().clone();
        }
    }
}
