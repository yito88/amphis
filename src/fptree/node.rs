use std::sync::Arc;
use std::sync::RwLock;

pub trait Node {
    fn is_root(&self) -> bool;
    fn set_root(&mut self, is_root: bool);
    fn is_leaf(&self) -> bool;
    fn get_next(&self) -> Option<Arc<RwLock<dyn Node + Send + Sync>>>;
    fn get_child(&self, key: &Vec<u8>) -> Option<Arc<RwLock<dyn Node + Send + Sync>>>;
    fn may_need_split(&self) -> bool;
    fn insert(&mut self, key: &Vec<u8>, value: &Vec<u8>)
        -> Result<Option<Vec<u8>>, std::io::Error>;
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error>;
    fn split(&mut self) -> Result<Vec<u8>, std::io::Error>;
    fn commit(&self) -> Result<(), std::io::Error>;
}
