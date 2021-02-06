use log::trace;
use std::sync::Arc;
use std::sync::RwLock;

use super::node::Node;

// TODO: parameterize them
const FANOUT: usize = 3;

pub struct Inner {
    keys: Vec<Vec<u8>>,
    children: Vec<Arc<RwLock<dyn Node + Send + Sync>>>,
    next: Option<Arc<RwLock<dyn Node + Send + Sync>>>,
    is_root: bool,
}

impl std::fmt::Display for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "keys: {:?}", self.keys)
    }
}

impl Node for Inner {
    fn is_root(&self) -> bool {
        self.is_root
    }

    fn set_root(&mut self, is_root: bool) {
        self.is_root = is_root;
    }

    fn is_leaf(&self) -> bool {
        false
    }

    fn get_next(&self) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
        match &self.next {
            Some(arc) => Some(arc.clone()),
            None => None,
        }
    }

    fn get_child(&self, key: &Vec<u8>) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
        trace!("check an inner - {} by key {:?}", self, key);
        let child_idx = match self.keys.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        if child_idx == self.children.len() {
            self.get_next()
        } else {
            Some(self.children[child_idx].clone())
        }
    }

    fn may_need_split(&self, _key: &Vec<u8>, _value: &Vec<u8>) -> bool {
        self.keys.len() == FANOUT
    }

    fn insert(
        &mut self,
        key: &Vec<u8>,
        inserted_key: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut ret: Option<Vec<u8>> = None;
        let child = self.get_child(key).unwrap();
        let new_child = child.read().unwrap().get_next().unwrap();

        match self.keys.binary_search(&inserted_key) {
            Ok(_) => panic!("should not reach here"),
            Err(i) => {
                self.keys.insert(i, inserted_key.clone());
                if i + 1 >= self.children.len() {
                    self.children.push(new_child.clone());
                } else {
                    self.children.insert(i + 1, new_child.clone());
                }
            }
        }

        if self.need_split() {
            ret = Some(self.split()?);
        }

        Ok(ret)
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
        match self.get_child(key) {
            Some(c) => c.read().unwrap().get(key),
            None => Ok(None),
        }
    }

    fn split(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let new_keys = self.keys.split_off((FANOUT + 1) / 2);
        let split_key = new_keys.first().unwrap().clone();
        let new_children = self.children.split_off((FANOUT + 1) / 2 + 1);

        let mut new_inner = Inner::new();
        for i in 1..new_keys.len() {
            new_inner.add_key(&new_keys[i]);
        }
        for i in 0..new_children.len() {
            new_inner.add_child(new_children[i].clone());
        }
        if let Some(next) = self.get_next() {
            new_inner.next = Some(next.clone());
        }
        trace!("split existing inner: {}", self);
        trace!("new inner: {}", new_inner);
        trace!("split_key: {:?}", split_key.clone());
        self.next = Some(Arc::new(RwLock::new(new_inner)));

        Ok(split_key)
    }
}

impl Inner {
    pub fn new() -> Self {
        Inner {
            keys: Vec::with_capacity(FANOUT),
            next: None,
            children: Vec::with_capacity(FANOUT),
            is_root: false,
        }
    }

    fn need_split(&self) -> bool {
        self.keys.len() > FANOUT
    }

    pub fn add_key(&mut self, key: &Vec<u8>) {
        self.keys.push(key.clone());
    }

    pub fn add_child(&mut self, child: Arc<RwLock<dyn Node + Send + Sync>>) {
        self.children.push(child);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockLeaf {
        val: usize,
    }

    impl Node for MockLeaf {
        fn is_root(&self) -> bool {
            false
        }
        fn set_root(&mut self, _is_root: bool) {}
        fn is_leaf(&self) -> bool {
            true
        }
        fn get_next(&self) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
            Some(Arc::new(RwLock::new(MockLeaf { val: 0 })))
        }
        fn get_child(&self, _key: &Vec<u8>) -> Option<Arc<RwLock<dyn Node + Send + Sync>>> {
            None
        }
        fn insert(
            &mut self,
            _key: &Vec<u8>,
            _value: &Vec<u8>,
        ) -> Result<Option<Vec<u8>>, std::io::Error> {
            Ok(None)
        }
        fn may_need_split(&self, _key: &Vec<u8>, _value: &Vec<u8>) -> bool {
            false
        }
        fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, std::io::Error> {
            if *key == "key".as_bytes().to_vec() {
                Ok(Some(format!("value{}", self.val).as_bytes().to_vec()))
            } else {
                Ok(None)
            }
        }
        fn split(&mut self) -> Result<Vec<u8>, std::io::Error> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_get_next() {
        // a new inner doesn't have the next
        let mut inner = Inner::new();
        let not_exists = match inner.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);

        // added the next
        let new_inner: Arc<RwLock<dyn Node + Send + Sync>> = Arc::new(RwLock::new(Inner::new()));
        inner.next = Some(new_inner.clone());

        let next = inner.get_next().unwrap();
        assert!(Arc::ptr_eq(&next, &new_inner));
    }

    #[test]
    fn test_get_child() {
        let mut inner = Inner::new();
        inner.add_key(&vec![10 as u8]);

        let mut new_child1 = Inner::new();
        new_child1.add_key(&vec![1 as u8]);
        let arc_new_child1: Arc<RwLock<dyn Node + Send + Sync>> = Arc::new(RwLock::new(new_child1));
        inner.add_child(arc_new_child1.clone());

        let mut new_child2 = Inner::new();
        new_child2.add_key(&vec![11 as u8]);
        let arc_new_child2: Arc<RwLock<dyn Node + Send + Sync>> = Arc::new(RwLock::new(new_child2));
        inner.add_child(arc_new_child2.clone());

        let child1 = inner.get_child(&vec![0u8]).unwrap();
        assert!(Arc::ptr_eq(&child1, &arc_new_child1));
        let child2 = inner.get_child(&vec![11u8]).unwrap();
        assert!(Arc::ptr_eq(&child2, &arc_new_child2));
    }

    #[test]
    fn test_need_split() {
        let mut inner = Inner::new();
        assert_eq!(inner.need_split(), false);

        for i in 0..(FANOUT + 1) {
            inner.add_key(&vec![i as u8]);
        }
        assert_eq!(inner.need_split(), true);
    }

    #[test]
    fn test_insert() {
        let mut inner = Inner::new();
        let child1: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 1 }));
        let child2: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 2 }));
        let key0 = "key0".as_bytes().to_vec();
        let key2 = "key2".as_bytes().to_vec();
        inner.keys = vec![key0.clone(), key2.clone()];
        inner.children = vec![Arc::clone(&child1), Arc::clone(&child2)];

        let inserted = "key1".as_bytes().to_vec();
        inner.insert(&key0, &inserted).unwrap();

        let not_exists = match inner.get_next() {
            Some(_) => false,
            None => true,
        };
        assert!(not_exists);
        assert_eq!(inner.keys.len(), 3);
        assert_eq!(inner.keys[0], key0);
        assert_eq!(inner.keys[1], inserted);
        assert_eq!(inner.keys[2], key2);
    }

    #[test]
    fn test_get() {
        let mut inner = Inner::new();
        let child1: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 1 }));
        let child2: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 2 }));
        let k1 = "key1".as_bytes().to_vec();
        let k2 = "key2".as_bytes().to_vec();
        inner.keys = vec![k1, k2];
        inner.children = vec![child1.clone(), child2.clone()];

        let k = "key".as_bytes().to_vec();
        let result = inner.get(&k);

        assert_eq!(result.unwrap().unwrap(), "value1".as_bytes().to_vec());
    }

    #[test]
    fn test_split() {
        let mut inner = Inner::new();
        let child1: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 1 }));
        let child2: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 2 }));
        let child3: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 3 }));
        let child4: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 4 }));
        let child5: Arc<RwLock<dyn Node + Send + Sync>> =
            Arc::new(RwLock::new(MockLeaf { val: 5 }));
        let k1 = "key1".as_bytes().to_vec();
        let k2 = "key2".as_bytes().to_vec();
        let k3 = "key3".as_bytes().to_vec();
        let k4 = "key4".as_bytes().to_vec();
        inner.keys = vec![k1.clone(), k2.clone(), k3.clone(), k4.clone()];
        inner.children = vec![
            child1.clone(),
            child2.clone(),
            child3.clone(),
            child4.clone(),
            child5.clone(),
        ];

        let split_key = inner.split().unwrap();

        assert_eq!(split_key, k3);
        assert_eq!(inner.keys, vec!(k1.clone(), k2.clone()));
        if let Some(next) = inner.get_next() {
            assert!(Arc::ptr_eq(
                &next.read().unwrap().get_child(&split_key).unwrap(),
                &child4
            ));
            assert!(Arc::ptr_eq(
                &next.read().unwrap().get_child(&k4).unwrap(),
                &child5
            ));
        } else {
            panic!("the next inner should exist");
        }
    }
}
