use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use super::{Indexer, LogRecordPos};

pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        let tree = Arc::new(RwLock::new(BTreeMap::new()));
        Self { tree }
    }
}

impl Indexer for BTree {
    fn put(&mut self, key: &[u8], pos: &LogRecordPos) -> Option<LogRecordPos> {
        let mut guard = self.tree.write().unwrap();
        guard.insert(key.to_vec(), pos.clone())
    }

    fn get(&self, key: &[u8]) -> Option<LogRecordPos> {
        let guard = self.tree.read().unwrap();
        guard.get(key).cloned()
    }

    fn delete(&mut self, key: &[u8]) -> Option<LogRecordPos> {
        let mut guard = self.tree.write().unwrap();
        guard.remove(key)
    }

    fn size(&self) -> usize {
        let guard = self.tree.read().unwrap();
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put() {
        let mut bt = BTree::new();
        let k1 = b"k1";
        let pos1 = LogRecordPos {
            file_id: 1,
            offset: 10,
            data_size: 100,
        };
        let pos2 = LogRecordPos {
            file_id: 2,
            offset: 20,
            data_size: 200,
        };

        let res = bt.put(k1, &pos1);
        assert!(res.is_none(), "should return NOne for new key");

        let res = bt.put(k1, &pos2);
        assert_eq!(res, Some(pos1.clone()));

        let res = bt.put(k1, &pos1);
        assert_eq!(res, Some(pos2.clone()));
    }

    #[test]
    fn test_get() {
        let mut bt = BTree::new();
        let k1 = b"k1";
        let pos1 = LogRecordPos {
            file_id: 1,
            offset: 10,
            data_size: 100,
        };

        bt.put(k1, &pos1);

        let res = bt.get(k1);
        assert_eq!(res, Some(pos1));
    }

    #[test]
    fn test_delete() {
        let mut bt = BTree::new();
        let key = b"k1";
        let pos = LogRecordPos {
            file_id: 1,
            offset: 10,
            data_size: 100,
        };

        bt.put(key, &pos);

        let result = bt.delete(key);
        assert_eq!(
            result,
            Some(pos.clone()),
            "Delete should return the deleted value"
        );

        let result = bt.get(key);
        assert!(result.is_none(), "Key should be removed after delete");
    }

    #[test]
    fn test_size() {
        let mut indexer = BTree::new();
        let key1 = b"k1";
        let key2 = b"k2";
        let pos = LogRecordPos {
            file_id: 1,
            offset: 10,
            data_size: 100,
        };

        assert_eq!(indexer.size(), 0, "Initial size should be 0");

        indexer.put(key1, &pos);
        assert_eq!(
            indexer.size(),
            1,
            "Size should be 1 after inserting 1 value"
        );

        indexer.put(key2, &pos);
        assert_eq!(
            indexer.size(),
            2,
            "Size should be 2 after inserting another value"
        );

        indexer.delete(key1);
        assert_eq!(indexer.size(), 1, "Size should be 1 after deleting 1 value");
    }
}
