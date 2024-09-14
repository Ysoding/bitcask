use crate::data::LogRecordPos;

pub mod btree;

pub trait Indexer {
    fn put(&mut self, key: &[u8], pos: &LogRecordPos) -> Option<LogRecordPos>;
    fn get(&self, key: &[u8]) -> Option<LogRecordPos>;
    fn delete(&mut self, key: &[u8]) -> Option<LogRecordPos>;
    fn size(&self) -> usize;
}

pub enum IndexerType {
    BTree,
}

pub fn new_indexer(typ: IndexerType) -> Box<dyn Indexer> {
    match typ {
        IndexerType::BTree => Box::new(btree::BTree::new()),
    }
}
