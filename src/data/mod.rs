pub struct LogRecord {}

#[derive(Clone, PartialEq, Debug)]
pub struct LogRecordPos {
    pub file_id: u32,
    pub offset: i64,
    pub data_size: u32,
}
