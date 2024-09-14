use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("read data file end")]
    ReadDataFileEOF,

    #[error("invalid log record crc value")]
    InvalidLogRecordCRC,
}
