use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};

use crate::{
    errors::Errors,
    io::{self, IOType, IoManager},
};
use std::{fmt::Write, path::PathBuf};

const DATA_FILE_NAME_SUFFIX: &str = ".data";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogRecordStatus {
    Normal = 1,
    Deleted = 2,
}

impl Default for LogRecordStatus {
    fn default() -> Self {
        LogRecordStatus::Normal
    }
}

impl From<u8> for LogRecordStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => LogRecordStatus::Normal,
            2 => LogRecordStatus::Deleted,
            _ => panic!("Unknown log record status"),
        }
    }
}

#[derive(Default, Debug)]
pub struct LogRecordHeader {
    pub crc: u32,
    pub status: LogRecordStatus,
    pub key_size: usize,
    pub val_size: usize,
}

#[derive(Default, PartialEq, Eq, Debug)]
pub struct LogRecord {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub status: LogRecordStatus,
}

impl LogRecord {
    fn max_log_record_header_size() -> usize {
        // crc type keySize valueSize
        // 4 +  1  +    +
        4 + 1 + length_delimiter_len(std::u32::MAX as usize) * 2
    }

    //	+-------------+-------------+-------------+--------------+-------------+--------------+
    //	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
    //	+-------------+-------------+-------------+--------------+-------------+--------------+
    //	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
    pub fn encode(&self) -> Vec<u8> {
        self.encode_ret_crc().0
    }

    pub fn crc(&self) -> u32 {
        self.encode_ret_crc().1
    }

    fn encode_ret_crc(&self) -> (Vec<u8>, u32) {
        let mut buffer = BytesMut::new();
        buffer.reserve(self.encoded_length());

        buffer.put_u32(0); // crc
        buffer.put_u8(self.status as u8);

        encode_length_delimiter(self.key.len(), &mut buffer).unwrap();
        encode_length_delimiter(self.val.len(), &mut buffer).unwrap();

        buffer.extend_from_slice(&self.key);
        buffer.extend_from_slice(&self.val);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buffer[4..]);

        let crc = hasher.finalize();
        buffer[0..4].copy_from_slice(&crc.to_le_bytes());

        (buffer.to_vec(), crc)
    }

    // wihtout key/value
    fn encoded_length(&self) -> usize {
        4 + 1
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.val.len())
            + self.key.len()
            + self.val.len()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct LogRecordPos {
    pub file_id: u32,
    pub offset: u32,
    pub data_size: u32,
}

impl LogRecordPos {
    pub fn encode() -> Vec<u8> {
        todo!()
    }
}

pub struct DataFile {
    pub file_id: u32,
    pub write_offset: u32,
    pub io_manager: Box<dyn IoManager>,
}

impl DataFile {
    pub fn new(dir_path: &str, file_id: u32, io_type: IOType) -> Result<Self> {
        let io_manager = io::new_io_manager(&Self::get_file_name(dir_path, file_id), io_type)?;
        Ok(DataFile {
            file_id,
            io_manager,
            write_offset: 0,
        })
    }

    pub fn get_file_name(dir_path: &str, file_id: u32) -> String {
        let mut file_name = String::new();
        write!(&mut file_name, "{:09}", file_id).unwrap();

        file_name.push_str(DATA_FILE_NAME_SUFFIX);

        let mut path = PathBuf::from(dir_path);
        path.push(file_name);

        path.to_string_lossy().into_owned()
    }

    pub fn write(&mut self, lg: &LogRecord) -> Result<usize> {
        let size = self.io_manager.write(&lg.encode())?;
        self.write_offset += size as u32;
        Ok(size)
    }

    pub fn read(&mut self, offset: u64) -> Result<(LogRecord, usize)> {
        let mut header_buf = BytesMut::zeroed(LogRecord::max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;

        let mut log_record_header = LogRecordHeader::default();

        log_record_header.crc = header_buf.get_u32_le();
        log_record_header.status = header_buf.get_u8().into();

        log_record_header.key_size = decode_length_delimiter(&mut header_buf).unwrap();
        log_record_header.val_size = decode_length_delimiter(&mut header_buf).unwrap();

        if log_record_header.key_size == 0 || log_record_header.val_size == 0 {
            return Err(Errors::ReadDataFileEOF.into());
        }

        let actual_header_size = length_delimiter_len(log_record_header.key_size)
            + length_delimiter_len(log_record_header.val_size)
            + 1
            + 4;

        let mut log_record = LogRecord::default();
        let mut buf = BytesMut::zeroed(log_record_header.key_size + log_record_header.val_size);
        self.io_manager
            .read(&mut buf, offset + actual_header_size as u64)?;

        log_record.key = buf.get(..log_record_header.key_size).unwrap().to_vec();
        log_record.val = buf.get(log_record_header.key_size..).unwrap().to_vec();

        log_record.status = log_record_header.status;

        if log_record_header.crc != log_record.crc() {
            return Err(Errors::InvalidLogRecordCRC.into());
        }

        let log_record_size =
            actual_header_size + log_record_header.key_size + log_record_header.val_size;
        Ok((log_record, log_record_size))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    pub fn test_data_file() {
        let t = env::temp_dir();
        let tmp_path = t.to_str().unwrap();

        let mut df = DataFile::new(tmp_path, 666, IOType::StandardFIO).unwrap_or_else(|e| {
            panic!("DataFile::new failed with error: {:?}", e);
        });

        let lg = LogRecord {
            key: "key".into(),
            val: "val".into(),
            status: LogRecordStatus::Normal,
        };

        let write_size = df.write(&lg).unwrap_or_else(|e| {
            panic!("Write failed with error: {:?}", e);
        });

        let (read_lg, read_size) = df.read(0).unwrap_or_else(|e| {
            panic!("Read failed with error: {:?}", e);
        });
        assert_eq!(write_size, read_size);
        assert_eq!(lg, read_lg, "The written and read log records do not match");
    }
}
