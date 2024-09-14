use std::{fs::OpenOptions, io::Write, os::unix::fs::FileExt};

use super::IoManager;
use anyhow::Result;

pub struct FileIoManager {
    file: std::fs::File,
}

impl FileIoManager {
    pub fn new(file_name: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_name)?;
        Ok(Self { file: file })
    }
}

impl IoManager for FileIoManager {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        Ok(self.file.read_at(buf, offset)?)
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        Ok(self.file.write(buf)?)
    }

    fn size(&self) -> Result<u64> {
        let size = self.file.metadata()?.len();
        Ok(size)
    }

    fn sync(&mut self) -> Result<()> {
        Ok(self.file.sync_all()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{remove_file, File};

    #[test]
    fn test_file_io_manager_new() {
        let file_name = "/tmp/test_fnew.txt";
        let result = FileIoManager::new(file_name);
        assert!(result.is_ok());

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_file_io_manager_write() {
        let file_name = "/tmp/test_fwrite.txt";
        let mut io_manager = FileIoManager::new(file_name).unwrap();
        let data = b"Hello, test!";

        let bytes_written = io_manager.write(data).unwrap();
        assert_eq!(bytes_written, data.len());

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_file_io_manager_read() {
        let file_name = "/tmp/test_fread.txt";
        let mut file = File::create(file_name).unwrap();
        file.write_all(b"Hello, test!").unwrap();

        let io_manager = FileIoManager::new(file_name).unwrap();
        let mut buf = vec![0u8; 5];

        let bytes_read = io_manager.read(&mut buf, 0).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(&buf, b"Hello");

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_file_io_manager_size() {
        let file_name = "/tmp/test_fsize.txt";
        let mut file = File::create(file_name).unwrap();
        file.write_all(b"Hello, test!").unwrap();

        let io_manager = FileIoManager::new(file_name).unwrap();
        let size = io_manager.size().unwrap();
        assert_eq!(size, 12);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_file_io_manager_sync() {
        let file_name = "/tmp/test_fsync.txt";
        let mut io_manager = FileIoManager::new(file_name).unwrap();
        let data = b"data";

        io_manager.write(data).unwrap();

        let sync_result = io_manager.sync();
        assert!(sync_result.is_ok());

        remove_file(file_name).unwrap();
    }
}
