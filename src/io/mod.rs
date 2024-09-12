use anyhow::Result;
use file_io::FileIoManager;
use mmap::MMapIOManager;

pub mod file_io;
pub mod mmap;

pub trait IoManager {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn size(&self) -> Result<u64>;
    fn sync(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub enum IoType {
    StandardFIO,
    MMap,
}

pub fn new_io_manager(file_name: &str, io_type: IoType) -> Result<Box<dyn IoManager>> {
    match io_type {
        IoType::StandardFIO => Ok(Box::new(FileIoManager::new(file_name)?)),
        IoType::MMap => Ok(Box::new(MMapIOManager::new(file_name)?)),
    }
}
