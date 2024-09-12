use std::fs::OpenOptions;

use anyhow::{anyhow, Ok, Result};
use memmap2::Mmap;

use super::IoManager;

pub struct MMapIOManager {
    mmap: Mmap,
}

impl MMapIOManager {
    pub fn new(file_name: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)?;

        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap })
    }
}

impl IoManager for MMapIOManager {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let end = offset as usize + buf.len();

        if let Some(data) = self.mmap.get(offset as usize..end) {
            buf.copy_from_slice(data);
            Ok(data.len())
        } else {
            Err(anyhow!("Out of bounds"))
        }
    }

    fn write(&mut self, _: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn size(&self) -> Result<u64> {
        Ok(self.mmap.len() as u64)
    }

    fn sync(&mut self) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{remove_file, File},
        io::Write,
    };

    use super::*;

    #[test]
    fn test_new() {
        let file_name = "/tmp/test_mmap_new.txt";
        let mio = MMapIOManager::new(&file_name);
        assert!(mio.is_ok());

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read() {
        let file_name = "/tmp/test_mmap_read.txt";
        let mut file = File::create(file_name).unwrap();
        file.write_all(b"Hello, test!").unwrap();
        file.sync_all().unwrap();

        let mut mio = MMapIOManager::new(&file_name).unwrap();
        assert_eq!(mio.size().unwrap(), 12);
        let mut buf = vec![0u8; 5];
        let bytes_read = mio.read(&mut buf, 7).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(buf, b"test!");

        remove_file(file_name).unwrap();
    }
}
