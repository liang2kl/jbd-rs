use std::{io, sync::Arc};

use jbd_rs::sal::{BlockDevice, BufferProvider, System};
use spin::Mutex;

use self::{cache::BlockCacheManager, dev::FileDevice};

pub mod buf;
pub mod cache;
pub mod dev;

pub struct UserSystem {
    device: Arc<dyn BlockDevice>,
    cache_manager: Arc<Mutex<BlockCacheManager>>,
}

impl UserSystem {
    pub fn new(path: &str, nblocks: usize) -> Result<Self, io::Error> {
        let device = FileDevice::new(path, nblocks)?;
        let cache_manager = Arc::new(Mutex::new(BlockCacheManager::new()));
        Ok(Self {
            device: Arc::new(device),
            cache_manager,
        })
    }

    pub fn block_device(&self) -> Arc<dyn BlockDevice> {
        self.device.clone()
    }
}

impl System for UserSystem {
    fn get_buffer_provider(&self) -> Arc<Mutex<dyn BufferProvider>> {
        self.cache_manager.clone()
    }
    fn get_time(&self) -> usize {
        // TODO
        0
    }
}
