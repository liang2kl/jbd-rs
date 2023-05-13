use std::{io, sync::Arc};

use jbd_rs::{
    sal::{BlockDevice, BufferProvider, System},
    Handle,
};
use spin::Mutex;

use self::{cache::BlockCacheManager, dev::FileDevice};

pub mod cache;
pub mod dev;

pub struct UserSystem {
    device: Arc<dyn BlockDevice>,
    cache_manager: Arc<Mutex<BlockCacheManager>>,
    current_handle: Mutex<Option<Arc<Mutex<Handle>>>>,
}

impl UserSystem {
    pub fn new(path: &str, nblocks: usize) -> Result<Self, io::Error> {
        let device = FileDevice::new(path, nblocks)?;
        let cache_manager = Arc::new(Mutex::new(BlockCacheManager::new()));
        Ok(Self {
            device: Arc::new(device),
            cache_manager,
            current_handle: Mutex::new(None),
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
    fn get_current_handle(&self) -> Option<Arc<Mutex<Handle>>> {
        self.current_handle.lock().clone()
    }
    fn set_current_handle(&self, handle: Option<Arc<Mutex<Handle>>>) {
        let mut this_handle = self.current_handle.lock();
        *this_handle = handle;
    }
}
