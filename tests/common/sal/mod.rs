use std::{cell::RefCell, io, sync::Arc};

use self::{cache::BlockCacheManager, dev::FileDevice};
use jbd_rs::{
    sal::{BlockDevice, BufferProvider, System},
    Handle,
};

pub mod cache;
pub mod dev;

struct UserSystemInner {
    device: Arc<dyn BlockDevice>,
    cache_manager: Arc<BlockCacheManager>,
    current_handle: Option<Arc<RefCell<Handle>>>,
}

pub struct UserSystem {
    inner: RefCell<UserSystemInner>,
}

impl UserSystem {
    pub fn new(path: &str, nblocks: usize) -> Result<Self, io::Error> {
        let device = FileDevice::new(path, nblocks)?;
        let cache_manager = Arc::new(BlockCacheManager::new());
        Ok(Self {
            inner: RefCell::new(UserSystemInner {
                device: Arc::new(device),
                cache_manager,
                current_handle: None,
            }),
        })
    }

    pub fn block_device(&self) -> Arc<dyn BlockDevice> {
        self.inner.borrow().device.clone()
    }
}

impl System for UserSystem {
    fn get_buffer_provider(&self) -> Arc<dyn BufferProvider> {
        self.inner.borrow().cache_manager.clone()
    }
    fn get_time(&self) -> usize {
        // TODO
        0
    }
    fn get_current_handle(&self) -> Option<Arc<RefCell<Handle>>> {
        self.inner.borrow().current_handle.clone()
    }
    fn set_current_handle(&self, handle: Option<Arc<RefCell<Handle>>>) {
        self.inner.borrow_mut().current_handle = handle;
    }
}
