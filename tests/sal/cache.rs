use jbd_rs::sal::{BlockDevice, Buffer, BufferProvider};
use spin::Mutex;
use std::{
    alloc::{alloc, Layout},
    any::Any,
    collections::VecDeque,
    slice,
    sync::Arc,
};

use super::dev;

const BLOCK_CACHE_SIZE: usize = 16;

struct BlockCache {
    device: Arc<dyn BlockDevice>,
    block_id: usize,
    size: usize,
    dirty: bool,
    data: *mut u8,
    private: Option<Box<dyn Any>>,
    jdb_managed: bool,
}

unsafe impl Sync for BlockCache {}
unsafe impl Send for BlockCache {}

impl BlockCache {
    pub fn new(block_id: usize, size: usize, device: Arc<dyn BlockDevice>) -> Self {
        let data = unsafe { alloc(Layout::from_size_align(size, 8).unwrap()) };
        device.read_block(block_id, unsafe { slice::from_raw_parts_mut(data, size) });
        Self {
            device,
            block_id,
            size,
            dirty: false,
            data,
            private: None,
            jdb_managed: false,
        }
    }
}

impl Buffer for BlockCache {
    fn device(&self) -> Arc<dyn BlockDevice> {
        self.device.clone()
    }

    fn block_id(&self) -> usize {
        self.block_id
    }

    fn size(&self) -> usize {
        self.size
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn data(&self) -> *mut u8 {
        self.data
    }

    fn private(&self) -> &Option<Box<dyn Any>> {
        &self.private
    }

    fn set_private(&mut self, private: Option<Box<dyn Any>>) {
        self.private = private;
    }

    fn set_jdb_managed(&mut self, managed: bool) {
        self.jdb_managed = managed;
    }

    fn jdb_managed(&self) -> bool {
        self.jdb_managed
    }

    fn lock_managed(&mut self) {}

    fn unlock_managed(&mut self) {}

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    fn sync(&mut self) {
        println!("sync block {}", self.block_id);
        if self.dirty {
            unsafe {
                self.device
                    .write_block(self.block_id, slice::from_raw_parts_mut(self.data, self.size));
            }
        }
        self.clear_dirty();
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        self.sync();
    }
}

pub struct BlockCacheManager {
    queue: VecDeque<(usize, Arc<Mutex<dyn Buffer>>)>,
}

impl BlockCacheManager {
    pub fn new() -> Self {
        Self { queue: VecDeque::new() }
    }
}

impl BufferProvider for BlockCacheManager {
    fn get_buffer(&mut self, dev: Arc<dyn BlockDevice>, block_id: usize) -> Option<Arc<Mutex<dyn Buffer>>> {
        println!("get buffer {}", block_id);
        if let Some(pair) = self.queue.iter().find(|pair| pair.0 == block_id) {
            Some(Arc::clone(&pair.1))
        } else {
            // substitute
            if self.queue.len() == BLOCK_CACHE_SIZE {
                // from front to tail
                if let Some((idx, _)) = self
                    .queue
                    .iter()
                    .enumerate()
                    .find(|(_, pair)| Arc::strong_count(&pair.1) == 1)
                {
                    self.queue.drain(idx..=idx);
                } else {
                    panic!("Run out of BlockCache!");
                }
            }
            // load block into mem and push back
            let block_cache = BlockCache::new(block_id, dev.block_size(), Arc::clone(&dev));
            let block_cache: Arc<Mutex<dyn Buffer>> = Arc::new(Mutex::new(block_cache));
            self.queue.push_back((block_id, Arc::clone(&block_cache)));
            Some(block_cache)
        }
    }

    fn sync(&mut self) -> bool {
        for (_, buf) in self.queue.iter() {
            let mut buf = buf.lock();
            buf.sync();
        }
        true
    }
}
