use jbd_rs::sal::{BlockDevice, Buffer, BufferProvider};
use spin::Mutex;
use std::{
    alloc::{self, Layout},
    any::Any,
    collections::VecDeque,
    slice,
    sync::Arc,
};

const BLOCK_CACHE_SIZE: usize = 16;

struct BlockCache {
    device: Arc<dyn BlockDevice>,
    block_id: usize,
    size: usize,
    dirty: bool,
    data: *mut u8,
    private: Option<Box<dyn Any>>,
    jbd_managed: bool,
    jbd_dirty: bool,
}

unsafe impl Sync for BlockCache {}
unsafe impl Send for BlockCache {}

impl BlockCache {
    pub fn new(block_id: usize, size: usize, device: Arc<dyn BlockDevice>) -> Self {
        let data = unsafe { alloc::alloc(Layout::from_size_align(size, 8).unwrap()) };
        device.read_block(block_id, unsafe { slice::from_raw_parts_mut(data, size) });
        Self {
            device,
            block_id,
            size,
            dirty: false,
            data,
            private: None,
            jbd_managed: false,
            jbd_dirty: false,
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

    fn set_jbd_managed(&mut self, managed: bool) {
        self.jbd_managed = managed;
    }

    fn jbd_managed(&self) -> bool {
        self.jbd_managed
    }

    fn lock_jbd(&mut self) {}

    fn unlock_jbd(&mut self) {}

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    fn sync(&mut self) {
        if self.dirty {
            unsafe {
                self.device
                    .write_block(self.block_id, slice::from_raw_parts_mut(self.data, self.size));
            }
        }
        self.clear_dirty();
    }

    fn mark_jbd_dirty(&mut self) {
        self.jbd_dirty = true;
    }

    fn clear_jbd_dirty(&mut self) {
        self.jbd_dirty = false;
    }

    fn jbd_dirty(&self) -> bool {
        self.jbd_dirty
    }

    fn test_clear_dirty(&mut self) -> bool {
        let ret = self.dirty;
        self.clear_dirty();
        ret
    }

    fn test_clear_jbd_dirty(&mut self) -> bool {
        let ret = self.jbd_dirty;
        self.clear_jbd_dirty();
        ret
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data, Layout::from_size_align(self.size, 8).unwrap());
        }
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
                    return None;
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
