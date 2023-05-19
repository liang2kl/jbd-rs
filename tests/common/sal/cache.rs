use jbd_rs::sal::{BlockDevice, Buffer, BufferProvider};
use std::{
    alloc::{self, Layout},
    any::Any,
    cell::{Ref, RefCell, RefMut},
    collections::VecDeque,
    rc::Rc,
    slice,
};

const BLOCK_CACHE_SIZE: usize = 16;

struct BlockCacheInner {
    device: Rc<dyn BlockDevice>,
    block_id: usize,
    size: usize,
    dirty: bool,
    data: *mut u8,
    private: Option<Box<dyn Any>>,
    jbd_managed: bool,
    jbd_dirty: bool,
}

struct BlockCache {
    inner: RefCell<BlockCacheInner>,
}

unsafe impl Sync for BlockCache {}
unsafe impl Send for BlockCache {}

impl BlockCache {
    pub fn new(block_id: usize, size: usize, device: Rc<dyn BlockDevice>) -> Self {
        let data = unsafe { alloc::alloc(Layout::from_size_align(size, 8).unwrap()) };
        device.read_block(block_id, unsafe { slice::from_raw_parts_mut(data, size) });
        Self {
            inner: RefCell::new(BlockCacheInner {
                device,
                block_id,
                size,
                dirty: false,
                data,
                private: None,
                jbd_managed: false,
                jbd_dirty: false,
            }),
        }
    }
}

impl BlockCache {
    fn inner_mut(&self) -> RefMut<BlockCacheInner> {
        self.inner.borrow_mut()
    }

    fn inner(&self) -> Ref<BlockCacheInner> {
        self.inner.borrow()
    }
}

impl Buffer for BlockCache {
    fn device(&self) -> Rc<dyn BlockDevice> {
        self.inner_mut().device.clone()
    }

    fn block_id(&self) -> usize {
        self.inner().block_id
    }

    fn size(&self) -> usize {
        self.inner().size
    }

    fn dirty(&self) -> bool {
        self.inner().dirty
    }

    fn data(&self) -> *mut u8 {
        self.inner().data
    }

    fn private(&self) -> &Option<Box<dyn Any>> {
        unsafe { &*(&self.inner().private as *const _) }
    }

    fn set_private(&self, private: Option<Box<dyn Any>>) {
        self.inner_mut().private = private;
    }

    fn set_jbd_managed(&self, managed: bool) {
        self.inner_mut().jbd_managed = managed;
    }

    fn jbd_managed(&self) -> bool {
        self.inner().jbd_managed
    }

    fn mark_dirty(&self) {
        self.inner_mut().dirty = true;
    }

    fn clear_dirty(&self) {
        self.inner_mut().dirty = false;
    }

    fn sync(&self) {
        let inner = self.inner();
        if inner.dirty {
            unsafe {
                inner
                    .device
                    .write_block(inner.block_id, slice::from_raw_parts_mut(inner.data, inner.size));
            }
        }
        drop(inner);
        self.clear_dirty();
    }

    fn mark_jbd_dirty(&self) {
        self.inner_mut().jbd_dirty = true;
    }

    fn clear_jbd_dirty(&self) {
        self.inner_mut().jbd_dirty = false;
    }

    fn jbd_dirty(&self) -> bool {
        self.inner().jbd_dirty
    }

    fn test_clear_dirty(&self) -> bool {
        let ret = self.inner().dirty;
        self.clear_dirty();
        ret
    }

    fn test_clear_jbd_dirty(&self) -> bool {
        let ret = self.inner().jbd_dirty;
        self.clear_jbd_dirty();
        ret
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(
                self.inner().data,
                Layout::from_size_align(self.inner().size, 8).unwrap(),
            );
        }
        self.sync();
    }
}

pub struct BlockCacheManager {
    queue: RefCell<VecDeque<(usize, Rc<dyn Buffer>)>>,
}

impl BlockCacheManager {
    pub fn new() -> Self {
        Self {
            queue: RefCell::new(VecDeque::new()),
        }
    }
}

impl BufferProvider for BlockCacheManager {
    fn get_buffer(&self, dev: Rc<dyn BlockDevice>, block_id: usize) -> Option<Rc<dyn Buffer>> {
        let mut queue = self.queue.borrow_mut();
        if let Some(pair) = queue.iter().find(|pair| pair.0 == block_id) {
            Some(Rc::clone(&pair.1))
        } else {
            // substitute
            if queue.len() == BLOCK_CACHE_SIZE {
                // from front to tail
                if let Some((idx, _)) = queue
                    .iter()
                    .enumerate()
                    .find(|(_, pair)| Rc::strong_count(&pair.1) == 1)
                {
                    queue.drain(idx..=idx);
                } else {
                    return None;
                }
            }
            // load block into mem and push back
            let block_cache = BlockCache::new(block_id, dev.block_size(), Rc::clone(&dev));
            let block_cache: Rc<dyn Buffer> = Rc::new(block_cache);
            queue.push_back((block_id, Rc::clone(&block_cache)));
            Some(block_cache)
        }
    }

    fn sync(&self) -> bool {
        let queue = self.queue.borrow();
        for (_, buf) in queue.iter() {
            buf.sync();
        }
        true
    }
}
