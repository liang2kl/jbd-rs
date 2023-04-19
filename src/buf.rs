// TODO: jbd_alloc, jbd_free

use core::{alloc::Layout, any::Any, mem};
extern crate alloc;
use alloc::{boxed::Box, collections::VecDeque, sync::Arc, vec, vec::Vec};
use spin::Mutex;

use crate::{
    config::JFS_MIN_JOURNAL_BLOCKS,
    err::{JBDError, JBDResult},
};

pub const BUFFER_LRU_SIZE: usize = 32;

pub trait BlockDevice: Send + Sync + Any {
    /// Read data form block to buffer
    fn read_block(&self, block_id: usize, buf: &mut [u8]);
    /// Write data from buffer to block
    fn write_block(&self, block_id: usize, buf: &[u8]);
    /// Block size of the device
    fn block_size(&self) -> usize;
}

pub struct BufferHead {
    device: Arc<dyn BlockDevice>,
    size: usize,
    block_id: usize,
    dirty: bool,
    cache: *mut u8,
}

impl BufferHead {
    fn new(device: Arc<dyn BlockDevice>, size: usize, block_id: usize) -> Self {
        let layout = Layout::from_size_align(size, mem::align_of::<u8>()).unwrap();
        let cache = unsafe { alloc::alloc::alloc(layout) };
        Self {
            device,
            size,
            block_id,
            dirty: false,
            cache,
        }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn discard(&mut self) {
        self.dirty = false;
    }

    pub fn sync(&mut self) {
        if self.dirty {
            self.device.write_block(self.block_id, self.buf());
        }
        self.dirty = false;
    }

    pub fn buf(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.cache, self.size) }
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        self.mark_dirty();
        unsafe { core::slice::from_raw_parts_mut(self.cache, self.size) }
    }

    pub fn convert<T>(&self) -> &T {
        unsafe { &*(self.cache as *const T) }
    }

    pub fn convert_mut<T>(&mut self) -> &mut T {
        self.mark_dirty();
        unsafe { &mut *(self.cache as *mut T) }
    }
}

unsafe impl Send for BufferHead {}
unsafe impl Sync for BufferHead {}

impl Drop for BufferHead {
    fn drop(&mut self) {
        self.sync();
        let layout = Layout::from_size_align(self.size, mem::align_of::<u8>()).unwrap();
        unsafe { alloc::alloc::dealloc(self.cache, layout) };
    }
}

pub trait BufferProvider: Send + Sync + Any {
    fn get_buffer(&mut self, dev: Arc<dyn BlockDevice>, block_id: usize) -> JBDResult<Arc<Mutex<BufferHead>>>;
    fn sync(&mut self) -> JBDResult;
}

/// A default buffer provider.
pub struct DefaultBufferProvider {
    queue: VecDeque<Arc<Mutex<BufferHead>>>,
}

impl DefaultBufferProvider {
    pub fn new() -> Self {
        Self { queue: VecDeque::new() }
    }
}

impl BufferProvider for DefaultBufferProvider {
    fn get_buffer(&mut self, dev: Arc<dyn BlockDevice>, block_id: usize) -> JBDResult<Arc<Mutex<BufferHead>>> {
        if let Some(idx) = self
            .queue
            .iter()
            .enumerate()
            .find(|(_, buf)| buf.lock().block_id == block_id)
            .map(|(idx, _)| idx)
        {
            let buf = self.queue.remove(idx).unwrap();
            self.queue.push_back(buf.clone());
            Ok(buf.clone())
        } else {
            if self.queue.len() == BUFFER_LRU_SIZE {
                if let Some((idx, _)) = self
                    .queue
                    .iter()
                    .enumerate()
                    .find(|(_, buf)| Arc::strong_count(&buf) == 1)
                {
                    self.queue.drain(idx..=idx);
                } else {
                    return Err(JBDError::InsufficientCache);
                }
            }
            let mut buf = BufferHead::new(dev.clone(), dev.block_size(), block_id);
            dev.read_block(block_id, buf.buf_mut());

            let buf = Arc::new(Mutex::new(buf));
            self.queue.push_back(buf.clone());
            Ok(buf)
        }
    }

    fn sync(&mut self) -> JBDResult {
        for buf in self.queue.iter_mut() {
            buf.lock().sync();
        }

        Ok(())
    }
}
