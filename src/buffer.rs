extern crate alloc;
use core::{alloc::Layout, mem};

use alloc::sync::Arc;

use crate::sal::BlockDevice;

pub struct Buffer {
    device: Arc<dyn BlockDevice>,
    block_id: usize,
    size: usize,
    dirty: bool,
    cache: *mut u8,
}

impl Buffer {
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn discard(&mut self) {
        self.dirty = false;
    }

    pub fn sync(&mut self) {
        if self.dirty {
            // FIXME: different block sizes?
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

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.sync();
        let layout = Layout::from_size_align(self.size, mem::align_of::<u8>()).unwrap();
        unsafe { alloc::alloc::dealloc(self.cache, layout) };
    }
}
