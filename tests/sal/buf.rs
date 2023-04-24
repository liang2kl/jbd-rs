use std::{any::Any, slice, sync::Arc};

use jbd_rs::sal::{self, BlockDevice};

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

impl sal::Buffer for BlockCache {
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
        unsafe {
            self.device
                .write_block(self.block_id, slice::from_raw_parts_mut(self.data, self.size));
        }
        self.clear_dirty();
    }
}
