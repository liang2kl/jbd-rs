//! The system abstraction layer.
// TODO: jbd_alloc, jbd_free
use core::any::Any;
extern crate alloc;
use alloc::{boxed::Box, sync::Arc};
use spin::Mutex;

use crate::tx::JournalBuffer;

pub trait BlockDevice: Send + Sync + Any {
    /// Read data form block to buffer
    fn read_block(&self, block_id: usize, buf: &mut [u8]);
    /// Write data from buffer to block
    fn write_block(&self, block_id: usize, buf: &[u8]);
    /// Block size of the device
    fn block_size(&self) -> usize;
}

pub trait Buffer: Send + Sync + Any {
    fn device(&self) -> Arc<dyn BlockDevice>;
    fn block_id(&self) -> usize;
    fn size(&self) -> usize;
    fn dirty(&self) -> bool;
    fn data(&self) -> *mut u8;
    fn private(&self) -> &Option<Box<dyn Any>>;
    fn set_private(&mut self, private: Option<Box<dyn Any>>);
    fn set_jdb_managed(&mut self, managed: bool);
    fn jdb_managed(&self) -> bool;
    fn lock_managed(&mut self);
    fn unlock_managed(&mut self);

    fn mark_dirty(&mut self);
    fn clear_dirty(&mut self);
    fn sync(&mut self);
}

impl dyn Buffer {
    pub(crate) fn buf(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.data(), self.size()) }
    }

    pub(crate) fn buf_mut(&mut self) -> &mut [u8] {
        self.mark_dirty();
        unsafe { core::slice::from_raw_parts_mut(self.data(), self.size()) }
    }

    pub(crate) fn convert<T>(&self) -> &T {
        unsafe { &*(self.data() as *const T) }
    }

    pub(crate) fn convert_mut<T>(&mut self) -> &mut T {
        self.mark_dirty();
        unsafe { &mut *(self.data() as *mut T) }
    }

    pub(crate) fn journal_buffer(&self) -> Option<Arc<Mutex<JournalBuffer>>> {
        self.private()
            .as_deref()?
            .downcast_ref()
            .map(|x: &Arc<Mutex<JournalBuffer>>| x.clone())
    }

    pub(crate) fn set_journal_buffer(&mut self, jb: Arc<Mutex<JournalBuffer>>) {
        self.set_jdb_managed(true);
        self.set_private(Some(Box::new(jb)));
    }
}

pub trait BufferProvider: Send + Sync + Any {
    fn get_buffer(&mut self, dev: Arc<dyn BlockDevice>, block_id: usize) -> Option<Arc<Mutex<dyn Buffer>>>;
    fn sync(&mut self) -> bool;
}

pub trait System: Send + Sync + Any {
    fn get_buffer_provider(&self) -> Arc<Mutex<dyn BufferProvider>>;
    fn get_time(&self) -> usize;
}
