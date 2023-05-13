//! The system abstraction layer.
// TODO: jbd_alloc, jbd_free
use core::{any::Any, marker::PhantomData};
extern crate alloc;
use alloc::{boxed::Box, sync::Arc};
use spin::Mutex;

use crate::tx::{Handle, JournalBuffer};

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

    // Related methods of the `private` field of `struct buffer_head`
    fn private(&self) -> &Option<Box<dyn Any>>;
    fn set_private(&mut self, private: Option<Box<dyn Any>>);

    // Normal writeback control. JBD might alter the related states
    // to control writeback behaviours.
    fn mark_dirty(&mut self);
    fn clear_dirty(&mut self);
    fn test_clear_dirty(&mut self) -> bool;
    fn sync(&mut self);

    // JBD-specific state management. The related states should only
    // be altered by JBD.
    fn jbd_managed(&self) -> bool;
    fn set_jbd_managed(&mut self, managed: bool);
    fn lock_jbd(&mut self);
    fn unlock_jbd(&mut self);
    fn mark_jbd_dirty(&mut self);
    fn clear_jbd_dirty(&mut self);
    fn test_clear_jbd_dirty(&mut self) -> bool;
    fn jbd_dirty(&self) -> bool;
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
        self.set_jbd_managed(true);
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
    fn get_current_handle(&self) -> Option<Arc<Mutex<Handle>>>;
    fn set_current_handle(&self, handle: Option<Arc<Mutex<Handle>>>);
}
