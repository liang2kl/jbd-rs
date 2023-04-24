//! The system abstraction layer.
// TODO: jbd_alloc, jbd_free
use core::any::Any;
extern crate alloc;
use alloc::sync::Arc;
use spin::Mutex;

use crate::buffer::Buffer;

pub trait BlockDevice: Send + Sync + Any {
    /// Read data form block to buffer
    fn read_block(&self, block_id: usize, buf: &mut [u8]);
    /// Write data from buffer to block
    fn write_block(&self, block_id: usize, buf: &[u8]);
    /// Block size of the device
    fn block_size(&self) -> usize;
}

pub trait BufferProvider: Send + Sync + Any {
    fn get_buffer(&self, dev: Arc<dyn BlockDevice>, block_id: usize) -> Option<Arc<Mutex<Buffer>>>;
    fn sync(&self) -> bool;
}

pub trait WaitQueue {
    fn notify_one(&self);
    fn notify_all(&self);
}

pub trait System: Send + Sync + Any {
    fn get_buffer_provider(&self) -> Arc<dyn BufferProvider>;
    fn get_time(&self) -> usize;
}
