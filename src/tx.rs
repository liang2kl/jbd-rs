//! Transaction.
extern crate alloc;
use spin::{Mutex, MutexGuard};

use crate::{buffer::BufferHead, err::JBDResult, journal::Journal};
use alloc::{collections::LinkedList, sync::Arc, sync::Weak};

/// Transaction id.
pub type Tid = u16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TransactionState {
    Running,
    Locked,
    Flush,
    Commit,
    CommitRecord,
    Finished,
}

pub struct Transaction {
    /// Journal for this transaction [no locking]
    pub journal: Weak<Mutex<Journal>>,
    /// Sequence number for this transaction [no locking]
    pub tid: Tid,
    /// Transaction's current state
    /// [no locking - only kjournald alters this]
    pub state: TransactionState,
    /// Where in the log does this transaction's commit start? [no locking]
    pub log_start: u32,
    // TODO: j_list_lock
    /// Number of buffers on the t_buffers list [j_list_lock]
    pub nr_buffers: i32,
    /// Doubly-linked circular list of all buffers reserved but not yet
    /// modified by this transaction [j_list_lock]
    pub reserved_list: LinkedList<Arc<Mutex<BufferHead>>>,
    /// Doubly-linked circular list of all buffers under writeout during
    /// commit [j_list_lock]
    pub locked_list: LinkedList<Arc<Mutex<BufferHead>>>,
    /// Doubly-linked circular list of all metadata buffers owned by this
    /// transaction [j_list_lock]
    pub buffers: LinkedList<Arc<Mutex<BufferHead>>>,
    /// Doubly-linked circular list of all data buffers still to be
    /// flushed before this transaction can be committed [j_list_lock]
    pub sync_datalist: LinkedList<Arc<Mutex<BufferHead>>>,

    pub forget: LinkedList<Arc<Mutex<BufferHead>>>,
    pub checkpoint_list: LinkedList<Arc<Mutex<BufferHead>>>,
    pub checkpoint_io_list: LinkedList<Arc<Mutex<BufferHead>>>,
    pub iobuf_list: LinkedList<Arc<Mutex<BufferHead>>>,
    pub shadow_list: LinkedList<Arc<Mutex<BufferHead>>>,
    pub log_list: LinkedList<Arc<Mutex<BufferHead>>>,

    pub handle_info: Mutex<TransactionHandleInfo>,

    pub expires: usize,
    pub start_time: usize, // TODO: ktime_t
    pub handle_count: i32,
    pub synchronous_commit: bool,
}

impl Transaction {
    pub fn new(journal: Weak<Mutex<Journal>>) -> Self {
        todo!()
    }
}

/// Info related to handles, protected by handle_lock in Linux.
pub struct TransactionHandleInfo {
    pub updates: u32,
    pub outstanding_credits: u32,
    pub handle_count: u32,
}

/// Represents a single atomic update being performed by some process.
pub struct Handle {
    /// Which compound transaction is this update a part of?
    pub transaction: Option<Arc<Mutex<Transaction>>>,
    /// Number of remaining buffers we are allowed to dirty
    pub buffer_credits: u32,
    pub err: i32, // TODO

    /* Flags [no locking] */
    /// Sync-on-close
    pub sync: bool,
    /// Force data journaling
    pub jdata: bool,
    /// Fatal error on handle
    pub aborted: bool,
}

impl Handle {
    pub fn new(nblocks: u32) -> Self {
        Self {
            transaction: None,
            buffer_credits: nblocks,
            err: 0,
            sync: false,
            jdata: false,
            aborted: false,
        }
    }
}

impl Handle {
    pub fn get_write_access(&self, buffer: Arc<Mutex<BufferHead>>) -> JBDResult {
        todo!()
    }

    pub fn restart(&self, nblocks: i32) -> JBDResult {
        todo!()
    }

    pub fn extend(nblocks: i32) -> JBDResult {
        todo!()
    }

    // TODO: get_write_access, ...
}
