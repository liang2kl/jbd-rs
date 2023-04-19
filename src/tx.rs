//! Transaction.
extern crate alloc;
use spin::Mutex;

use alloc::{sync::Arc, collections::LinkedList};
use crate::{journal::Journal, err::JBDResult, buf::BufferHead};

/// Transaction id.
pub type Tid = u16;

pub enum TransactionState {
    Running,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

type BufferHeadList = LinkedList<Arc<BufferHead>>;

pub struct Transaction {
    /// Journal for this transaction [no locking]
    journal: Arc<Journal>,
    /// Sequence number for this transaction [no locking]
    tid: Tid,
    /// Transaction's current state
    /// [no locking - only kjournald alters this]
    state: TransactionState,
    /// Where in the log does this transaction's commit start? [no locking]
    log_start: u32,
    // TODO: j_list_lock
    /// Number of buffers on the t_buffers list [j_list_lock]
    nr_buffers: i32,
    /// Doubly-linked circular list of all buffers reserved but not yet
    /// modified by this transaction [j_list_lock]
    reserved_list: BufferHeadList,
    /// Doubly-linked circular list of all buffers under writeout during
	/// commit [j_list_lock]
    locked_list: BufferHeadList,
    /// Doubly-linked circular list of all metadata buffers owned by this
	/// transaction [j_list_lock]
    buffers: BufferHeadList,
    /// Doubly-linked circular list of all data buffers still to be
	/// flushed before this transaction can be committed [j_list_lock]
    sync_datalist: BufferHeadList,

    forget: BufferHeadList,
    checkpoint_list: BufferHeadList,
    checkpoint_io_list: BufferHeadList,
    iobuf_list: BufferHeadList,
    shadow_list: BufferHeadList,
    log_list: BufferHeadList,
    
    handle_info: Mutex<TransactionHandleInfo>,

    expires: usize,
    start_time: usize, // TODO: ktime_t
    handle_count: i32,
    synchronous_commit: bool,
}

/// Info related to handles, protected by handle_lock in Linux.
pub struct TransactionHandleInfo {
    updates: i32,
    outstanding_credits: i32,
}


/// Represents a single atomic update being performed by some process.
struct Handle {
    /// Which compound transaction is this update a part of?
    transaction: Arc<Transaction>,
    /// Number of remaining buffers we are allowed to dirty
    buffer_credits: i32,
    err: i32, // TODO

    /* Flags [no locking] */
    /// Sync-on-close
    sync: bool,
    /// Force data journaling
    jdata: bool,
    /// Fatal error on handle
    aborted: bool,
}

impl Handle {
    pub fn start(journal: Arc<Journal>, nblocks: i32) -> Arc<Handle> {
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
