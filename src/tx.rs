//! Transaction.
extern crate alloc;
use spin::{Mutex, MutexGuard};

use crate::{
    err::{JBDError, JBDResult},
    journal::Journal,
    sal::Buffer,
};
use alloc::{collections::LinkedList, sync::Arc, sync::Weak};

/// Journal-internal buffer management unit, equivalent to journal_head in Linux.
pub struct JournalBuffer {
    buf: Arc<Mutex<dyn Buffer>>,
    transaction: Option<Weak<Mutex<Transaction>>>,
    /// Pointer to the running compound transaction which is currently
    /// modifying the buffer's metadata, if there was already a transaction
    /// committing it when the new transaction touched it.
    next_transaction: Option<Weak<Mutex<Transaction>>>,
    /// This flag signals the buffer has been modified by the currently running transaction
    modified: bool,
}

impl JournalBuffer {
    pub fn new_or_get(buf: Arc<Mutex<dyn Buffer>>) -> Arc<Mutex<Self>> {
        let mut buf_locked = buf.lock();
        match buf_locked.journal_buffer() {
            Some(jb) => jb.clone(),
            None => {
                let ret = Arc::new(Mutex::new(Self {
                    buf: buf.clone(),
                    transaction: None,
                    next_transaction: None,
                    modified: false,
                }));
                buf_locked.set_journal_buffer(ret.clone());
                ret
            }
        }
    }
}

impl Drop for JournalBuffer {
    fn drop(&mut self) {
        // FIXME: journal_put_journal_head
        let mut buf_locked = self.buf.lock();
        if buf_locked.jdb_managed() {
            buf_locked.set_jdb_managed(false);
            buf_locked.set_private(None);
        }
    }
}

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
    pub reserved_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    /// Doubly-linked circular list of all buffers under writeout during
    /// commit [j_list_lock]
    pub locked_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    /// Doubly-linked circular list of all metadata buffers owned by this
    /// transaction [j_list_lock]
    pub buffers: LinkedList<Arc<Mutex<JournalBuffer>>>,
    /// Doubly-linked circular list of all data buffers still to be
    /// flushed before this transaction can be committed [j_list_lock]
    pub sync_datalist: LinkedList<Arc<Mutex<JournalBuffer>>>,

    pub forget: LinkedList<Arc<Mutex<JournalBuffer>>>,
    pub checkpoint_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    pub checkpoint_io_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    pub iobuf_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    pub shadow_list: LinkedList<Arc<Mutex<JournalBuffer>>>,
    pub log_list: LinkedList<Arc<Mutex<JournalBuffer>>>,

    pub handle_info: Mutex<TransactionHandleInfo>,

    pub expires: usize,
    pub start_time: usize, // TODO: ktime_t
    pub handle_count: i32,
    // pub synchronous_commit: bool,
}

impl Transaction {
    pub fn new(journal: Weak<Mutex<Journal>>) -> Self {
        Self {
            journal,
            tid: 0,
            state: TransactionState::Running,
            log_start: 0,
            nr_buffers: 0,
            reserved_list: LinkedList::new(),
            locked_list: LinkedList::new(),
            buffers: LinkedList::new(),
            sync_datalist: LinkedList::new(),
            forget: LinkedList::new(),
            checkpoint_list: LinkedList::new(),
            checkpoint_io_list: LinkedList::new(),
            iobuf_list: LinkedList::new(),
            shadow_list: LinkedList::new(),
            log_list: LinkedList::new(),
            handle_info: Mutex::new(TransactionHandleInfo {
                updates: 0,
                outstanding_credits: 0,
                handle_count: 0,
            }),
            expires: 0,
            start_time: 0,
            handle_count: 0,
        }
    }
}

#[derive(Clone, Copy)]
enum BufferListType {
    None,
    SyncData,
    Metadata,
    Forget,
    IO,
    Shwdow,
    LogCtl,
    Reserved,
    Locked,
}

impl Transaction {
    fn file_buffer(
        &mut self,
        jh: Arc<Mutex<JournalBuffer>>,
        jh_guard: &MutexGuard<JournalBuffer>,
        list_type: BufferListType,
    ) {
        todo!("file_buffer")
    }
}

/// Info related to handles, protected by handle_lock in Linux.
pub struct TransactionHandleInfo {
    /// Number of outstanding updates running on this transaction
    pub updates: u32,
    /// Number of buffers reserved for use by all handles in this transaction
    /// handle but not yet modified
    pub outstanding_credits: u32,
    /// How many handles used this transaction?
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
    pub fn get_create_access(&self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        let jh_rc = JournalBuffer::new_or_get(buffer.clone());
        let mut jh = jh_rc.lock();

        if self.aborted {
            return Err(JBDError::HandleAborted);
        }

        let mut buf = buffer.lock();
        // TODO: j_list_lock
        buf.lock_managed();
        // TODO: Lots of assertions here

        let tx_binding = self.transaction.clone().unwrap();
        let mut tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.lock();

        let should_set_next_tx = match &jh.transaction {
            None => {
                // From Linux:
                // Previous journal_forget() could have left the buffer
                // with jbddirty bit set because it was being committed. When
                // the commit finished, we've filed the buffer for
                // checkpointing and marked it dirty. Now we are reallocating
                // the buffer so the transaction freeing it must have
                // committed and so it's safe to clear the dirty bit.
                buf.clear_dirty();
                jh.modified = false;

                tx.file_buffer(jh_rc.clone(), &jh, BufferListType::Reserved);
                false
            }
            Some(tx) => {
                let states = journal.states.lock();

                match &states.committing_transaction {
                    None => false,
                    Some(committing_tx) => Arc::ptr_eq(&tx.upgrade().unwrap(), &committing_tx),
                }
            }
        };

        if should_set_next_tx {
            jh.modified = false;
            jh.next_transaction = Some(Arc::downgrade(&tx_binding));
        }

        buf.unlock_managed();
        self.cancel_revoke(jh_rc.clone());

        Ok(())
    }

    pub fn get_write_access(&self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        todo!()
    }

    pub fn cancel_revoke(&self, jh: Arc<Mutex<JournalBuffer>>) -> JBDResult {
        todo!()
    }

    // TODO: get_write_access, ...
}
