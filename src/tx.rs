//! Transaction.
extern crate alloc;
use spin::{Mutex, MutexGuard, RwLock};

use crate::{
    err::{JBDError, JBDResult},
    journal::{log_space_left, start_handle, Journal},
    sal::Buffer,
};
use alloc::{sync::Arc, sync::Weak, vec::Vec};

#[derive(Clone, Copy, PartialEq, Eq)]
enum BufferListType {
    None,
    SyncData,
    Metadata,
    Forget,
    IO,
    Shadow,
    LogCtl,
    Reserved,
    Locked,
}

/// Journal-internal buffer management unit, equivalent to journal_head in Linux.
pub struct JournalBuffer {
    buf: Arc<Mutex<dyn Buffer>>,
    /// Pointer to the compound transaction which owns this buffer's
    /// metadata: either the running transaction or the committing
    /// transaction (if there is one).  Only applies to buffers on a
    /// transaction's data or metadata journaling list.
    /// [j_list_lock] [jbd_lock_bh_state()]
    /// Either of these locks is enough for reading, both are needed for
    /// changes.
    transaction: Option<Weak<Mutex<Transaction>>>,
    /// Pointer to the running compound transaction which is currently
    /// modifying the buffer's metadata, if there was already a transaction
    /// committing it when the new transaction touched it.
    next_transaction: Option<Weak<Mutex<Transaction>>>,
    /// Pointer to the compound transaction against which this buffer
    /// is checkpointed.  Only dirty buffers can be checkpointed.
    /// [j_list_lock]
    cp_transaction: Option<Weak<Mutex<Transaction>>>,
    /// This flag signals the buffer has been modified by the currently running transaction
    modified: bool,
    /// List that the buffer is in
    jlist: BufferListType,
    /// Copy of the buffer data frozen for writing to the log.
    frozen_data: Option<Vec<u8>>,
    /// Pointer to a saved copy of the buffer containing no uncommitted
    /// deallocation references, so that allocations can avoid overwriting
    /// uncommitted deletes.
    commited_data: Option<Vec<u8>>,
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
                    cp_transaction: None,
                    modified: false,
                    jlist: BufferListType::None,
                    frozen_data: None,
                    commited_data: None,
                }));
                buf_locked.set_journal_buffer(ret.clone());
                ret
            }
        }
    }

    fn tx_eq(&self, tx: &Arc<Mutex<Transaction>>) -> bool {
        match &self.transaction {
            Some(t) => t.upgrade().map_or(false, |x| Arc::ptr_eq(&x, tx)),
            None => false,
        }
    }
}

impl Drop for JournalBuffer {
    fn drop(&mut self) {
        // FIXME: journal_put_journal_head
        let mut buf_locked = self.buf.lock();
        if buf_locked.jbd_managed() {
            buf_locked.set_jbd_managed(false);
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
    pub journal: Weak<RwLock<Journal>>,
    /// Sequence number for this transaction [no locking]
    pub tid: Tid,
    /// Transaction's current state
    /// [no locking - only kjournald alters this]
    pub state: TransactionState,
    /// Where in the log does this transaction's commit start? [no locking]
    pub log_start: u32,
    pub lists: Mutex<TransactionLists>,
    pub handle_info: Mutex<TransactionHandleInfo>,

    pub expires: usize,
    pub start_time: usize, // TODO: ktime_t
    pub handle_count: i32,
    // pub synchronous_commit: bool,
}

pub struct JournalBufferList(Vec<Arc<Mutex<JournalBuffer>>>);

impl JournalBufferList {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn remove(&mut self, jb: &Arc<Mutex<JournalBuffer>>) {
        self.0.retain(|x| !Arc::ptr_eq(x, jb));
    }
    fn insert(&mut self, jb: Arc<Mutex<JournalBuffer>>) {
        self.0.push(jb);
    }
}

pub struct TransactionLists {
    /// Doubly-linked circular list of all buffers reserved but not yet
    /// modified by this transaction [j_list_lock]
    pub reserved_list: JournalBufferList,
    /// Doubly-linked circular list of all buffers under writeout during
    /// commit [j_list_lock]
    pub locked_list: JournalBufferList,
    /// Doubly-linked circular list of all metadata buffers owned by this
    /// transaction [j_list_lock]
    pub buffers: JournalBufferList,
    /// Doubly-linked circular list of all data buffers still to be
    /// flushed before this transaction can be committed [j_list_lock]
    pub sync_datalist: JournalBufferList,

    pub forget: JournalBufferList,
    pub checkpoint_list: JournalBufferList,
    pub checkpoint_io_list: JournalBufferList,
    pub iobuf_list: JournalBufferList,
    pub shadow_list: JournalBufferList,
    pub log_list: JournalBufferList,

    /// Number of buffers on the t_buffers list [j_list_lock]
    pub nr_buffers: i32,
}

impl Transaction {
    pub fn new(journal: Weak<RwLock<Journal>>) -> Self {
        Self {
            journal,
            tid: 0,
            state: TransactionState::Running,
            log_start: 0,
            lists: Mutex::new(TransactionLists {
                nr_buffers: 0,
                reserved_list: JournalBufferList::new(),
                locked_list: JournalBufferList::new(),
                buffers: JournalBufferList::new(),
                sync_datalist: JournalBufferList::new(),
                forget: JournalBufferList::new(),
                checkpoint_list: JournalBufferList::new(),
                checkpoint_io_list: JournalBufferList::new(),
                iobuf_list: JournalBufferList::new(),
                shadow_list: JournalBufferList::new(),
                log_list: JournalBufferList::new(),
            }),
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

impl TransactionLists {
    fn remove(&mut self, jb: &Arc<Mutex<JournalBuffer>>, list_type: BufferListType) {
        // FIXME: Linux has lots of asserts here
        match list_type {
            BufferListType::None => {}
            BufferListType::SyncData => self.sync_datalist.remove(jb),
            BufferListType::Metadata => self.buffers.remove(jb),
            BufferListType::Forget => self.forget.remove(jb),
            BufferListType::IO => self.checkpoint_io_list.remove(jb),
            BufferListType::Shadow => self.shadow_list.remove(jb),
            BufferListType::LogCtl => self.log_list.remove(jb),
            BufferListType::Reserved => self.reserved_list.remove(jb),
            BufferListType::Locked => self.locked_list.remove(jb),
        };
    }
    fn insert(&mut self, jb: Arc<Mutex<JournalBuffer>>, list_type: BufferListType) {
        match list_type {
            BufferListType::None => {}
            BufferListType::SyncData => self.sync_datalist.insert(jb),
            BufferListType::Metadata => self.buffers.insert(jb),
            BufferListType::Forget => self.forget.insert(jb),
            BufferListType::IO => self.checkpoint_io_list.insert(jb),
            BufferListType::Shadow => self.shadow_list.insert(jb),
            BufferListType::LogCtl => self.log_list.insert(jb),
            BufferListType::Reserved => self.reserved_list.insert(jb),
            BufferListType::Locked => self.locked_list.insert(jb),
        };
    }
}

impl Transaction {
    /// Add a buffer to a transaction's list of buffers. Please call it with list_lock held
    /// and make sure the generic buffer is unlocked.
    fn file_buffer(
        tx_ref: &Arc<Mutex<Transaction>>,
        tx: &MutexGuard<Transaction>,
        jb_ref: &Arc<Mutex<JournalBuffer>>,
        jb: &mut MutexGuard<JournalBuffer>,
        list_type: BufferListType,
    ) -> JBDResult {
        if jb.transaction.is_some() && jb.jlist == list_type {
            // The buffer is already in the right list
            return Ok(());
        }

        let was_dirty = match list_type {
            BufferListType::Metadata | BufferListType::Reserved | BufferListType::Shadow | BufferListType::Forget => {
                // For metadata buffers, we track dirty bit in buffer_jbddirty
                // instead of buffer_dirty. We should not see a dirty bit set
                // here because we clear it in do_get_write_access but e.g.
                // tune2fs can modify the sb and set the dirty bit at any time
                // so we try to gracefully handle that.
                let mut buf = jb.buf.lock();
                if buf.dirty() {
                    warn_dirty_buffer();
                }
                buf.test_clear_dirty() || buf.test_clear_jbd_dirty()
            }
            _ => false,
        };

        // The lock is acquired here.
        let mut lists = tx.lists.lock();

        if jb.transaction.is_some() {
            lists.remove(&jb_ref, list_type);
            jb.jlist = list_type;
            if list_type == BufferListType::Metadata {
                lists.nr_buffers -= 1;
            }
            // FIXME: Should we mark dirty here?
        } else {
            // Linux increments the ref count here, but we don't need to.
        }
        jb.transaction = Some(Arc::downgrade(&tx_ref));

        lists.insert(jb_ref.clone(), list_type);

        if list_type == BufferListType::Metadata {
            lists.nr_buffers += 1;
        } else if list_type == BufferListType::None {
            return Ok(());
        }

        jb.jlist = list_type;

        if was_dirty {
            jb.buf.lock().mark_jbd_dirty();
        }

        Ok(())
    }

    /// Remove a buffer from the appropriate transaction list.
    /// Call under j_list_lock and unlocked tx.lists.
    fn temp_unlink_buffer(
        tx: &MutexGuard<Transaction>,
        jb_ref: &Arc<Mutex<JournalBuffer>>,
        jb: &mut MutexGuard<JournalBuffer>,
        buf: &mut MutexGuard<dyn Buffer>,
    ) {
        // The lock is acquired here.
        let mut lists = tx.lists.lock();

        if jb.jlist == BufferListType::Metadata {
            lists.nr_buffers -= 1;
        }

        lists.remove(&jb_ref, jb.jlist);
        jb.jlist = BufferListType::None;

        if buf.test_clear_jbd_dirty() {
            buf.mark_dirty();
        }
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

    pub fn extend(&mut self, nblocks: u32) -> JBDResult {
        if self.aborted {
            return Err(JBDError::HandleAborted);
        }

        let tx_binding = self.transaction.clone().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.write();

        let states = journal.states.lock();

        if tx.state != TransactionState::Running {
            log::debug!("Denied handle extend {} blocks: transaction not running", nblocks);
            return Err(JBDError::TransactionNotRunning);
        }

        let mut handle_info = tx.handle_info.lock();
        let wanted = handle_info.outstanding_credits + nblocks;

        if wanted > journal.max_transaction_buffers || wanted > log_space_left(&states) {
            log::debug!("Denied handle extend {} blocks: not enough space", nblocks);
            return Err(JBDError::NotEnoughSpace);
        }

        self.buffer_credits += nblocks;
        handle_info.outstanding_credits += nblocks;

        log::debug!("Extended handle by {} blocks", nblocks);

        Ok(())
    }

    /// All done for a particular handle.
    ///
    /// There is not much action needed here.  We just return any remaining
    /// buffer credits to the transaction and remove the handle.  The only
    /// complication is that we need to start a commit operation if the
    /// filesystem is marked for synchronous update.
    ///
    /// journal_stop itself will not usually return an error, but it may
    /// do so in unusual circumstances.  In particular, expect it to
    /// return -EIO if a journal_abort has been executed since the
    /// transaction began.
    pub fn stop(&self) -> JBDResult {
        let tx_binding = self.transaction.clone().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.write();

        // TODO: async version

        journal.system.set_current_handle(None);

        todo!();
    }

    /// Restart a handle for a multi-transaction filesystem
    /// operation.
    ///
    /// If the Handle::extend() call above fails to grant new buffer credits
    /// to a running handle, a call to journal_restart will commit the
    /// handle's transaction so far and reattach the handle to a new
    /// transaction capabable of guaranteeing the requested number of
    /// credits.
    pub fn restart(&mut self, nblocks: u32) -> JBDResult {
        if self.aborted {
            return Ok(());
        }

        let tx_binding = self.transaction.clone().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.write();

        let mut handle_info = tx.handle_info.lock();
        handle_info.outstanding_credits -= self.buffer_credits;
        handle_info.updates -= 1;

        // TODO: updates == 0
        log::debug!("Restarted handle");
        todo!("__log_start_commit");

        self.buffer_credits = nblocks;
        start_handle(journal_binding.clone(), &journal, &mut self);
    }
}

// Buffer managements
impl Handle {
    /// Notify intent to use newly created buffer. Call this if you create a new buffer.
    /// The buffer must not be locked.
    pub fn get_create_access(&self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        let jb_rc = JournalBuffer::new_or_get(buffer.clone());
        let mut jb = jb_rc.lock();

        if self.aborted {
            return Err(JBDError::HandleAborted);
        }

        let mut buf = buffer.lock();
        // TODO: Lots of assertions here

        let tx_binding = self.transaction.clone().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.read();

        buf.lock_jbd();
        let list_lock = journal.list_lock.lock();

        let should_set_next_tx = match &jb.transaction {
            None => {
                // From Linux:
                // Previous journal_forget() could have left the buffer
                // with jbddirty bit set because it was being committed. When
                // the commit finished, we've filed the buffer for
                // checkpointing and marked it dirty. Now we are reallocating
                // the buffer so the transaction freeing it must have
                // committed and so it's safe to clear the dirty bit.
                buf.clear_dirty();
                jb.modified = false;

                Transaction::file_buffer(&tx_binding, &tx, &jb_rc, &mut jb, BufferListType::Reserved)?;
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
            jb.modified = false;
            jb.next_transaction = Some(Arc::downgrade(&tx_binding));
        }

        drop(list_lock);
        buf.unlock_jbd();

        self.cancel_revoke(jb_rc.clone())?;

        Ok(())
    }

    /// Notify intent to modify a buffer for metadata (not data) update.
    pub fn get_write_access(&self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        let jb_rc = JournalBuffer::new_or_get(buffer.clone());
        // Make sure that the buffer completes any outstanding IO before proceeding
        self.do_get_write_access(jb_rc.clone(), false)?;
        Ok(())
    }

    /// Sometimes there is a need to distinguish between metadata which has
    /// been committed to disk and that which has not.  The ext3fs code uses
    /// this for freeing and allocating space, we have to make sure that we
    /// do not reuse freed space until the deallocation has been committed,
    /// since if we overwrote that space we would make the delete
    /// un-rewindable in case of a crash.
    ///
    /// To deal with that, journal_get_undo_access requests write access to a
    /// buffer for parts of non-rewindable operations such as delete
    /// operations on the bitmaps.  The journaling code must keep a copy of
    /// the buffer's contents prior to the undo_access call until such time
    /// as we know that the buffer has definitely been committed to disk.
    ///
    /// We never need to know which transaction the committed data is part
    /// of, buffers touched here are guaranteed to be dirtied later and so
    /// will be committed to a new transaction in due course, at which point
    /// we can discard the old committed data pointer.
    pub fn get_undo_access(&self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        let jb_rc = JournalBuffer::new_or_get(buffer.clone());
        let buf = buffer.lock();

        self.do_get_write_access(jb_rc.clone(), true)?;

        let mut jb = jb_rc.lock();
        if jb.commited_data.is_none() {
            jb.commited_data = Some(Vec::new());
        }

        let commited_data = jb.commited_data.as_mut().unwrap();
        commited_data.copy_from_slice(buf.buf());

        Ok(())
    }

    /// Mark a buffer as containing dirty data which needs to be flushed before
    /// we can commit the current transaction.
    ///
    /// The buffer is placed on the transaction's data list and is marked as
    /// belonging to the transaction.
    pub fn dirty_data(&mut self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        if self.aborted {
            return Err(JBDError::HandleAborted);
        }

        let jb_rc = JournalBuffer::new_or_get(buffer.clone());
        let mut jb = jb_rc.lock();
        let tx_binding = self.transaction.as_mut().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.read();

        // What if the buffer is already part of a running transaction?
        //
        // There are two cases:
        // 1) It is part of the current running transaction.  Refile it,
        //    just in case we have allocated it as metadata, deallocated
        //    it, then reallocated it as data.
        // 2) It is part of the previous, still-committing transaction.
        //    If all we want to do is to guarantee that the buffer will be
        //    written to disk before this new transaction commits, then
        //    being sure that the *previous* transaction has this same
        //    property is sufficient for us!  Just leave it on its old
        //    transaction.
        //
        // In case (2), the buffer must not already exist as metadata
        // --- that would violate write ordering (a transaction is free
        // to write its data at any point, even before the previous
        // committing transaction has committed).  The caller must
        // never, ever allow this to happen: there's nothing we can do
        // about it in this layer.
        let mut buf = buffer.lock();
        buf.lock_jbd();
        let mut list_lock = journal.list_lock.lock();

        // TODO: buffer_mapped

        if let Some(jb_tx) = &jb.transaction {
            let jb_tx = jb_tx.upgrade().unwrap();
            if !Arc::ptr_eq(&jb_tx, &tx_binding) {
                // The buffer belongs to a different transaction.
                if jb.jlist != BufferListType::None
                    && jb.jlist != BufferListType::SyncData
                    && jb.jlist != BufferListType::Locked
                {
                    drop(list_lock);
                    buf.unlock_jbd();

                    return Ok(());
                }

                if buf.dirty() {
                    // write back synchronously
                    drop(list_lock);
                    buf.unlock_jbd();
                    buf.sync();
                    buf.lock_jbd();
                    list_lock = journal.list_lock.lock();
                    // TODO: if !buffer_mapped
                }
                // TODO: if (unlikely(!buffer_uptodate(bh)))
                if let Some(jb_tx) = &jb.transaction {
                    let jb_tx = jb_tx.upgrade().unwrap();
                    if !Arc::ptr_eq(&jb_tx, &tx_binding) {
                        // Unlink buffer from old transaction
                        Transaction::temp_unlink_buffer(&jb_tx.lock(), &jb_rc, &mut jb, &mut buf);
                        jb.transaction = Some(Arc::downgrade(tx_binding));
                    }
                }
            }

            if jb.jlist != BufferListType::SyncData && jb.jlist != BufferListType::Locked {
                // jb.jlist == BufferListType::None here
                Transaction::file_buffer(&tx_binding, &tx, &jb_rc, &mut jb, BufferListType::SyncData)?;
            }
        } else {
            Transaction::file_buffer(&tx_binding, &tx, &jb_rc, &mut jb, BufferListType::SyncData)?;
        }

        // no_journal:
        drop(list_lock);
        buf.unlock_jbd();

        Ok(())
    }

    /// Mark dirty metadata which needs to be journaled as part of the current
    /// transaction.
    ///
    /// The buffer is placed on the transaction's metadata list and is marked
    /// as belonging to the transaction.
    ///
    /// Special care needs to be taken if the buffer already belongs to the
    /// current committing transaction (in which case we should have frozen
    /// data present for that commit).  In that case, we don't relink the
    /// buffer: that only gets done when the old transaction finally
    /// completes its commit.
    pub fn dirty_metadata(&mut self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        if self.aborted {
            return Err(JBDError::HandleAborted);
        }

        let jb_binding = JournalBuffer::new_or_get(buffer.clone());
        let mut jb = jb_binding.lock();
        let tx_binding = self.transaction.as_mut().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.write();

        let mut buf = buffer.lock();
        buf.lock_jbd();

        if !jb.modified {
            jb.modified = true;
            self.buffer_credits -= 1;
        }

        if jb.tx_eq(&tx_binding) && jb.jlist == BufferListType::Metadata {
            // The buffer is already part of the metadata of current transaction.
            // Nothing to do.
            buf.unlock_jbd();
            return Ok(());
        }

        let list_lock = journal.list_lock.lock();
        Transaction::file_buffer(&tx_binding, &tx, &jb_binding, &mut jb, BufferListType::Metadata)?;
        drop(list_lock);
        buf.unlock_jbd();

        Ok(())
    }

    /// bforget() for potentially-journaled buffers.
    ///
    /// We can only do the bforget if there are no commits pending against the
    /// buffer.  If the buffer is dirty in the current running transaction we
    /// can safely unlink it.
    ///
    /// bh may not be a journalled buffer at all - it may be a non-JBD
    /// buffer which came off the hashtable.  Check for this.
    ///
    /// Allow this call even if the handle has aborted --- it may be part of
    /// the caller's cleanup after an abort.
    fn forget(&mut self, buffer: Arc<Mutex<dyn Buffer>>) -> JBDResult {
        let jb_binding = JournalBuffer::new_or_get(buffer.clone());
        let mut jb = jb_binding.lock();
        let tx_binding = self.transaction.as_mut().unwrap();
        let tx = tx_binding.lock();
        let journal_binding = tx.journal.upgrade().unwrap();
        let journal = journal_binding.write();

        let mut buf = buffer.lock();
        buf.lock_jbd();
        let list_lock = journal.list_lock.lock();

        buf.lock_jbd();
        if !buf.jbd_managed() {
            drop(list_lock);
            buf.unlock_jbd();
            return Ok(());
        }

        if jb.commited_data.is_some() {
            // Critical error: attempting to delete a bitmap buffer, maybe?
            drop(list_lock);
            buf.unlock_jbd();
            return Err(JBDError::IOError);
        }

        let was_modified = jb.modified;
        jb.modified = false;

        let mut drop_reserve = false;

        if jb.tx_eq(&tx_binding) {
            buf.clear_dirty();
            buf.clear_jbd_dirty();

            // We only want to drop a reference if this transaction
            // modified the buffer
            if was_modified {
                drop_reserve = true;
            }

            if jb.cp_transaction.is_some() {
                Transaction::temp_unlink_buffer(&tx, &jb_binding, &mut jb, &mut buf);
                Transaction::file_buffer(&tx_binding, &tx, &jb_binding, &mut jb, BufferListType::Forget)?;
            } else {
                todo!("unfile_buffer")
            }
        } else if jb.transaction.is_some() {
            // Belongs to older transaction
            if jb.next_transaction.is_some() {
                jb.next_transaction = None;
                if was_modified {
                    drop_reserve = true;
                }
            }
        }

        drop(list_lock);
        buf.unlock_jbd();

        if drop_reserve {
            // No need to reserve log space for this block
            self.buffer_credits -= 1;
        }

        Ok(())
    }

    pub fn cancel_revoke(&self, jh: Arc<Mutex<JournalBuffer>>) -> JBDResult {
        todo!()
    }
}

impl Handle {
    /// If the buffer is already part of the current transaction, then there
    /// is nothing we need to do.  If it is already part of a prior
    /// transaction which we are still committing to disk, then we need to
    /// make sure that we do not overwrite the old copy: we do copy-out to
    /// preserve the copy going to disk.  We also account the buffer against
    /// the handle's metadata buffer credits (unless the buffer is already
    /// part of the transaction, that is).
    fn do_get_write_access(&self, jb_rc: Arc<Mutex<JournalBuffer>>, force_copy: bool) -> JBDResult {
        if self.aborted {
            return Err(JBDError::HandleAborted);
        }
        let mut jb = jb_rc.lock();
        // TODO

        let buf_rc = jb.buf.clone();
        let mut buf = buf_rc.lock();

        buf.lock_jbd();

        // We now hold the buffer lock so it is safe to query the buffer
        // state.  Is the buffer dirty?
        //
        // If so, there are two possibilities.  The buffer may be
        // non-journaled, and undergoing a quite legitimate writeback.
        // Otherwise, it is journaled, and we don't expect dirty buffers
        // in that state (the buffers should be marked JBD_Dirty
        // instead.)  So either the IO is being done under our own
        // control and this is a bug, or it's a third party IO such as
        // dump(8) (which may leave the buffer scheduled for read ---
        // ie. locked but not dirty) or tune2fs (which may actually have
        // the buffer dirtied, ugh.)
        if buf.dirty() {
            // First question: is this buffer already part of the current
            // transaction or the existing committing transaction?
            if jb.transaction.is_some() {
                warn_dirty_buffer();
            }
            // In any case we need to clean the dirty flag.
            buf.clear_dirty();
            buf.mark_jbd_dirty();
        }

        if self.aborted {
            buf.unlock_jbd();
            return Err(JBDError::HandleAborted);
        }

        // A loop to conveniently convert from gotoes in Linux.
        loop {
            let this_tx = self.transaction.clone().ok_or(JBDError::Unknown)?;

            // The buffer is already part of this transaction if b_transaction or
            // b_next_transaction points to it
            if let Some(tx) = &jb.transaction {
                if tx.upgrade().map_or(false, |tx| Arc::ptr_eq(&tx, &this_tx)) {
                    break;
                }
            }

            jb.modified = false;

            // If there is already a copy-out version of this buffer, then we don't
            // need to make another one
            if jb.frozen_data.is_some() {
                jb.next_transaction = Some(Arc::downgrade(&this_tx));
                break;
            }

            // Is there data here we need to preserve?
            if let Some(tx) = &jb.transaction {
                if !Arc::ptr_eq(&tx.upgrade().unwrap(), &this_tx) {
                    // There is one case we have to be very careful about.
                    // If the committing transaction is currently writing
                    // this buffer out to disk and has NOT made a copy-out,
                    // then we cannot modify the buffer contents at all
                    // right now.  The essence of copy-out is that it is the
                    // extra copy, not the primary copy, which gets
                    // journaled.  If the primary copy is already going to
                    // disk then we cannot do copy-out here.
                    if jb.jlist == BufferListType::Shadow {
                        todo!()
                    }

                    // Only do the copy if the currently-owning transaction
                    // still needs it.  If it is on the Forget list, the
                    // committing transaction is past that stage.  The
                    // buffer had better remain locked during the kmalloc,
                    // but that should be true --- we hold the journal lock
                    // still and the buffer is already on the BUF_JOURNAL
                    // list so won't be flushed.
                    //
                    // Subtle point, though: if this is a get_undo_access,
                    // then we will be relying on the frozen_data to contain
                    // the new value of the committed_data record after the
                    // transaction, so we HAVE to force the frozen_data copy
                    // in that case.
                    if jb.jlist == BufferListType::Forget || force_copy {
                        // Allocate memory for buffer and copy the data
                        let mut frozen_data = Vec::new();
                        frozen_data.clone_from_slice(buf.buf());
                        jb.frozen_data = Some(frozen_data);
                    }
                    jb.next_transaction = Some(Arc::downgrade(&this_tx));
                }
            }
            // Finally, if the buffer is not journaled right now, we need to make
            // sure it doesn't get written to disk before the caller actually
            // commits the new data
            if jb.transaction.is_none() {
                jb.transaction = Some(Arc::downgrade(&this_tx));
                let tx = this_tx.lock();
                let journal_binding = tx.journal.upgrade().unwrap();
                let journal = journal_binding.read();
                let _ = journal.list_lock.lock();
                Transaction::file_buffer(&this_tx, &tx, &jb_rc, &mut jb, BufferListType::Reserved)?;
            }
        }
        // done:
        buf.unlock_jbd();
        self.cancel_revoke(jb_rc.clone())?;

        Ok(())
    }
}

#[inline]
fn warn_dirty_buffer() {
    log::warn!("Buffer is dirty");
}
