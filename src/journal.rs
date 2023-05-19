extern crate alloc;

use core::{
    borrow::Borrow,
    cell::{Ref, RefCell},
};

use alloc::{collections::LinkedList, rc::Rc, sync::Weak, vec::Vec};
use bitflags::bitflags;

use crate::{
    config::{JFS_MAGIC_NUMBER, JFS_MIN_JOURNAL_BLOCKS, MIN_LOG_RESERVED_BLOCKS},
    disk::{BlockType, Superblock},
    err::{JBDError, JBDResult},
    sal::{BlockDevice, Buffer, System},
    tx::{Handle, Tid, Transaction, TransactionState},
};

pub struct Journal {
    pub system: Rc<dyn System>,
    pub sb_buffer: Rc<dyn Buffer>,
    pub format_version: i32,
    pub flags: JournalFlag,
    pub errno: i32, // TODO: Strongly-typed error?
    pub running_transaction: Option<Rc<RefCell<Transaction>>>,
    pub committing_transaction: Option<Rc<RefCell<Transaction>>>,
    /// Journal head: identifies the first unused block in the journal.
    pub head: u32,
    /// Journal tail: identifies the oldest still-used block in the journal
    pub tail: u32,
    /// Journal free: how many free blocks are there in the journal?
    pub free: u32,
    /// Journal start: the block number of the first usable block in the journal
    pub first: u32,
    /// Journal end: the block number of the last usable block in the journal
    pub last: u32,
    /// Sequence number of the oldest transaction in the log
    pub tail_sequence: Tid,
    /// Sequence number of the next transaction to grant
    pub transaction_sequence: Tid,
    /// Sequence number of the most recently committed transaction
    pub commit_sequence: Tid,
    /// Sequence number of the most recent transaction wanting commit
    pub commit_request: Tid,
    /// List of all transactions waiting for checkpointing
    pub checkpoint_transactions: LinkedList<Rc<RefCell<Transaction>>>,
    /// Block devices
    pub devs: JournalDevs,
    /// Total maximum capacity of the journal region on disk
    pub maxlen: u32,
    /// Maximum number of metadata buffers to allow in a single compound
    /// commit transaction
    pub max_transaction_buffers: u32,
    // commit_interval: usize,
    // wbuf: Vec<Option<Rc<BufferHead>>>,
}

bitflags! {
    pub struct JournalFlag: usize {
        const UNMOUNT = 0x001;
        const ABORT = 0x002;
        const ACK_ERR = 0x004;
        const FLUSHED = 0x008;
        const LOADED = 0x010;
        const BARRIER = 0x020;
        const ABORT_ON_SYNCDATA_ERR = 0x040;
    }
}

pub struct JournalDevs {
    dev: Rc<dyn BlockDevice>,
    blk_offset: u32,
    fs_dev: Rc<dyn BlockDevice>,
}

/// Journal states protected by a single spin lock in Linux.
pub struct JournalStates {
    pub flags: JournalFlag,
    pub errno: i32, // TODO: Strongly-typed error?
    pub running_transaction: Option<Rc<Transaction>>,
    pub committing_transaction: Option<Rc<Transaction>>,
    /// Journal head: identifies the first unused block in the journal.
    pub head: u32,
    /// Journal tail: identifies the oldest still-used block in the journal
    pub tail: u32,
    /// Journal free: how many free blocks are there in the journal?
    pub free: u32,
    /// Journal start: the block number of the first usable block in the journal
    pub first: u32,
    /// Journal end: the block number of the last usable block in the journal
    pub last: u32,

    /// Sequence number of the oldest transaction in the log
    pub tail_sequence: Tid,
    /// Sequence number of the next transaction to grant
    pub transaction_sequence: Tid,
    /// Sequence number of the most recently committed transaction
    pub commit_sequence: Tid,
    /// Sequence number of the most recent transaction wanting commit
    pub commit_request: Tid,
}

struct RevokeTable;

pub struct JournalHead {
    bh: Weak<dyn Buffer>,
}

impl JournalHead {
    pub fn new(bh: Weak<dyn Buffer>) -> Self {
        Self { bh }
    }
}

/// Public interfaces.
impl Journal {
    /// Initialize an in-memory journal structure with a block device.
    pub fn init_dev(
        system: Rc<dyn System>,
        dev: Rc<dyn BlockDevice>,
        fs_dev: Rc<dyn BlockDevice>,
        start: u32,
        len: u32,
    ) -> JBDResult<Self> {
        let devs = JournalDevs {
            dev,
            blk_offset: start,
            fs_dev,
        };
        let sb_buffer = system
            .get_buffer_provider()
            .get_buffer(devs.dev.clone(), devs.blk_offset as usize);
        if sb_buffer.is_none() {
            return Err(JBDError::IOError);
        }

        let ret = Self {
            system: system.clone(),
            sb_buffer: sb_buffer.unwrap(),
            format_version: 0,
            flags: JournalFlag::ABORT,
            errno: 0,
            running_transaction: None,
            committing_transaction: None,
            head: 0,
            tail: 0,
            free: 0,
            first: 0,
            last: 0,
            tail_sequence: 0,
            transaction_sequence: 0,
            commit_sequence: 0,
            commit_request: 0,
            checkpoint_transactions: LinkedList::new(),
            devs,
            maxlen: len,
            max_transaction_buffers: 0,
        };

        Ok(ret)
    }

    pub fn create(&mut self) -> JBDResult {
        if self.maxlen < JFS_MIN_JOURNAL_BLOCKS {
            log::error!("Journal too small: {} blocks.", self.maxlen);
            return Err(JBDError::InvalidJournalSize);
        }

        // TODO: j_inode
        log::debug!("Zeroing out journal blocks.");
        for i in 0..self.maxlen {
            let block_id = i;
            let page_head = self.get_buffer(block_id)?;
            let buf = page_head.buf_mut();
            buf.fill(0);
        }

        // FIXME
        self.sync_buf()?;
        log::debug!("Journal cleared.");

        let sb = self.superblock_mut();

        sb.header.magic = JFS_MAGIC_NUMBER.to_be();
        sb.header.block_type = <BlockType as Into<u32>>::into(BlockType::SuperblockV2).to_be();

        sb.block_size = (self.devs.dev.block_size() as u32).to_be();
        sb.maxlen = self.maxlen.to_be();
        sb.first = 1_u32.to_be();

        self.transaction_sequence = 1;

        self.flags.remove(JournalFlag::ABORT);

        self.format_version = 2;

        self.reset()
    }

    pub fn load(&mut self) -> JBDResult {
        self.load_superblock()?;

        // TODO: self.recover()?;
        self.reset()?;

        self.flags.remove(JournalFlag::ABORT);
        self.flags.insert(JournalFlag::LOADED);

        Ok(())
    }

    // void journal_lock_updates () - establish a transaction barrier.
    // @journal:  Journal to establish a barrier on.
    //
    // This locks out any further updates from being started, and blocks until all
    // existing updates have completed, returning only once the journal is in a
    // quiescent state with no updates running.
    //
    // We do not use simple mutex for synchronization as there are syscalls which
    // want to return with filesystem locked and that trips up lockdep. Also
    // hibernate needs to lock filesystem but locked mutex then blocks hibernation.
    // Since locking filesystem is rare operation, we use simple counter and
    // waitqueue for locking.
    pub fn lock_updates(&mut self) {
        todo!()
    }

    pub fn unlock_updates(&mut self) {
        todo!()
    }

    pub fn force_commit(&mut self) {
        todo!()
    }

    pub fn start(journal_rc: Rc<RefCell<Journal>>, nblocks: u32) -> JBDResult<Rc<RefCell<Handle>>> {
        let journal = journal_rc.as_ref().borrow();
        if let Some(current_handle) = journal.system.get_current_handle() {
            return Ok(current_handle.clone());
        }
        let mut handle = Handle::new(nblocks);
        drop(journal);
        start_handle(&journal_rc, &mut handle)?;
        let handle = Rc::new(RefCell::new(handle));
        journal_rc
            .as_ref()
            .borrow()
            .system
            .set_current_handle(Some(handle.clone()));
        Ok(handle)
    }
}

/// Commit related interfaces.
impl Journal {
    pub fn commit_transaction(&mut self) {
        // First job: lock down the current transaction and wait for
        // all outstanding updates to complete.
        if self.flags.contains(JournalFlag::FLUSHED) {
            self.update_superblock();
        }
        assert!(self.running_transaction.is_some());
        assert!(self.committing_transaction.is_none());

        let mut commit_tx = self.running_transaction.as_mut().unwrap().borrow_mut();
        assert!(commit_tx.state == TransactionState::Running);

        log::debug!("Start committing transaction {}.", commit_tx.tid);
        commit_tx.state = TransactionState::Locked;

        while commit_tx.updates > 0 {
            todo!("wait for updates to complete");
        }

        assert!(commit_tx.outstanding_credits <= self.max_transaction_buffers);

        let mut reserved_list: Vec<_> = commit_tx.reserved_list.0.clone();
        commit_tx.reserved_list.0.clear();

        for jb_rc in reserved_list.into_iter() {
            {
                let mut jb = jb_rc.borrow_mut();
                if jb.commited_data.is_some() {
                    jb.commited_data = None;
                }
            }
            Transaction::refile_buffer(&jb_rc);
        }

        // TODO: Now try to drop any written-back buffers from the journal's
        // checkpoint lists.  We do this *before* commit because it potentially
        // frees some memory.

        log::debug!("Commit phase 1.");

        // TODO: Clear revoked flag to reflect there is no revoked buffers
        // in the next transaction which is going to be started.

        commit_tx.state = TransactionState::Flush;
        drop(commit_tx);

        self.committing_transaction = self.running_transaction.clone();
        self.running_transaction = None;
        let mut commit_tx = self.running_transaction.as_mut().unwrap().borrow_mut();
        let start_time = self.system.get_time();
        commit_tx.log_start = start_time as u32;

        log::debug!("Commit phase 2.");

        // Now start flushing things to disk, in the order they appear
        // on the transaction lists.  Data blocks go first.
        // self.
    }

    fn submit_data_buffers(&self, transaction: &mut Transaction) {
        // let datalist = transaction.sync_datalist.0.clone();
        // transaction.sync_datalist.0.clear();
        // for jb in datalist.into_iter() {
        //     let jb = jb.lock();
        //     let mut buf = transaction.buf;
        // }
    }
}

/// Checkpoint related interfaces.
impl Journal {
    pub fn do_checkpoint(&mut self) -> JBDResult {
        todo!()
    }

    pub fn cleanup_tail(&mut self) -> JBDResult {
        todo!()
    }
}

/// Internal helper functions.
impl Journal {
    /// Given a journal_t structure, initialize the various fields for
    /// startup of a new journaling session.  We use this both when creating
    /// a journal, and after recovering an old journal to reset it for
    /// subsequent use.
    fn reset(&mut self) -> JBDResult {
        let sb = self.superblock_mut();

        let first = u32::from_be(sb.first);
        let last = u32::from_be(sb.maxlen);

        if first + JFS_MIN_JOURNAL_BLOCKS > last + 1 {
            log::error!("Journal too small: blocks {}-{}.", first, last);
            // TODO: Discard
            return Err(JBDError::InvalidJournalSize);
        }

        self.first = first;
        self.last = last;

        self.head = first;
        self.tail = first;
        self.free = last - first;

        self.tail_sequence = self.transaction_sequence;
        self.commit_sequence = self.transaction_sequence - 1;
        self.commit_request = self.commit_sequence;

        self.max_transaction_buffers = self.maxlen / 4;

        self.update_superblock();

        Ok(()) // FIXME: Should start deamon thread here.
    }

    /// Load the on-disk journal superblock and read the key fields.
    fn load_superblock(&mut self) -> JBDResult {
        self.validate_superblock()?;

        let sb = self.superblock_ref();

        let tail_sequence = u32::from_be(sb.sequence) as u16;
        let tail = u32::from_be(sb.start);
        let first = u32::from_be(sb.first);
        let last = u32::from_be(sb.maxlen);
        let errno = i32::from_be(sb.errno);

        drop(sb);

        self.tail_sequence = tail_sequence;
        self.tail = tail;
        self.first = first;
        self.last = last;
        self.errno = errno;

        Ok(())
    }

    /// Update a journal's dynamic superblock fields and write it to disk,
    /// ~~optionally waiting for the IO to complete~~.
    fn update_superblock(&mut self) {
        let sb = self.superblock_mut();

        if sb.start == 0 && self.tail_sequence == self.transaction_sequence {
            log::debug!("Skipping superblock update on newly created / recovered journal.");
            self.flags.insert(JournalFlag::FLUSHED);
            return;
        }

        log::debug!("Updating superblock.");
        sb.sequence = (self.tail_sequence as u32).to_be();
        sb.start = self.tail.to_be();
        sb.errno = self.errno;

        self.sb_buffer.sync();

        if self.tail != 0 {
            self.flags.insert(JournalFlag::FLUSHED);
        } else {
            self.flags.remove(JournalFlag::FLUSHED);
        }
    }

    fn validate_superblock(&mut self) -> JBDResult {
        // No need to test buffer_uptodate here as in our implementation as far,
        // the buffer will always be valid.
        let sb = self.superblock_ref();

        if sb.header.magic != JFS_MAGIC_NUMBER.to_be() || sb.block_size != (self.devs.dev.block_size() as u32).to_be() {
            log::error!("Invalid journal superblock magic number or block size.");
            return Err(JBDError::InvalidSuperblock);
        }

        let block_type: BlockType = u32::from_be(sb.header.block_type).try_into()?;

        drop(sb);

        match block_type {
            BlockType::SuperblockV1 => self.format_version = 1,
            BlockType::SuperblockV2 => self.format_version = 2,
            _ => {
                log::error!("Invalid journal superblock block type.");
                return Err(JBDError::InvalidSuperblock);
            }
        }

        if u32::from_be(self.superblock_ref().maxlen) <= self.maxlen {
            self.maxlen = u32::from_be(self.superblock_ref().maxlen);
        } else {
            log::error!("Journal too short.");
            // Linux returns -EINVAL here, so as we.
            return Err(JBDError::InvalidSuperblock);
        }

        if u32::from_be(self.superblock_ref().first) == 0 || u32::from_be(self.superblock_ref().first) >= self.maxlen {
            log::error!("Journal has invalid start block.");
            return Err(JBDError::InvalidSuperblock);
        }

        Ok(())
    }

    fn superblock_ref<'a>(&'a self) -> &'a Superblock {
        self.sb_buffer.convert::<Superblock>()
    }

    fn superblock_mut<'a>(&'a self) -> &'a mut Superblock {
        self.sb_buffer.convert_mut::<Superblock>()
    }

    fn get_buffer(&mut self, block_id: u32) -> JBDResult<Rc<dyn Buffer>> {
        self.system
            .get_buffer_provider()
            .get_buffer(self.devs.dev.clone(), block_id as usize)
            .map_or(Err(JBDError::IOError), |bh| Ok(bh))
    }

    fn sync_buf(&mut self) -> JBDResult {
        if self.system.get_buffer_provider().sync() {
            Ok(())
        } else {
            Err(JBDError::IOError)
        }
    }

    /// Start a new transaction in the journal, equivalent to get_transaction()
    /// in linux.
    fn set_transaction(&mut self, tx: &Rc<RefCell<Transaction>>) {
        {
            let mut tx_mut = tx.borrow_mut();
            tx_mut.state = TransactionState::Running;
            tx_mut.start_time = self.system.get_time();
            tx_mut.tid = self.transaction_sequence;
        }
        self.transaction_sequence += 1;
        // TODO: tx.expires
        self.running_transaction = Some(tx.clone());
    }

    pub(crate) fn log_space_left(&self) -> u32 {
        let mut left = self.free;
        left -= MIN_LOG_RESERVED_BLOCKS;
        if left <= 0 {
            0
        } else {
            left - (left >> 3)
        }
    }
}

pub(crate) fn start_handle(journal_rc: &Rc<RefCell<Journal>>, handle: &mut Handle) -> JBDResult {
    let mut journal = journal_rc.borrow_mut();
    let nblocks = handle.buffer_credits;

    if nblocks > journal.max_transaction_buffers {
        log::error!(
            "Transaction requires too many credits ({} > {}).",
            nblocks,
            journal.max_transaction_buffers
        );
        return Err(JBDError::NotEnoughSpace);
    }

    let system = &journal.system;

    log::debug!("New handle going live.");

    if journal.flags.contains(JournalFlag::ABORT)
        || (journal.errno != 0 && !journal.flags.contains(JournalFlag::ACK_ERR))
    {
        log::error!("Journal has aborted.");
        return Err(JBDError::IOError);
    }

    if journal.running_transaction.is_none() {
        let tx = Transaction::new(Rc::downgrade(journal_rc));
        let tx = Rc::new(RefCell::new(tx));
        journal.set_transaction(&tx);
    }

    let transaction_rc = journal.running_transaction.as_ref().unwrap().clone();
    let mut transaction = journal.running_transaction.as_ref().unwrap().borrow_mut();

    if transaction.state == TransactionState::Locked {
        todo!("Wait for transaction to unlock.");
    }

    let needed = transaction.outstanding_credits as u32 + nblocks;

    if needed > journal.max_transaction_buffers {
        todo!("Wait for previous transaction to commit.");
    }

    if journal.log_space_left() < needed {
        todo!("Wait for checkpoint.");
    }

    handle.transaction = Some(transaction_rc);
    transaction.outstanding_credits += nblocks;
    transaction.updates += 1;
    transaction.handle_count += 1;

    log::debug!("Handle now has {} credits.", transaction.outstanding_credits);

    return Ok(());
}
