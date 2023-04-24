extern crate alloc;
use core::mem::size_of;

use alloc::{collections::LinkedList, sync::Arc, vec::Vec};
use bitflags::bitflags;
use spin::{Mutex, MutexGuard};

use crate::{
    buffer::BufferHead,
    config::{JBD_DEFAULT_MAX_COMMIT_AGE, JFS_MAGIC_NUMBER, JFS_MIN_JOURNAL_BLOCKS, MIN_LOG_RESERVED_BLOCKS},
    disk::{BlockTag, BlockType, Superblock},
    err::{JBDError, JBDResult},
    errno::EROFS,
    sal::{BlockDevice, System, WaitQueue},
    tx::{Handle, Tid, Transaction, TransactionState},
};

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

pub struct Journal {
    system: Arc<Mutex<dyn System>>,
    // TODO: j_inode, j_task, j_commit_timer, j_last_sync_writer, j_private
    sb_buffer: Arc<Mutex<BufferHead>>,
    // superblock: Superblock,
    format_version: i32,
    /// Journal states protected by j_state_lock in Linux.
    states: Mutex<JournalStates>,

    /// Journal lists protected by j_list_lock in Linux.
    lists: Mutex<JournalLists>,

    /// Wait queue for waiting for a locked transaction to start committing,
    /// or for a barrier lock to be released
    wait_transaction_locked: Arc<Mutex<dyn WaitQueue>>,
    /// Wait queue for waiting for checkpointing to complete
    wait_logspace: Arc<Mutex<dyn WaitQueue>>,
    /// Wait queue for waiting for commit to complete
    wait_done_commit: Arc<Mutex<dyn WaitQueue>>,
    /// Wait queue to trigger checkpointing
    wait_checkpoint: Arc<Mutex<dyn WaitQueue>>,
    /// Wait queue to trigger commit
    wait_commit: Arc<Mutex<dyn WaitQueue>>,
    /// Wait queue to wait for updates to complete
    wait_updates: Arc<Mutex<dyn WaitQueue>>,

    devs: JournalDevs,

    maxlen: u32,

    uuid: [u8; 16],
    max_transaction_buffers: u32,
    commit_interval: usize,

    revoke_tables: Mutex<JournalRevokeTables>,

    wbuf: Vec<Option<Arc<BufferHead>>>,
}

struct JournalDevs {
    dev: Arc<dyn BlockDevice>,
    blk_offset: u32,
    fs_dev: Arc<dyn BlockDevice>,
}

/// Journal states protected by a single spin lock in Linux.
struct JournalStates {
    barrier_count: u32,
    flags: JournalFlag,
    errno: i32, // TODO: Strongly-typed error?
    running_transaction: Option<Arc<Mutex<Transaction>>>,
    committing_transaction: Option<Arc<Mutex<Transaction>>>,
    head: u32,
    /// Journal tail: identifies the oldest still-used block in the journal
    tail: u32,
    /// Journal free: how many free blocks are there in the journal?
    free: u32,
    /// Journal start and end: the block numbers of the first usable block
    /// and one beyond the last usable block in the journal
    first: u32,
    last: u32,

    /// Sequence number of the oldest transaction in the log
    tail_sequence: Tid,
    /// Sequence number of the next transaction to grant
    transaction_sequence: Tid,
    /// Sequence number of the most recently committed transaction
    commit_sequence: Tid,
    /// Sequence number of the most recent transaction wanting commit
    commit_request: Tid,
    average_commit_time: u64,
}

struct JournalLists {
    /// a linked circular list of all transactions waiting for
    /// checkpointing. [j_list_lock]
    checkpoint_transactions: LinkedList<Arc<Transaction>>,
}

struct JournalRevokeTables {
    current: u8,
    revoke_table: [RevokeTable; 2],
}

struct RevokeTable;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryPassType {
    Scan,
    Revoke,
    Replay,
}

struct RecoveryInfo {
    start_transaction: Tid,
    end_transaction: Tid,
    num_replays: usize,
    num_revokes: usize,
    num_revoke_hits: usize,
}

impl RecoveryInfo {
    fn zero_init() -> Self {
        Self {
            start_transaction: 0,
            end_transaction: 0,
            num_replays: 0,
            num_revokes: 0,
            num_revoke_hits: 0,
        }
    }
}

/// Public interfaces.
impl Journal {
    pub fn init_dev(
        system: Arc<Mutex<dyn System>>,
        dev: Arc<dyn BlockDevice>,
        fs_dev: Arc<dyn BlockDevice>,
        start: u32,
        len: u32,
    ) -> JBDResult<Self> {
        let devs = JournalDevs {
            dev,
            blk_offset: start,
            fs_dev,
        };
        let mut journal = Self::init_common(system, devs)?;

        let n_blks = journal.devs.dev.block_size() / size_of::<BlockTag>();
        journal.wbuf.resize(n_blks, None);
        journal.maxlen = len;

        Ok(journal)
    }

    pub fn create(&mut self) -> JBDResult {
        if self.maxlen < JFS_MIN_JOURNAL_BLOCKS {
            log::error!("Journal too small: {} blocks.", self.maxlen);
            return Err(JBDError::InvalidJournalSize);
        }

        // TODO: j_inode
        log::debug!("Zeroing out journal blocks.");
        for i in 0..self.maxlen {
            let block_id = self.bmap(i)?;
            let page_head_locked = self.get_buffer(block_id)?;
            let mut page_head = page_head_locked.lock();
            let buf = page_head.buf_mut();
            buf.fill(0);
            // TODO: What is set_buffer_uptodate?
            // No need to __brelse, as rust has done the ref count for us :)
        }

        self.sync_buf()?;
        log::debug!("Journal cleared.");

        let mut sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_mut(&mut sb_guard);

        sb.header.magic = JFS_MAGIC_NUMBER.to_be();
        sb.header.block_type = <BlockType as Into<u32>>::into(BlockType::SuperblockV2).to_be();

        sb.block_size = (self.devs.dev.block_size() as u32).to_be();
        sb.maxlen = self.maxlen.to_be();
        sb.first = 1_u32.to_be();
        drop(sb_guard);

        let mut states = self.states.lock();
        states.transaction_sequence = 1;

        states.flags.remove(JournalFlag::ABORT);
        drop(states);

        self.format_version = 2;

        self.reset()
    }

    pub fn load(&mut self) -> JBDResult {
        self.load_superblock()?;

        // TODO: check features

        // FIXME: Do we need to follow the same errno here?
        self.recover()?;
        self.reset()?;

        let mut states = self.states.lock();
        states.flags.remove(JournalFlag::ABORT);
        states.flags.insert(JournalFlag::LOADED);

        Ok(())
    }

    pub fn recover(&mut self) -> JBDResult {
        todo!()
    }

    /// Wipe out all of the contents of a journal, safely.  This will produce
    /// a warning if the journal contains any valid recovery information.
    /// Must be called between Journal::init_*() and Journal::load().
    ///
    /// If 'write' is true, then we wipe out the journal on disk; otherwise
    /// we merely suppress recovery.
    pub fn wipe(&mut self, write: bool) -> JBDResult {
        self.load_superblock()?;

        let states = self.states.lock();
        if states.tail == 0 {
            return Ok(());
        }
        drop(states);

        log::warn!(
            "{} recovery information on journal.",
            if write { "Clearing" } else { "Ignoring" }
        );

        self.skip_recovery()?;
        self.update_superblock();

        Ok(())
    }

    /// Locate any valid recovery information from the journal and set up the
    /// journal structures in memory to ignore it (presumably because the
    /// caller has evidence that it is out of date).
    /// This function does'nt appear to be exorted..
    ///
    /// We perform one pass over the journal to allow us to tell the user how
    /// much recovery information is being erased, and to let us initialise
    /// the journal transaction sequence numbers to the next unused ID.
    pub fn skip_recovery(&mut self) -> JBDResult {
        let rec_res = self.recover_one_pass(RecoveryPassType::Scan);
        let mut states = self.states.lock();
        let sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_ref(&sb_guard);

        match rec_res {
            Ok(info) => {
                let dropped = info.end_transaction as isize - u32::from_be(sb.header.sequence) as isize;
                log::debug!("Ignoring {} transactions from the journal.", dropped);
                states.transaction_sequence = info.end_transaction + 1;
            }
            Err(e) => {
                log::error!("Error scanning journal: {:?}", e);
                states.transaction_sequence += 1;
                return Err(e);
            }
        }

        states.tail = 0;

        Ok(())
    }

    /// Perform a complete, immediate shutdown of the ENTIRE
    /// journal (not of a single transaction).  This operation cannot be
    /// undone without closing and reopening the journal.
    ///
    /// The journal_abort function is intended to support higher level error
    /// recovery mechanisms such as the ext2/ext3 remount-readonly error
    /// mode.
    ///
    /// Journal abort has very specific semantics.  Any existing dirty,
    /// unjournaled buffers in the main filesystem will still be written to
    /// disk by bdflush, but the journaling mechanism will be suspended
    /// immediately and no further transaction commits will be honoured.
    ///
    /// Any dirty, journaled buffers will be written back to disk without
    /// hitting the journal.  Atomicity cannot be guaranteed on an aborted
    /// filesystem, but we _do_ attempt to leave as much data as possible
    /// behind for fsck to use for cleanup.
    ///
    /// Any attempt to get a new transaction handle on a journal which is in
    /// ABORT state will just result in an -EROFS error return.  A
    /// journal_stop on an existing handle will return -EIO if we have
    /// entered abort state during the update.
    ///
    /// Recursive transactions are not disturbed by journal abort until the
    /// final journal_stop, which will receive the -EIO error.
    ///
    /// Finally, the journal_abort call allows the caller to supply an errno
    /// which will be recorded (if possible) in the journal superblock.  This
    /// allows a client to record failure conditions in the middle of a
    /// transaction without having to complete the transaction to record the
    /// failure to disk.  ext3_error, for example, now uses this
    /// functionality.
    ///
    /// Errors which originate from within the journaling layer will NOT
    /// supply an errno; a null errno implies that absolutely no further
    /// writes are done to the journal (unless there are any already in
    /// progress).
    pub fn abort(&mut self, errno: i32) {
        self.abort_soft(errno);
    }

    pub fn errno(&self) -> i32 {
        let states = self.states.lock();
        if states.flags.contains(JournalFlag::ABORT) {
            -EROFS
        } else {
            states.errno
        }
    }

    pub fn clear_err(&mut self) -> i32 {
        let mut states = self.states.lock();
        if states.flags.contains(JournalFlag::ABORT) {
            -EROFS
        } else {
            states.errno = 0;
            0
        }
    }

    pub fn ack_err(&mut self) {
        let mut states = self.states.lock();
        if states.errno != 0 {
            states.flags.insert(JournalFlag::ACK_ERR);
        }
    }

    // TODO: journal_blocks_per_page, ...
}

pub fn start(journal: Arc<Mutex<Journal>>, nblocks: u32) -> JBDResult<Arc<Mutex<Handle>>> {
    let mut journal_guard = journal.lock();
    let mut system = journal_guard.system.lock();
    if let Some(handle) = system.get_task_handle() {
        return Ok(handle.clone());
    }
    let handle = Arc::new(Mutex::new(Handle::new(nblocks)));
    system.set_task_handle(Some(handle.clone()));
    drop(system);

    let mut handle_guard = handle.lock();

    let res = start_handle(journal.clone(), &mut journal_guard, &mut handle_guard);
    let mut system = journal_guard.system.lock();

    match res {
        Ok(_) => Ok(handle.clone()),
        Err(e) => {
            system.set_task_handle(None);
            Err(e)
        }
    }
}

/// Internal helper functions.
impl Journal {
    fn init_common(system: Arc<Mutex<dyn System>>, devs: JournalDevs) -> JBDResult<Self> {
        let sb_buffer = system
            .lock()
            .get_buffer_provider()
            .lock()
            .get_buffer(devs.dev.clone(), devs.blk_offset as usize);
        if sb_buffer.is_none() {
            return Err(JBDError::IOError);
        }
        let system_guard = system.lock();
        let ret = Self {
            system: system.clone(),
            sb_buffer: sb_buffer.unwrap(),
            format_version: 0,
            states: Mutex::new(JournalStates {
                barrier_count: 0,
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
                average_commit_time: 0,
            }),
            lists: Mutex::new(JournalLists {
                checkpoint_transactions: LinkedList::new(),
            }),
            revoke_tables: Mutex::new(JournalRevokeTables {
                current: 1,
                // TODO: Size
                revoke_table: [RevokeTable, RevokeTable],
            }),
            wbuf: Vec::new(),
            wait_transaction_locked: system_guard.new_wait_queue(),
            wait_logspace: system_guard.new_wait_queue(),
            wait_done_commit: system_guard.new_wait_queue(),
            wait_checkpoint: system_guard.new_wait_queue(),
            wait_commit: system_guard.new_wait_queue(),
            wait_updates: system_guard.new_wait_queue(),
            devs,
            maxlen: 0,
            uuid: [0; 16],
            max_transaction_buffers: 0,
            // FIXME: HZ
            commit_interval: JBD_DEFAULT_MAX_COMMIT_AGE,
        };
        Ok(ret)
    }

    fn bmap(&self, block_id: u32) -> JBDResult<u32> {
        // TODO: j_inode
        Ok(block_id)
    }

    /// Given a journal_t structure, initialize the various fields for
    /// startup of a new journaling session.  We use this both when creating
    /// a journal, and after recovering an old journal to reset it for
    /// subsequent use.
    fn reset(&mut self) -> JBDResult {
        let mut sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_mut(&mut sb_guard);

        let first = u32::from_be(sb.first);
        let last = u32::from_be(sb.maxlen);
        drop(sb_guard);

        if first + JFS_MIN_JOURNAL_BLOCKS > last + 1 {
            log::error!("Journal too small: blocks {}-{}.", first, last);
            // TODO: Discard
            return Err(JBDError::InvalidJournalSize);
        }

        let mut states = self.states.lock();
        states.first = first;
        states.last = last;

        states.head = first;
        states.tail = first;
        states.free = last - first;

        states.tail_sequence = states.transaction_sequence;
        states.commit_sequence = states.transaction_sequence - 1;
        states.commit_request = states.commit_sequence;
        drop(states);

        self.max_transaction_buffers = self.maxlen / 4;

        self.update_superblock();

        Ok(()) // FIXME: Should start deamon thread here.
    }

    /// Load the on-disk journal superblock and read the key fields.
    fn load_superblock(&mut self) -> JBDResult {
        self.validate_superblock()?;

        let sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_ref(&sb_guard);
        let mut states = self.states.lock();

        states.tail_sequence = u32::from_be(sb.sequence) as u16;
        states.tail = u32::from_be(sb.start);
        states.first = u32::from_be(sb.first);
        states.last = u32::from_be(sb.maxlen);
        states.errno = i32::from_be(sb.errno);

        Ok(())
    }

    /// Update a journal's dynamic superblock fields and write it to disk,
    /// ~~optionally waiting for the IO to complete~~.
    fn update_superblock(&mut self) {
        let mut sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_mut(&mut sb_guard);
        let mut states = self.states.lock();

        if sb.start == 0 && states.tail_sequence == states.transaction_sequence {
            log::debug!("Skipping superblock update on newly created / recovered journal.");
            states.flags.insert(JournalFlag::FLUSHED);
            return;
        }

        // TODO: buffer_write_io_error

        log::debug!("Updating superblock.");
        sb.sequence = (states.tail_sequence as u32).to_be();
        sb.start = states.tail.to_be();
        sb.errno = states.errno;

        // No need to mark dirty here, as superblock_mut has already set that.
        // TODO: Non-blocking I/O; trace_jbd_update_superblock_end (what is this?)

        if sb.start != 0 {
            states.flags.insert(JournalFlag::FLUSHED);
        } else {
            states.flags.remove(JournalFlag::FLUSHED);
        }
    }

    fn validate_superblock(&mut self) -> JBDResult {
        // No need to test buffer_uptodate here as in our implementation as far,
        // the buffer will always be valid.
        let sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_ref(&sb_guard);

        if sb.header.magic != JFS_MAGIC_NUMBER.to_be() || sb.block_size != (self.devs.dev.block_size() as u32).to_be() {
            log::error!("Invalid journal superblock magic number or block size.");
            return Err(JBDError::InvalidSuperblock);
        }

        let block_type: BlockType = u32::from_be(sb.header.block_type).try_into()?;

        match block_type {
            BlockType::SuperblockV1 => self.format_version = 1,
            BlockType::SuperblockV2 => self.format_version = 2,
            _ => {
                log::error!("Invalid journal superblock block type.");
                return Err(JBDError::InvalidSuperblock);
            }
        }

        if u32::from_be(sb.maxlen) <= self.maxlen {
            self.maxlen = u32::from_be(sb.maxlen);
        } else {
            log::error!("Journal too short.");
            // Linux returns -EINVAL here, so as we.
            return Err(JBDError::InvalidSuperblock);
        }

        if u32::from_be(sb.first) == 0 || u32::from_be(sb.first) >= self.maxlen {
            log::error!("Journal has invalid start block.");
            return Err(JBDError::InvalidSuperblock);
        }

        Ok(())
    }

    fn abort_soft(&mut self, errno: i32) {
        let mut states = self.states.lock();
        if states.flags.contains(JournalFlag::ABORT) {
            return;
        }

        if states.errno != 0 {
            states.errno = errno;
        }
        drop(states);

        self.abort_hard();

        if errno != 0 {
            self.update_superblock();
        }
    }

    fn abort_hard(&mut self) {
        todo!()
    }

    fn recover_one_pass(&mut self, pass_type: RecoveryPassType) -> JBDResult<RecoveryInfo> {
        let mut info = RecoveryInfo::zero_init();

        // First thing is to establish what we expect to find in the log
        // (in terms of transaction IDs), and where (in terms of log
        // block offsets): query the superblock.
        let mut sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_mut(&mut sb_guard);

        let next_commit_id = u32::from_be(sb.sequence);
        let next_log_block = u32::from_be(sb.start);

        let first_commit_id = next_commit_id;
        if pass_type == RecoveryPassType::Scan {
            info.start_transaction = first_commit_id as u16;
        }

        log::debug!("Starting recovery pass {:?}.", pass_type);

        // Now we walk through the log, transaction by transaction,
        // making sure that each transaction has a commit block in the
        // expected place.  Each complete transaction gets replayed back
        // into the main filesystem.

        todo!()
    }

    fn superblock_ref<'a>(buf: &'a MutexGuard<BufferHead>) -> &'a Superblock {
        buf.convert::<Superblock>()
    }

    fn superblock_mut<'a>(buf: &'a mut MutexGuard<BufferHead>) -> &'a mut Superblock {
        buf.convert_mut::<Superblock>()
    }

    fn get_buffer(&mut self, block_id: u32) -> JBDResult<Arc<Mutex<BufferHead>>> {
        self.system
            .lock()
            .get_buffer_provider()
            .lock()
            .get_buffer(self.devs.dev.clone(), block_id as usize)
            .map_or(Err(JBDError::IOError), |bh| Ok(bh))
    }

    fn sync_buf(&mut self) -> JBDResult {
        if self.system.lock().get_buffer_provider().lock().sync() {
            Ok(())
        } else {
            Err(JBDError::IOError)
        }
    }
}

fn start_handle(
    journal_ref: Arc<Mutex<Journal>>,
    journal_guard: &mut MutexGuard<Journal>,
    handle: &mut Handle,
) -> JBDResult {
    let nblocks = handle.buffer_credits;

    if nblocks > journal_guard.max_transaction_buffers {
        log::error!(
            "Transaction requires too many credits ({} > {}).",
            nblocks,
            journal_guard.max_transaction_buffers
        );
        return Err(JBDError::NotEnoughSpace);
    }

    let system = journal_guard.system.lock();

    log::debug!("New handle going live.");

    'repeat: loop {
        let mut states: MutexGuard<JournalStates> = journal_guard.states.lock();
        if states.flags.contains(JournalFlag::ABORT)
            || (states.errno != 0 && !states.flags.contains(JournalFlag::ACK_ERR))
        {
            log::error!("Journal has aborted.");
            return Err(JBDError::IOError);
        }

        // Wait on the journal's transaction barrier if necessary
        if states.barrier_count > 0 {
            todo!("Wait for barrier.");
            drop(states);
            system.wait_event(journal_guard.wait_transaction_locked.clone(), &|| {
                // FIXME: Can we lock the states again? RW lock?
                journal_guard.states.lock().barrier_count == 0
            });
            continue 'repeat;
        }

        if states.running_transaction.is_none() {
            let tx = Transaction::new(Arc::downgrade(&journal_ref.clone()));
            let tx = Arc::new(Mutex::new(tx));
            set_transaction(&mut states, &system, &tx);
        }

        let transaction_rc = states.running_transaction.as_ref().unwrap().clone();
        let transaction = transaction_rc.lock();

        if transaction.state == TransactionState::Locked {
            todo!("Wait for transaction to unlock.");
        }

        let mut handle_info = transaction.handle_info.lock();
        let needed = handle_info.outstanding_credits as u32 + nblocks;

        if needed > journal_guard.max_transaction_buffers {
            todo!("Wait for transaction to commit.");
        }

        if log_space_left(&states) < needed {
            todo!("Wait for checkpoint.");
        }

        handle.transaction = Some(transaction_rc.clone());
        handle_info.outstanding_credits += nblocks;
        handle_info.updates += 1;
        handle_info.handle_count += 1;

        log::debug!("Handle now has {} credits.", handle_info.outstanding_credits);

        return Ok(());
    }
}

/// Start a new transaction in the journal, equivalent to get_transaction()
/// in linux.
fn set_transaction(
    states: &mut MutexGuard<JournalStates>,
    system: &MutexGuard<dyn System>,
    tx_rc: &Arc<Mutex<Transaction>>,
) {
    let mut tx = tx_rc.lock();
    tx.state = TransactionState::Running;
    tx.start_time = system.get_time();
    tx.tid = states.transaction_sequence;
    states.transaction_sequence += 1;
    // Here we replace jiffies with get_time.
    // TODO: Timer
    // tx.expires = tx.start_time + journal.commit_interval;
    states.running_transaction = Some(tx_rc.clone());
}

fn log_space_left(states: &MutexGuard<JournalStates>) -> u32 {
    let mut left = states.free;
    left -= MIN_LOG_RESERVED_BLOCKS;
    if left <= 0 {
        0
    } else {
        left - (left >> 3)
    }
}
