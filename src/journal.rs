extern crate alloc;

use alloc::{
    collections::LinkedList,
    sync::{Arc, Weak},
};
use bitflags::bitflags;
use spin::{Mutex, MutexGuard};

use crate::{
    buffer::Buffer,
    config::{JFS_MAGIC_NUMBER, JFS_MIN_JOURNAL_BLOCKS, MIN_LOG_RESERVED_BLOCKS},
    disk::{BlockType, Superblock},
    err::{JBDError, JBDResult},
    sal::{BlockDevice, System},
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
    system: Arc<dyn System>,
    sb_buffer: Arc<Mutex<Buffer>>,
    format_version: i32,
    /// Journal states protected by a single spin lock
    states: Mutex<JournalStates>,
    /// List of all transactions waiting for checkpointing
    checkpoint_transactions: Mutex<LinkedList<Arc<Mutex<Transaction>>>>,
    /// Block devices
    devs: JournalDevs,
    /// Total maximum capacity of the journal region on disk
    maxlen: u32,
    /// Maximum number of metadata buffers to allow in a single compound
    /// commit transaction
    max_transaction_buffers: u32,
    // commit_interval: usize,
    // wbuf: Vec<Option<Arc<BufferHead>>>,
}

struct JournalDevs {
    dev: Arc<dyn BlockDevice>,
    blk_offset: u32,
    fs_dev: Arc<dyn BlockDevice>,
}

/// Journal states protected by a single spin lock in Linux.
struct JournalStates {
    flags: JournalFlag,
    errno: i32, // TODO: Strongly-typed error?
    running_transaction: Option<Arc<Mutex<Transaction>>>,
    committing_transaction: Option<Arc<Mutex<Transaction>>>,
    /// Journal head: identifies the first unused block in the journal.
    head: u32,
    /// Journal tail: identifies the oldest still-used block in the journal
    tail: u32,
    /// Journal free: how many free blocks are there in the journal?
    free: u32,
    /// Journal start: the block number of the first usable block in the journal
    first: u32,
    /// Journal end: the block number of the last usable block in the journal
    last: u32,

    /// Sequence number of the oldest transaction in the log
    tail_sequence: Tid,
    /// Sequence number of the next transaction to grant
    transaction_sequence: Tid,
    /// Sequence number of the most recently committed transaction
    commit_sequence: Tid,
    /// Sequence number of the most recent transaction wanting commit
    commit_request: Tid,
}

struct RevokeTable;

pub struct JournalHead {
    bh: Weak<Mutex<Buffer>>,
}

impl JournalHead {
    pub fn new(bh: Weak<Mutex<Buffer>>) -> Self {
        Self { bh }
    }
}

/// Public interfaces.
impl Journal {
    /// Initialize an in-memory journal structure with a block device.
    pub fn init_dev(
        system: Arc<dyn System>,
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
            states: Mutex::new(JournalStates {
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
            }),
            checkpoint_transactions: Mutex::new(LinkedList::new()),
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
            let page_head_locked = self.get_buffer(block_id)?;
            let mut page_head = page_head_locked.lock();
            let buf = page_head.buf_mut();
            buf.fill(0);
        }

        // FIXME
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

        // TODO: self.recover()?;
        self.reset()?;

        let mut states = self.states.lock();
        states.flags.remove(JournalFlag::ABORT);
        states.flags.insert(JournalFlag::LOADED);

        Ok(())
    }
}

pub fn start(journal: Arc<Mutex<Journal>>, nblocks: u32) -> JBDResult<Arc<Mutex<Handle>>> {
    // FIXME: Is there a chance that we are already runing a transaction?
    let mut handle = Handle::new(nblocks);
    let mut journal_guard = journal.lock();
    start_handle(journal.clone(), &mut journal_guard, &mut handle)?;
    todo!()
}

/// Internal helper functions.
impl Journal {
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

        log::debug!("Updating superblock.");
        sb.sequence = (states.tail_sequence as u32).to_be();
        sb.start = states.tail.to_be();
        sb.errno = states.errno;

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

    fn superblock_ref<'a>(buf: &'a MutexGuard<Buffer>) -> &'a Superblock {
        buf.convert::<Superblock>()
    }

    fn superblock_mut<'a>(buf: &'a mut MutexGuard<Buffer>) -> &'a mut Superblock {
        buf.convert_mut::<Superblock>()
    }

    fn get_buffer(&mut self, block_id: u32) -> JBDResult<Arc<Mutex<Buffer>>> {
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

    let system = &journal_guard.system;

    log::debug!("New handle going live.");

    let mut states: MutexGuard<JournalStates> = journal_guard.states.lock();
    if states.flags.contains(JournalFlag::ABORT) || (states.errno != 0 && !states.flags.contains(JournalFlag::ACK_ERR))
    {
        log::error!("Journal has aborted.");
        return Err(JBDError::IOError);
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
        todo!("Wait for previous transaction to commit.");
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

/// Start a new transaction in the journal, equivalent to get_transaction()
/// in linux.
fn set_transaction(states: &mut MutexGuard<JournalStates>, system: &Arc<dyn System>, tx_rc: &Arc<Mutex<Transaction>>) {
    let mut tx = tx_rc.lock();
    tx.state = TransactionState::Running;
    tx.start_time = system.get_time();
    tx.tid = states.transaction_sequence;
    states.transaction_sequence += 1;
    // TODO: tx.expires
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
