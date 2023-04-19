extern crate alloc;
use core::mem::size_of;

use alloc::{boxed::Box, collections::LinkedList, sync::Arc, vec::Vec};
use bitflags::bitflags;
use spin::{Mutex, MutexGuard};

use crate::{
    buf::{BlockDevice, BufferHead, BufferProvider, DefaultBufferProvider},
    config::{JBD_DEFAULT_MAX_COMMIT_AGE, JFS_MAGIC_NUMBER, JFS_MIN_JOURNAL_BLOCKS},
    disk::{BlockTag, BlockType, Superblock},
    err::{JBDError, JBDResult},
    tx::{Tid, Transaction},
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
    buf_provider: Box<dyn BufferProvider>,
    // TODO: j_inode, j_task, j_commit_timer, j_last_sync_writer, j_private
    flags: JournalFlag,
    err: Option<JBDError>,
    sb_buffer: Arc<Mutex<BufferHead>>,
    // superblock: Superblock,
    format_version: i32,
    /// Journal states protected by j_state_lock in Linux.
    states: Mutex<JournalStates>,

    /// Journal lists protected by j_list_lock in Linux.
    lists: Mutex<JournalLists>,

    // TODO: Wait queues
    devs: JournalDevs,

    maxlen: u32,

    uuid: [u8; 16],
    max_transaction_buffers: i32,
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
    running_transaction: Option<Arc<Transaction>>,
    committing_transaction: Option<Arc<Transaction>>,
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

impl Journal {
    pub fn init_dev(
        buf_provider: Box<dyn BufferProvider>,
        dev: Arc<dyn BlockDevice>,
        fs_dev: Arc<dyn BlockDevice>,
        start: u32,
        len: u32,
    ) -> Self {
        let devs = JournalDevs {
            dev,
            blk_offset: start,
            fs_dev,
        };
        let mut journal = Self::init_common(buf_provider, devs);

        let n_blks = journal.devs.dev.block_size() / size_of::<BlockTag>();
        journal.wbuf.resize(n_blks, None);
        journal.maxlen = len;

        journal.sb_buffer = journal
            .buf_provider
            .get_buffer(journal.devs.dev.clone(), start as usize)
            .unwrap();

        journal
    }

    pub fn init_dev_default(
        dev: Arc<dyn BlockDevice>,
        fs_dev: Arc<dyn BlockDevice>,
        start: u32,
        len: u32,
        block_size: u32,
    ) -> Self {
        let provider = Box::new(DefaultBufferProvider::new());
        Self::init_dev(provider, dev, fs_dev, start, len)
    }

    fn init_common(provider: Box<dyn BufferProvider>, devs: JournalDevs) -> Self {
        let mut provider = provider;
        let sb_buffer = provider.get_buffer(devs.dev.clone(), devs.blk_offset as usize).unwrap();
        Self {
            buf_provider: provider,
            flags: JournalFlag::ABORT,
            err: None,
            sb_buffer,
            format_version: 0,
            states: Mutex::new(JournalStates {
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
            devs,
            maxlen: 0,
            uuid: [0; 16],
            max_transaction_buffers: 0,
            // FIXME: HZ
            commit_interval: JBD_DEFAULT_MAX_COMMIT_AGE,
        }
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
            let page_head_locked = self.buf_provider.get_buffer(self.devs.dev.clone(), block_id as usize)?;
            let mut page_head = page_head_locked.lock();
            let buf = page_head.buf_mut();
            buf.fill(0);
            // TODO: What is set_buffer_uptodate?
            // No need to __brelse, as rust has done the ref count for us :)
        }

        self.buf_provider.sync()?;
        log::debug!("Journal cleared.");

        let mut sb_guard = self.sb_buffer.lock();
        let sb = Self::superblock_mut(&mut sb_guard);

        sb.header.magic = JFS_MAGIC_NUMBER.to_be();
        sb.header.block_type = BlockType::SUPERBLOCK_V2.to_u32().to_be();

        sb.block_size = (self.devs.dev.block_size() as u32).to_be();
        sb.maxlen = self.maxlen.to_be();
        sb.first = 1_u32.to_be();
        drop(sb_guard);

        let mut states = self.states.lock();
        states.transaction_sequence = 1;
        drop(states);

        self.flags &= !JournalFlag::ABORT; // TODO: Check
        self.format_version = 2;

        self.reset()
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

        self.max_transaction_buffers = self.maxlen as i32 / 4;

        self.update_superblock();

        Ok(())
    }
}

impl Journal {
    fn bmap(&self, block_id: u32) -> JBDResult<u32> {
        // TODO: j_inode
        Ok(block_id)
    }

    /// Update a journal's dynamic superblock fields and write it to disk,
    /// ~~optionally waiting for the IO to complete~~.
    fn update_superblock(&mut self) {}

    fn superblock_ref<'a>(buf: &'a MutexGuard<BufferHead>) -> &'a Superblock {
        buf.convert::<Superblock>()
    }

    fn superblock_mut<'a>(buf: &'a mut MutexGuard<BufferHead>) -> &'a mut Superblock {
        buf.convert_mut::<Superblock>()
    }
}
