extern crate alloc;

use core::{
    borrow::{Borrow, BorrowMut},
    cell::{Ref, RefCell},
    mem::{self, size_of},
    ptr,
};

use alloc::{collections::LinkedList, rc::Rc, sync::Weak, vec::Vec};
use bitflags::bitflags;

use crate::{
    config::{JFS_MAGIC_NUMBER, JFS_MIN_JOURNAL_BLOCKS, MIN_LOG_RESERVED_BLOCKS},
    disk::{BlockTag, BlockType, Header, Superblock, TagFlag},
    err::{JBDError, JBDResult},
    sal::{BlockDevice, Buffer, System},
    tx::{BufferListType, Handle, JournalBuffer, Tid, Transaction, TransactionState},
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
    pub wbuf: Vec<Rc<dyn Buffer>>,
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
            .get_buffer(&devs.dev, devs.blk_offset as usize);
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
            wbuf: Vec::new(),
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
    pub fn commit_transaction(&mut self) -> JBDResult {
        // First job: lock down the current transaction and wait for
        // all outstanding updates to complete.
        if self.flags.contains(JournalFlag::FLUSHED) {
            self.update_superblock();
        }
        assert!(self.running_transaction.is_some());
        assert!(self.committing_transaction.is_none());

        let mut commit_tx = self.running_transaction.as_ref().unwrap().as_ref().borrow_mut();
        assert!(commit_tx.state == TransactionState::Running);

        log::debug!("Start committing transaction {}.", commit_tx.tid);
        commit_tx.state = TransactionState::Locked;

        assert!(commit_tx.updates == 0);
        assert!(commit_tx.outstanding_credits <= self.max_transaction_buffers);

        for jb_rc in commit_tx.reserved_list.0.clone().into_iter() {
            let mut jb = jb_rc.as_ref().borrow_mut();
            if jb.commited_data.is_some() {
                jb.commited_data = None;
            }
            Transaction::refile_buffer(&jb_rc, &mut jb, &mut commit_tx);
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
        let commit_tx_rc = self.committing_transaction.as_mut().unwrap().clone();
        let mut commit_tx = commit_tx_rc.as_ref().borrow_mut();
        let start_time = self.system.get_time();
        commit_tx.log_start = start_time as u32;

        log::debug!("Commit phase 2.");

        // Now start flushing things to disk, in the order they appear
        // on the transaction lists.  Data blocks go first.
        // self.
        self.submit_data_buffers(&commit_tx_rc, &mut commit_tx)?;

        for jb_rc in commit_tx.locked_list.0.clone().into_iter() {
            let mut jb = jb_rc.as_ref().borrow_mut();
            let buf = &jb.buf;

            if buf.jbd_managed() && jb.jlist == BufferListType::Locked {
                Transaction::unfile_buffer(&jb_rc, &mut jb, &mut commit_tx);
            }
        }

        // TODO: write revoke records

        assert!(commit_tx.sync_datalist.0.is_empty());

        log::debug!("Commit phase 3.");

        commit_tx.state = TransactionState::Commit;

        assert!(commit_tx.nr_buffers <= commit_tx.outstanding_credits as i32);

        let mut descriptor_rc: Option<Rc<RefCell<JournalBuffer>>> = None;
        let mut first_tag = false;
        let mut descriptor_buf_data: Option<*mut u8> = None;
        let mut space_left = 0;

        // Metadata list
        let buffers = commit_tx.buffers.0.clone();
        let buffer_count = buffers.len();

        let mut refile_buffers = Vec::new();

        for (i, jb_rc) in commit_tx.buffers.0.clone().into_iter().enumerate() {
            let mut jb = jb_rc.as_ref().borrow_mut();
            if self.flags.contains(JournalFlag::ABORT) {
                jb.buf.clear_jbd_dirty();
                refile_buffers.push(jb_rc.clone());
                continue;
            }

            if descriptor_rc.is_none() {
                log::debug!("Get descriptor.");
                descriptor_rc = Some(self.get_descriptor_buffer()?);
                let descriptor_rc = descriptor_rc.as_mut().unwrap();
                let descriptor = descriptor_rc.as_ref().borrow_mut();
                let buf = &descriptor.buf;
                let header: &mut Header = buf.convert_mut();
                header.magic = JFS_MAGIC_NUMBER.to_be();
                let block_type_host: u32 = BlockType::DescriptorBlock.into();
                header.block_type = block_type_host.to_be();
                header.sequence = (commit_tx.tid as u32).to_be();

                buf.mark_dirty();
                self.wbuf.push(buf.clone());
                // Transaction::file_buffer(&commit_tx_rc, &descriptor_rc, &mut descriptor, BufferListType::LogCtl)?;
                first_tag = true;
                descriptor_buf_data = Some(buf.buf_mut()[size_of::<Header>()..].as_mut_ptr());
                space_left = buf.size() - size_of::<Header>();
            }

            // Where is the buffer to be written?
            let blocknr = self.next_log_block();

            commit_tx.outstanding_credits -= 1;

            let (new_jb_rc, do_escape, _) =
                self.write_metadata_buffer(&commit_tx_rc, &mut commit_tx, &jb_rc, &mut jb, blocknr)?;
            self.wbuf.push(new_jb_rc.as_ref().borrow().buf.clone());

            let mut tag_flag = TagFlag::default();
            if do_escape {
                tag_flag.insert(TagFlag::ESCAPE);
            }
            if !first_tag {
                tag_flag.insert(TagFlag::SAME_UUID);
            }

            let tag_mut = unsafe { mem::transmute::<_, &mut BlockTag>(descriptor_buf_data.as_mut().unwrap()) };
            tag_mut.block_nr = (jb.buf.block_id() as u32).to_be();
            tag_mut.flag = tag_flag.bits().to_be();
            space_left -= size_of::<BlockTag>();

            unsafe {
                *descriptor_buf_data.as_mut().unwrap() = descriptor_buf_data
                    .as_ref()
                    .unwrap()
                    .offset(size_of::<BlockTag>() as isize);
            }

            if first_tag {
                // TODO: uuid
                // let uuid_mut = unsafe { mem::transmute::<_, &mut [u8; 16]>(descriptor_buf_data.as_mut().unwrap()) };
                unsafe {
                    *descriptor_buf_data.as_mut().unwrap() = descriptor_buf_data.as_ref().unwrap().offset(16);
                }
                first_tag = false;
                space_left -= 16;
            }

            // Submit IO
            if i == buffer_count - 1 || space_left < size_of::<BlockTag>() + 16 {
                log::debug!("Submit {} IO.", self.wbuf.len());
                // Write an end-of-descriptor marker before submitting the IOs.
                tag_flag.insert(TagFlag::LAST_TAG);
                tag_mut.flag = tag_flag.bits().to_be();

                for buf in self.wbuf.iter() {
                    buf.mark_dirty();
                    buf.sync();
                }
                self.wbuf.clear();

                descriptor_rc = None;
            }
        }

        for jb_rc in refile_buffers.into_iter() {
            let mut jb = jb_rc.as_ref().borrow_mut();
            Transaction::refile_buffer(&jb_rc, &mut jb, &mut commit_tx);
        }

        // We don't need to wait for the buffer to be written, as they are synced.

        log::debug!("Commit phase 4-5.");

        // IO bufs
        let iobuf_len = commit_tx.iobuf_list.0.len();
        for i in 0..iobuf_len {
            let jb_rc = &commit_tx.iobuf_list.0[i].clone();
            let mut jb = jb_rc.as_ref().borrow_mut();
            Transaction::unfile_buffer(&jb_rc, &mut jb, &mut commit_tx);
            let jb_rc = &commit_tx.shadow_list.0[i].clone();
            let mut jb = jb_rc.as_ref().borrow_mut();
            Transaction::unfile_buffer(&jb_rc, &mut jb, &mut commit_tx);
            Transaction::file_buffer(&commit_tx_rc, &mut commit_tx, jb_rc, &mut jb, BufferListType::Forget)?;
        }

        commit_tx.iobuf_list.0.clear();
        commit_tx.shadow_list.0.clear();

        log::debug!("Commit phase 6.");

        assert!(commit_tx.state == TransactionState::Commit);
        commit_tx.state = TransactionState::CommitRecord;
        self.write_commit_record(&mut commit_tx)?;

        log::debug!("Commit phase 7.");

        assert!(commit_tx.sync_datalist.0.is_empty());
        assert!(commit_tx.buffers.0.is_empty());
        assert!(commit_tx.checkpoint_list.0.is_empty());
        assert!(commit_tx.iobuf_list.0.is_empty());
        assert!(commit_tx.shadow_list.0.is_empty());
        assert!(commit_tx.log_list.0.is_empty());

        // TODO: Checkpoints
        // let forget_list = commit_tx.forget.0.clone();
        // commit_tx.forget.0.clear();

        // for jb_rc in forget_list {
        //     let mut jb = jb_rc.as_ref().borrow_mut();
        //     if jb.commited_data.is_some() {
        //         jb.commited_data = None;
        //         if jb.frozen_data.is_some() {
        //             jb.commited_data = jb.frozen_data;
        //             jb.frozen_data = None;
        //         }
        //     } else if jb.frozen_data.is_some() {
        //         jb.frozen_data = None;
        //     }
        // }

        log::debug!("Commit phase 8.");
        commit_tx.state = TransactionState::Finished;

        self.commit_sequence = commit_tx.tid;
        self.committing_transaction = None;

        // TODO: Checkpoint

        log::debug!("Commit {} completed.", self.commit_sequence);

        Ok(())
    }

    fn write_commit_record(&mut self, commit_tx: &mut Transaction) -> JBDResult {
        if self.flags.contains(JournalFlag::ABORT) {
            return Ok(());
        }

        let descriptor_rc = self.get_descriptor_buffer()?;
        let descriptor = descriptor_rc.as_ref().borrow_mut();
        let header: &mut Header = descriptor.buf.convert_mut();
        header.magic = JFS_MAGIC_NUMBER.to_be();
        let block_type_host: u32 = BlockType::CommitBlock.into();
        header.block_type = block_type_host.to_be();
        header.sequence = (commit_tx.tid as u32).to_be();

        descriptor.buf.mark_dirty();
        descriptor.buf.sync();

        Ok(())
    }

    fn write_metadata_buffer(
        &mut self,
        tx_rc: &Rc<RefCell<Transaction>>,
        tx: &mut Transaction,
        jb_rc: &Rc<RefCell<JournalBuffer>>,
        jb: &mut JournalBuffer,
        blocknr: u32,
    ) -> JBDResult<(Rc<RefCell<JournalBuffer>>, bool, bool)> {
        let mut need_copy_out = false;
        let mut done_copy_out = false;
        let mut do_escape = false;

        let buf = self.get_buffer(blocknr)?;

        let new_jb_rc = JournalBuffer::new_or_get(&buf);

        let (data, is_frozen) = if let Some(frozen_data) = &jb.frozen_data {
            done_copy_out = true;
            (&frozen_data[..], true)
        } else {
            (jb.buf.buf(), false)
        };

        // Check for escaping
        if u32::from_be_bytes(data[..4].try_into().unwrap()) == JFS_MAGIC_NUMBER {
            need_copy_out = true;
            do_escape = true;
        }

        if need_copy_out && !done_copy_out {
            let mut new_data: Vec<u8> = Vec::with_capacity(data.len());
            new_data.resize(data.len(), 0);
            new_data.copy_from_slice(data);

            if do_escape {
                new_data[0] = 0;
            }

            jb.frozen_data = Some(new_data);
            done_copy_out = true;
        } else {
            if is_frozen {
                jb.frozen_data.as_mut().unwrap()[0] = 0;
            } else {
                jb.buf.buf_mut()[0] = 0;
            }
        };

        let mut new_jb = new_jb_rc.as_ref().borrow_mut();
        new_jb.transaction = None;
        new_jb.buf.mark_dirty();

        Transaction::file_buffer(tx_rc, tx, jb_rc, jb, BufferListType::Shadow)?;
        Transaction::file_buffer(tx_rc, tx, &new_jb_rc, &mut new_jb, BufferListType::IO)?;

        Ok((new_jb_rc.clone(), do_escape, done_copy_out))
    }

    fn submit_data_buffers(&mut self, tx_rc: &Rc<RefCell<Transaction>>, tx: &mut Transaction) -> JBDResult {
        let datalist = tx.sync_datalist.0.clone();
        tx.sync_datalist.0.clear();

        for jb_rc in datalist.into_iter() {
            let mut jb = jb_rc.as_ref().borrow_mut();
            let buf = &jb.buf;

            assert!(buf.jbd_managed());

            if buf.test_clear_dirty() {
                self.wbuf.push(buf.clone());
                Transaction::file_buffer(tx_rc, tx, &jb_rc, &mut jb, BufferListType::Locked)?;
            } else {
                Transaction::unfile_buffer(&jb_rc, &mut jb, tx);
            }
        }

        self.do_submit_data();

        Ok(())
    }

    fn do_submit_data(&mut self) {
        for buf in self.wbuf.iter() {
            buf.sync();
        }
        self.wbuf.clear();
    }

    fn get_descriptor_buffer(&mut self) -> JBDResult<Rc<RefCell<JournalBuffer>>> {
        let blocknr = self.next_log_block();
        let buf = self.get_buffer(blocknr)?;
        buf.buf_mut().fill(0);

        Ok(JournalBuffer::new_or_get(&buf))
    }

    fn next_log_block(&mut self) -> u32 {
        assert!(self.free > 1);
        let block = self.head;
        self.head += 1;
        self.free -= 1;
        if self.head == self.last {
            self.head = self.first;
        }
        // TODO: bmap
        block
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
            .get_buffer(&self.devs.dev, (block_id + self.devs.blk_offset) as usize)
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
            let mut tx_mut = tx.as_ref().borrow_mut();
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
    let mut journal = journal_rc.as_ref().borrow_mut();
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
    let mut transaction = journal.running_transaction.as_ref().unwrap().as_ref().borrow_mut();

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
