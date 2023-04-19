//! On-disk structures for the journal.

use bitflags::bitflags;

use crate::err::{JBDError, JBDResult};

/// Descriptor block types.
#[derive(Clone, Copy)]
pub enum BlockType {
    DescriptorBlock = 1,
    CommitBlock = 2,
    SuperblockV1 = 3,
    SuperblockV2 = 4,
    Revokeblock = 5,
}

impl Into<u32> for BlockType {
    fn into(self) -> u32 {
        self as u32
    }
}

impl TryFrom<u32> for BlockType {
    type Error = JBDError;
    fn try_from(value: u32) -> JBDResult<Self> {
        match value {
            1 => Ok(BlockType::DescriptorBlock),
            2 => Ok(BlockType::CommitBlock),
            3 => Ok(BlockType::SuperblockV1),
            4 => Ok(BlockType::SuperblockV2),
            5 => Ok(BlockType::Revokeblock),
            _ => Err(JBDError::InvalidSuperblock),
        }
    }
}

bitflags! {
    #[derive(Default)]
    #[repr(C)]
    pub struct TagFlag: u32 {
        const ESCAPE = 1;
        const SAME_UUID = 1 << 1;
        const DELETED = 1 << 2;
        const LAST_TAG = 1 << 3;
    }
}

/// Standard header for all descriptor blocks.
#[repr(C)]
pub struct Header {
    pub magic: u32,
    pub block_type: u32,
    pub sequence: u32,
}

/// Used to describe a single buffer in the journal.
#[repr(C)]
pub struct BlockTag {
    /// The on-disk block number
    pub block_nr: u32,
    pub flag: TagFlag,
}

/// The revoke descriptor: used on disk to describe a series of blocks to be revoked from the log
#[repr(C)]
pub struct RevokeBlockHeader {
    pub header: Header,
    pub count: u32,
}

/// The journal superblock. All fields are in big-endian byte order.
#[repr(C)]
pub struct Superblock {
    pub header: Header,

    /* Static information describing the journal */
    /// Journal device blocksize
    pub block_size: u32,
    /// Yotal blocks in journal file
    pub maxlen: u32,
    /// First block of log information
    pub first: u32,

    /* Dynamic information describing the current state of the log */
    /// First commit ID expected in log
    pub sequence: u32,
    /// Block_nr of start of log
    pub start: u32,

    /* Error value, as set by journal_abort(). */
    // TODO: enum?
    pub errno: u32,

    /* Remaining fields are only valid in a version-2 superblock */
    /// Compatible feature set
    pub feature_compat: u32,
    /// Incompatible feature set
    pub feature_incompat: u32,
    /// Readonly-compatible feature set
    pub feature_ro_compat: u32,
    /// UUID of journal superblock
    pub uuid: [u8; 16],
    /// Number of filesystems sharing log
    pub nr_users: u32,
    /// Blocknr of dynamic superblock copy
    pub dyn_super: u32,
    /// Limit of journal blocks per trans
    pub max_transaction: u32,
    /// Limit of data blocks per trans
    pub max_trans_data: u32,
    pub padding: [u32; 44],
    /// Ids of all fs'es sharing the log
    pub users: [u8; 16 * 48],
}
