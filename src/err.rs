#[derive(Debug)]
pub enum JBDError {
    // Buffer
    InsufficientCache,
    CacheNotFound,
    // Journal
    InvalidJournalSize,
    InvalidSuperblock,
    NotEnoughSpace,
    // Handle
    HandleAborted,
    // Misc
    IOError,
    Unknown,
}

pub type JBDResult<T = ()> = Result<T, JBDError>;
