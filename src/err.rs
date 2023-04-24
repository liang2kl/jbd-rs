#[derive(Debug)]
pub enum JBDError {
    // Buffer
    InsufficientCache,
    CacheNotFound,
    // Journal
    InvalidJournalSize,
    InvalidSuperblock,
    NotEnoughSpace,
    // Misc
    IOError,
}

pub type JBDResult<T = ()> = Result<T, JBDError>;
