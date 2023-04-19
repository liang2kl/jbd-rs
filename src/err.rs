#[derive(Debug)]
pub enum JBDError {
    InvalidJournalSize,
    InsufficientCache,
    CacheNotFound,
    InvalidSuperblock,
    IOError,
}

pub type JBDResult<T = ()> = Result<T, JBDError>;
