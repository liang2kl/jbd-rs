#[derive(Debug)]
pub enum JBDError {
    InvalidJournalSize,
    InsufficientCache,
    CacheNotFound,
}

pub type JBDResult<T = ()> = Result<T, JBDError>;
