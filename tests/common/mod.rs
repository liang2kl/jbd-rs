mod sal;

use jbd_rs::{self, err::JBDResult, Handle, Journal};
use sal::UserSystem;
use spin::{Mutex, RwLock};
use std::sync::Arc;

pub fn create_journal() -> JBDResult<Arc<RwLock<Journal>>> {
    const NBLOCKS: usize = 4096;
    let system = Arc::new(UserSystem::new("target/test.img", NBLOCKS).unwrap());
    let dev = system.block_device();
    let mut journal = Journal::init_dev(system, dev.clone(), dev.clone(), 0, NBLOCKS as u32).unwrap();
    journal.create()?;
    Ok(Arc::new(RwLock::new(journal)))
}

pub fn create_handle(journal: Arc<RwLock<Journal>>) -> JBDResult<Arc<Mutex<Handle>>> {
    Journal::start(journal, 1024)
}
