pub mod mock;
pub mod sal;

use std::{cell::RefCell, sync::Arc};

use jbd_rs::{self, err::JBDResult, Handle, Journal};
use sal::UserSystem;

pub const JOURNAL_SIZE: usize = 1024;

pub fn existing_journal(system: Arc<UserSystem>) -> Arc<RefCell<Journal>> {
    let dev = system.block_device();
    let journal = Journal::init_dev(system.clone(), dev.clone(), dev.clone(), 0, JOURNAL_SIZE as u32).unwrap();
    Arc::new(RefCell::new(journal))
}

pub fn create_journal() -> JBDResult<(Arc<UserSystem>, Arc<RefCell<Journal>>)> {
    const NBLOCKS: usize = 2048;
    let system = Arc::new(UserSystem::new("target/test.img", NBLOCKS).unwrap());
    let dev = system.block_device();
    let mut journal = Journal::init_dev(system.clone(), dev.clone(), dev.clone(), 0, JOURNAL_SIZE as u32).unwrap();
    journal.create()?;
    Ok((system, Arc::new(RefCell::new(journal))))
}

pub fn create_handle(journal: Arc<RefCell<Journal>>) -> JBDResult<Arc<RefCell<Handle>>> {
    Journal::start(journal, 128)
}

pub fn setup_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}
