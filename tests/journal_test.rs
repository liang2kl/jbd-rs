mod sal;
use std::sync::Arc;

use jbd_rs::{self, Journal};
use sal::UserSystem;

#[test]
fn test_create() {
    const NBLOCKS: usize = 4096;
    let system = Arc::new(UserSystem::new("target/test.img", NBLOCKS).unwrap());
    let dev = system.block_device();
    let mut journal = Journal::init_dev(system, dev.clone(), dev.clone(), 0, NBLOCKS as u32).unwrap();
    journal.create().unwrap();
}
