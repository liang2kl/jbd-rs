mod common;

use std::{borrow::Borrow, env, rc::Rc};

use common::{create_handle, create_journal, mock, JOURNAL_SIZE};

fn setup() {
    let _ = env_logger::builder().is_test(true).try_init();
    env::set_var("RUST_LOG", "debug");
}

#[test]
fn test_create_handle() {
    setup();
    let (_, journal) = create_journal().unwrap();
    let handle1 = create_handle(journal.clone()).unwrap();
    let handle2 = create_handle(journal.clone()).unwrap();
    // Each process has a singleton handle.
    assert!(Rc::ptr_eq(&handle1, &handle2));
}

#[test]
fn test_write_meta() {
    setup();
    let (system, journal) = create_journal().unwrap();
    let handle_rc = create_handle(journal.clone()).unwrap();
    let mut handle = handle_rc.as_ref().borrow_mut();
    // Write a random block.
    let block_id = JOURNAL_SIZE;
    let meta_buf = mock::write_random_block(&system, system.block_device().borrow(), block_id);

    // Write the block to the journal.
    handle.get_write_access(&meta_buf).unwrap();
    handle.dirty_metadata(&meta_buf).unwrap();
    handle.stop().unwrap();

    journal.borrow_mut().commit_transaction().unwrap();
}

#[test]
fn test_write_data() {
    setup();
    let (system, journal) = create_journal().unwrap();
    let handle_rc = create_handle(journal.clone()).unwrap();
    let mut handle = handle_rc.as_ref().borrow_mut();
    let block_id = JOURNAL_SIZE;
    let data_buf = mock::write_random_block(&system, system.block_device().borrow(), block_id);

    handle.get_write_access(&data_buf).unwrap();
    handle.dirty_data(&data_buf).unwrap();
    handle.stop().unwrap();

    journal.borrow_mut().commit_transaction().unwrap();
}
