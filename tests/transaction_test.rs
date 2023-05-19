mod common;

use std::rc::Rc;

use common::create_handle;

use crate::common::create_journal;

#[test]
fn test_create_handle() {
    let journal = create_journal().unwrap();
    let handle1 = create_handle(journal.clone()).unwrap();
    let handle2 = create_handle(journal.clone()).unwrap();
    // Each process has a singleton handle.
    assert!(Rc::ptr_eq(&handle1, &handle2));
}
