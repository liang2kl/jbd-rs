use core::cell::RefCell;

extern crate alloc;
use alloc::rc::Rc;

use crate::{err::JBDResult, tx::JournalBuffer, Journal};

impl Journal {
    pub fn log_do_checkpoint(&mut self) -> JBDResult {
        log::debug!("Start checkpoint.");

        self.cleanup_tail()?;

        // Start writing disk blocks
        if self.checkpoint_transactions.is_empty() {
            return Ok(());
        }

        let tx_rc = self.checkpoint_transactions[0].clone();
        let mut tx = tx_rc.borrow_mut();

        for jb_rc in tx.checkpoint_list.0.clone().into_iter() {
            let mut jb = jb_rc.borrow_mut();
        }

        Ok(())
    }

    fn cleanup_tail(&mut self) -> JBDResult {
        todo!()
    }

    // Try to flush one buffer from the checkpoint list to disk.
    fn process_buffer(&mut self, jb_rc: Rc<RefCell<JournalBuffer>>, jb: &mut JournalBuffer) -> JBDResult {
        todo!()
    }
}
