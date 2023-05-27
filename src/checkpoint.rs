use crate::{journal::JournalFlag, Journal};

impl Journal {
    pub fn do_all_checkpoints(&mut self) -> usize {
        let mut count = 0;
        while self.log_do_checkpoint() {
            count += 1;
        }

        count
    }

    pub fn log_do_checkpoint(&mut self) -> bool {
        log::debug!("Start checkpoint.");

        let cleaned = self.cleanup_tail();

        // Start writing disk blocks
        if self.checkpoint_transactions.is_empty() || cleaned {
            return false;
        }

        let tx_rc = self.checkpoint_transactions[0].clone();
        let mut tx = tx_rc.borrow_mut();

        while !tx.checkpoint_list.0.is_empty() {
            let jb_rc = tx.checkpoint_list.0[0].clone();
            let jb = jb_rc.borrow();
            tx.checkpoint_list.0.remove(0);
            self.sync_buffer(jb.buf.clone());
        }

        self.checkpoint_transactions.remove(0);

        true
    }

    fn cleanup_tail(&mut self) -> bool {
        let (first_tid, blocknr) = if !self.checkpoint_transactions.is_empty() {
            let tx_rc = &self.checkpoint_transactions[0];
            let tx = tx_rc.borrow();
            (tx.tid, tx.log_start)
        } else if let Some(tx_rc) = &self.committing_transaction {
            let tx = tx_rc.borrow();
            (tx.tid, tx.log_start)
        } else if let Some(tx_rc) = &self.running_transaction {
            let tx = tx_rc.borrow();
            (tx.tid, self.head)
        } else {
            (self.transaction_sequence, self.head)
        };

        assert!(blocknr != 0);

        log::debug!(
            "Cleanup tail: first_tid: {}, blocknr: {}, self.tail_sequence: {}",
            first_tid,
            blocknr,
            self.tail_sequence
        );

        if self.tail_sequence == first_tid {
            return true;
        }

        assert!(first_tid > self.tail_sequence);

        let mut freed = blocknr - self.tail;
        if blocknr < self.tail {
            freed += self.last - self.first;
        }

        self.free += freed;
        self.tail_sequence = first_tid;
        self.tail = blocknr;

        if !self.flags.contains(JournalFlag::ABORT) {
            self.update_superblock();
        }

        false
    }
}
