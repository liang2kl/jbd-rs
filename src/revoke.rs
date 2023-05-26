use core::{borrow::BorrowMut, cell::RefCell};

extern crate alloc;
use alloc::{collections::BTreeMap, rc::Rc};

use crate::{
    err::{JBDError, JBDResult},
    sal::Buffer,
    tx::{JournalBuffer, Tid, Transaction},
    Handle, Journal,
};

pub(crate) struct RevokeRecord {
    sequence: Tid,
    blocknr: u32,
}

impl Handle {
    pub fn revoke(&mut self, buf: &Rc<dyn Buffer>) -> JBDResult {
        let transcation_rc = self.transaction.as_ref().unwrap().clone();
        let mut transaction = transcation_rc.as_ref().borrow_mut();
        let journal_rc = transaction.journal.upgrade().unwrap();
        let mut journal = journal_rc.as_ref().borrow_mut();

        if buf.revoked() {
            log::error!("Buffer {} is revoked again; data is inconsistent!", buf.block_id());
            return Err(JBDError::IOError);
        }

        buf.set_revoked();
        buf.set_revoke_valid();

        self.forget(buf)?;

        journal.insert_revoke_record(buf.block_id() as u32, transaction.tid);

        log::debug!("Revoked buffer {} in transaction {}", buf.block_id(), transaction.tid);

        Ok(())
    }

    pub(crate) fn cancel_revoke(&self, jb_rc: &Rc<RefCell<JournalBuffer>>, jb: &JournalBuffer) -> JBDResult {
        let buf = &jb.buf;
        let transaction_rc = jb.transaction.as_ref().unwrap().upgrade().unwrap();
        let transaction = transaction_rc.borrow();
        let journal_rc = transaction.journal.upgrade().unwrap();
        let mut journal = journal_rc.as_ref().borrow_mut();

        log::debug!("Canceling revoke for buffer {}", buf.block_id());

        let need_cancel = if buf.test_set_revoke_valid() {
            buf.test_clear_revoked()
        } else {
            buf.clear_revoked();
            true
        };

        if need_cancel {
            if journal
                .get_revoke_table_mut()
                .remove_entry(&(buf.block_id() as u32))
                .is_some()
            {
                log::debug!(
                    "Canceling revoke for buffer {} in transaction {}",
                    buf.block_id(),
                    transaction.tid
                );
            }
        }

        Ok(())
    }
}

impl Journal {
    fn insert_revoke_record(&mut self, blocknr: u32, sequence: Tid) {
        let record = RevokeRecord { sequence, blocknr };
        self.get_revoke_table_mut().insert(blocknr, record);
    }

    pub(crate) fn switch_revoke_table(&mut self) {
        if self.current_revoke_table == 0 {
            self.current_revoke_table = 1;
        } else {
            self.current_revoke_table = 0;
        }
    }

    pub(crate) fn write_revoke_records(&mut self, transaction: &Transaction) {
        let mut revoke_table = &mut self.revoke_tables[(self.current_revoke_table + 1) % 2];
        let mut descriptor_rc: Option<Rc<RefCell<JournalBuffer>>> = None;
    }

    pub(crate) fn clear_buffer_revoked_flags(&mut self) {
        for (blocknr, _) in self.get_revoke_table().iter() {
            if let Ok(buf) = self.get_buffer(*blocknr) {
                buf.clear_revoked();
            }
        }
    }

    fn get_revoke_table(&self) -> &BTreeMap<u32, RevokeRecord> {
        &self.revoke_tables[self.current_revoke_table]
    }

    fn get_revoke_table_mut(&mut self) -> &mut BTreeMap<u32, RevokeRecord> {
        &mut self.revoke_tables[self.current_revoke_table]
    }
}
