use crate::{err::JBDResult, tx::Tid, Journal};

pub struct RecoveryInfo {
    start_transcation: Tid,
    end_transaction: Tid,

    num_replays: usize,
    num_revokes: usize,
    num_revoke_hits: usize,
}

impl RecoveryInfo {
    fn new() -> Self {
        Self {
            start_transcation: 0,
            end_transaction: 0,
            num_replays: 0,
            num_revokes: 0,
            num_revoke_hits: 0,
        }
    }
}

pub enum PassType {
    Scan,
    Revoke,
    Replay,
}

impl Journal {
    pub fn recover(&mut self) -> JBDResult {
        let mut sb = self.superblock_mut();

        // If sb.start == 0, the journal has already been safely unmounted.
        if sb.start == 0 {
            log::debug!("No recovery required, last transaction: {}", u32::from_be(sb.sequence));
            self.transaction_sequence = (u32::from_be(sb.sequence) + 1) as u16;
            return Ok(());
        }

        let mut info = RecoveryInfo::new();
        self.do_one_pass(&mut info, PassType::Scan)?;
        self.do_one_pass(&mut info, PassType::Revoke)?;
        self.do_one_pass(&mut info, PassType::Replay)?;

        todo!();

        Ok(())
    }
}

impl Journal {
    fn do_one_pass(&mut self, info: &mut RecoveryInfo, pass_type: PassType) -> JBDResult {
        Ok(())
    }
}
