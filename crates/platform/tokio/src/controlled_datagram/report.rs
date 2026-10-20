//! Nonblocking observation of the same original native states. No probing a
//! lock and then reacquiring it through a blocking report path.
use super::*;

impl ControlledDatagramOwner {
    pub(super) fn try_take_report(&mut self) -> Option<DatagramOwnerReport> {
        let mut claims = Vec::with_capacity(self.entries.len());
        let mut gates = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            claims.push(NativeCloseClaim::acquire(&entry.close_running)?);
            let gate = match entry.native.try_lock() {
                Ok(gate) => gate,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return None,
            };
            gates.push((entry, gate));
        }
        let mut report = DatagramOwnerReport::default();
        let mut completed = Vec::with_capacity(gates.len());
        for (entry, gate) in &gates {
            match &**gate {
                NativeHandleState::Closed => {
                    completed.push(entry.id);
                    report.confirmed_closed += 1;
                }
                NativeHandleState::NoNative => {
                    completed.push(entry.id);
                    report.known_no_native += 1;
                }
                NativeHandleState::Unknown(source) => {
                    let acquisition = match entry.acquisition_failure.try_lock() {
                        Ok(source) => source,
                        Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                        Err(std::sync::TryLockError::WouldBlock) => return None,
                    };
                    report.unconfirmed.push(UnconfirmedDatagram {
                        reservation: entry.id,
                        source: source.clone(),
                        acquisition_source: acquisition.clone(),
                    });
                }
                _ => report.retained_live += 1,
            }
        }
        drop(gates);
        drop(claims);
        // Checked reservation identities bound the total possible receipts.
        self.confirmed_closed = self
            .confirmed_closed
            .checked_add(report.confirmed_closed)
            .expect("confirmed original count cannot exceed allocated identities");
        self.known_no_native = self
            .known_no_native
            .checked_add(report.known_no_native)
            .expect("uncreated original count cannot exceed allocated identities");
        let mut released = Vec::with_capacity(completed.len());
        for id in completed {
            if let Some(entry) = self.entries.remove(&id) {
                released.push(entry);
            }
        }
        // Counters already persist if an original's last external waker Drop
        // unwinds. No native gate/close lease is held across that destructor.
        drop(released);
        report.confirmed_closed = std::mem::take(&mut self.confirmed_closed);
        report.known_no_native = std::mem::take(&mut self.known_no_native);
        Some(report)
    }
}
