//! Explicit close outcomes belong to the same native socket set, across reuse.
use super::{DirectUdpSocket, DirectUdpSockets};
use std::{collections::BTreeMap, io, sync::Arc};
use zero_platform_tokio::{
    contract::lifecycle::{CloseReport, Unfinished},
    TokioDatagramSocket,
};
#[derive(Default)]
pub(super) struct NativeCloseLedger {
    confirmed: u64,
    // An unknown Unix outcome contains no reconstructed fd. A retryable Windows
    // error alone can retain its original, documented still-open SOCKET.
    failures: BTreeMap<u64, Arc<io::Error>>,
}
impl NativeCloseLedger {
    pub(super) fn retire(&mut self, entry: DirectUdpSocket) {
        let DirectUdpSocket {
            id,
            socket,
            replies,
            ..
        } = entry;
        // Revoke every already-read response token before descriptor retirement.
        drop(replies);
        match socket.close() {
            Ok(()) => self.confirmed += 1,
            Err(source) => {
                tracing::warn!(socket_id=id,error=%source,"Direct UDP native close remains unconfirmed");
                self.failures.insert(id, Arc::new(source));
            }
        }
    }
    fn retry_known_open(&mut self) {
        self.failures.retain(|_id, source| {
            if !TokioDatagramSocket::close_is_retryable(source) {
                return true;
            }
            match TokioDatagramSocket::retry_close(source) {
                Ok(true) => {
                    self.confirmed += 1;
                    false
                }
                Ok(false) => true,
                Err(error) => {
                    *source = Arc::new(error);
                    true
                }
            }
        });
    }
    fn take_report(&mut self) -> CloseReport {
        let mut report = CloseReport::default();
        let count = std::mem::take(&mut self.confirmed);
        if count != 0 {
            report.closed_io.push(format!(
                "explicitly closed {count} original Direct UDP sockets"
            ));
        }
        for id in self.failures.keys() {
            report.unfinished.push(Unfinished::Io(format!(
                "unconfirmed original Direct UDP socket {id}"
            )));
        }
        report
    }
}
impl DirectUdpSockets {
    pub(super) fn retire_sockets(&mut self, mut remove: impl FnMut(&DirectUdpSocket) -> bool) {
        // extract_if retains the order/capacity of surviving shared sockets.
        // No replacement handle, descriptor clone, read task, or warm allocation.
        for entry in self.sockets.extract_if(.., |entry| remove(entry)) {
            self.close.retire(entry);
        }
    }
    pub(super) fn reserve_native_open(&mut self) -> Result<(), zero_engine::EngineError> {
        if let Some(budget) = &mut self.native_budget {
            if self.sockets.len().saturating_add(self.close.failures.len()) >= budget.sockets.get()
                || budget.started >= budget.attempts.get()
            {
                use zero_platform_tokio::contract::{
                    ChannelError, DeliveryState, Impact, Reason, Stage,
                };
                let mut failure =
                    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component);
                failure.delivery = DeliveryState::NotSubmitted;
                return Err(io::Error::other(failure).into());
            }
            budget.started += 1;
        }
        Ok(())
    }
    pub(super) fn ensure_native_admission(&self) -> Result<(), zero_engine::EngineError> {
        if let Some((_id, source)) = self.native_close_failure() {
            use zero_platform_tokio::contract::{
                ChannelError, DeliveryState, Impact, Reason, Stage,
            };
            let mut failure = ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                .with_source(source);
            failure.delivery = DeliveryState::NotSubmitted;
            // RetryClass::Never: opening another socket cannot repair the
            // unknown outcome of this owner's original native descriptor.
            return Err(io::Error::other(failure).into());
        }
        Ok(())
    }
    pub(crate) fn close_all_native(&mut self) {
        self.retire_sockets(|_| true);
        self.isolated_sessions.clear();
        self.reset_receiver_if_empty();
        self.close.retry_known_open();
    }
    /// Caller must persist this actual report before its next suspension or
    /// error return. Unknown original-descriptor failures remain in this ledger.
    pub(crate) fn take_native_close_report(&mut self) -> CloseReport {
        self.close.take_report()
    }
    pub(crate) fn native_close_failure(&self) -> Option<(u64, Arc<io::Error>)> {
        self.close
            .failures
            .first_key_value()
            .map(|(id, source)| (*id, source.clone()))
    }
}
