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
    controlled: BTreeMap<u64, Arc<io::Error>>,
    // The provider alone aggregates native receipts. Retain an unsuccessful
    // unique view until the same persistent original confirms closure.
    bound: BTreeMap<
        u64,
        (
            Box<zero_platform_tokio::contract::component::BoundDatagramSocket>,
            Arc<io::Error>,
        ),
    >,
}
impl NativeCloseLedger {
    pub(super) fn bound_occupied(&self) -> usize {
        self.bound.len()
    }
    pub(super) fn retire(&mut self, entry: DirectUdpSocket) {
        let DirectUdpSocket {
            id,
            socket,
            replies,
            ..
        } = entry;
        // Revoke every already-read response token before descriptor retirement.
        drop(replies);
        match socket {
            super::native::DirectDatagramIo::Bound(socket) => {
                if let Err(source) = socket.close() {
                    self.bound.insert(id, (socket, source));
                }
            }
            super::native::DirectDatagramIo::Controlled(socket) => {
                // Its persistent owner retains both the original and outcome.
                // Only collect_controlled consumes its actual receipt below.
                let _ = socket.close();
            }
            super::native::DirectDatagramIo::Raw(socket) => match socket.close() {
                Ok(()) => self.confirmed += 1,
                Err(source) => {
                    tracing::warn!(socket_id=id,error=%source,"Direct UDP native close remains unconfirmed");
                    self.failures.insert(id, Arc::new(source));
                }
            },
        }
    }
    pub(super) fn collect_controlled(
        &mut self,
        report: zero_platform_tokio::controlled_datagram::DatagramOwnerReport,
    ) {
        self.confirmed += report.confirmed_closed;
        // This owner's authoritative current ledger alone can remove a prior
        // unknown after a documented Windows known-open retry actually closes.
        self.controlled = report
            .unconfirmed
            .into_iter()
            .map(|failure| (failure.reservation.get(), failure.source))
            .collect();
    }
    fn retry_known_open(&mut self) {
        self.bound
            .retain(|_, (socket, source)| match socket.close() {
                Ok(_) => false,
                Err(actual) => {
                    *source = actual;
                    true
                }
            });
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
        for id in self.bound.keys() {
            report.unfinished.push(Unfinished::Io(format!(
                "unconfirmed host-bound original Direct UDP socket {id}"
            )));
        }
        for id in self.controlled.keys() {
            report.unfinished.push(Unfinished::Io(format!(
                "unconfirmed controlled original Direct UDP socket {id}"
            )));
        }
        report
    }
}
impl DirectUdpSockets {
    pub(super) fn retire_sockets(&mut self, mut remove: impl FnMut(&DirectUdpSocket) -> bool) {
        if self.defer_native_close {
            for entry in &mut self.sockets {
                if remove(entry) {
                    entry.replies.retire_all();
                    entry.retired = true;
                }
            }
            return;
        }
        self.close_rotated_sockets(remove);
    }
    /// The sole I/O owner has dropped the old native poll and revoked mappings.
    /// Real FD close here is normal rotation, never a containing TaskExit.
    pub(super) fn close_rotated_sockets(
        &mut self,
        mut remove: impl FnMut(&DirectUdpSocket) -> bool,
    ) {
        // extract_if retains the order/capacity of surviving shared sockets.
        // No replacement handle, descriptor clone, read task, or warm allocation.
        for entry in self.sockets.extract_if(.., |entry| remove(entry)) {
            self.close.retire(entry);
        }
        self.collect_native_outcomes();
    }
    fn collect_native_outcomes(&mut self) {
        if let Some(owner) = &mut self.native_owner {
            self.close.collect_controlled(owner.take_report());
        }
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
        // Only the actual owning lifecycle may call this terminal operation.
        // Peer mode reaches it after matching the real joined-task receipt;
        // logical retirement alone retains originals; explicit normal rotation
        // above separately consumes real native-close outcomes under I/O lock.
        for entry in self.sockets.drain(..) {
            self.close.retire(entry);
        }
        self.isolated_sessions.clear();
        self.reset_receiver_if_empty();
        self.close.retry_known_open();
        if let Some(owner) = &mut self.native_owner {
            self.close.collect_controlled(owner.close_all());
        }
    }
    /// Caller must persist this actual report before its next suspension or
    /// error return. Unknown original-descriptor failures remain in this ledger.
    pub(crate) fn take_native_close_report(&mut self) -> CloseReport {
        self.collect_native_outcomes();
        self.close.take_report()
    }
    pub(crate) fn native_close_failure(&self) -> Option<(u64, Arc<io::Error>)> {
        if let Some((id, (_, source))) = self.close.bound.first_key_value() {
            return Some((*id, source.clone()));
        }
        self.close
            .failures
            .first_key_value()
            .or_else(|| self.close.controlled.first_key_value())
            .map(|(id, source)| (*id, source.clone()))
    }
}
