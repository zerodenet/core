use super::*;
use std::sync::atomic::Ordering;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};
impl DirectEndpointOwner {
    pub(in super::super) async fn shutdown_peer(
        &mut self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let peer = self.peer.as_mut().expect("peer mode owns host binding");
        peer.binding.request_stop();
        if peer.joined.is_none() && peer.binding.registration_rejection().is_none() {
            let joined = peer.binding.wait_joined(deadline).await?;
            if joined.task() != peer.task {
                return Err(error(
                    Stage::Close,
                    Reason::StalePlan,
                    DeliveryState::Unknown,
                ));
            }
            // Commit the genuine receipt before the next suspension. Parent
            // Scope records this task/source once; this resource reports I/O only.
            peer.joined = Some(joined);
        }
        let Ok(mut sockets) = timeout_at(deadline, self.shared.sockets.lock()).await else {
            let mut report = CloseReport::default();
            report.unfinished.push(Unfinished::Resource(
                "joined Direct peer still holds original socket state".into(),
            ));
            return Ok(report);
        };
        sockets.close_all_native();
        let mut native = sockets.take_native_close_report();
        let unfinished = std::mem::take(&mut native.unfinished);
        self.io_report.append(native);
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, entry) in std::mem::take(&mut state.sessions) {
            entry.logical.cancel.cancel();
            entry.logical.drain_receive_queue();
            entry.logical.closed.store(true, Ordering::Release);
            entry.logical.changed.notify_waiters();
        }
        if let Some((_id, source)) = sockets.native_close_failure() {
            return Err(error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source));
        }
        let mut report = std::mem::take(&mut self.io_report);
        report.unfinished = unfinished;
        Ok(report)
    }
}
