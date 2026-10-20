use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::Ordering,
};
struct IoGuard<'a> {
    sockets: tokio::sync::MutexGuard<'a, DirectUdpSockets>,
    shared: Arc<Shared>,
}
impl Deref for IoGuard<'_> {
    type Target = DirectUdpSockets;
    fn deref(&self) -> &Self::Target {
        &self.sockets
    }
}
impl DerefMut for IoGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sockets
    }
}
impl Drop for IoGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (session, entry) in std::mem::take(&mut state.sessions) {
            entry.logical.cancel.cancel();
            self.sockets.retire_reply_mappings(session.0);
            entry.logical.drain_receive_queue();
            entry.logical.closed.store(true, Ordering::Release);
            entry.logical.changed.notify_waiters();
        }
        // No native close here: this guard can be dropped before the host has
        // observed the containing registered task's real TaskExit.
    }
}
use zero_platform_tokio::contract::lifecycle::ComponentError;

type CancelWatch = Pin<Box<dyn Future<Output = (SessionId, AssociationId)> + Send>>;
struct DriverGuard(Arc<Shared>);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closing && !self.0.scope_cancel.is_cancelled() {
            state.failure.get_or_insert_with(|| {
                Arc::new(
                    error(Stage::Receive, Reason::Internal, DeliveryState::Unknown).with_source(
                        std::io::Error::other("shared Direct driver exited before revoke"),
                    ),
                )
            });
        }
        state.closing = true;
        for entry in state.sessions.values() {
            entry.logical.cancel.cancel();
        }
        drop(state);
        self.0.changed.notify_waiters();
    }
}
impl Shared {
    fn maintain(
        &self,
        sockets: &mut DirectUdpSockets,
        watches: &mut FuturesUnordered<CancelWatch>,
    ) {
        let refresh = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .refresh_requested;
        sockets.refresh_if_stale();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (session, entry) in state
            .sessions
            .extract_if(.., |_, entry| entry.logical.cancel.is_cancelled())
        {
            if self.bound_mode {
                sockets.retire_reply_mappings(session.0);
                // The sole I/O guard has canceled native polls and removed all
                // mappings from these originals. Real physical close releases
                // provider leases; it is not the peer task's terminal receipt.
                sockets.close_rotated_sockets(|entry| entry.replies.is_empty());
            } else if self.retain_native_until_peer_exit {
                sockets.retire_reply_mappings(session.0);
            } else {
                sockets.retire_session(session.0);
            }
            // Logical completion means this exact mapping and every already-read
            // source guard were revoked. It says nothing about a shared fd.
            entry.logical.drain_receive_queue();
            entry.logical.closed.store(true, Ordering::Release);
            entry.logical.changed.notify_waiters();
        }
        for entry in state.sessions.values_mut() {
            if !entry.watched {
                let logical = entry.logical.clone();
                watches.push(Box::pin(async move {
                    logical.cancel.cancelled().await;
                    (logical.session, logical.association)
                }));
                entry.watched = true;
            }
        }
        let refreshed = state.refresh_completed < refresh;
        state.refresh_completed = refresh;
        if let Some((_id, source)) = sockets.native_close_failure() {
            state.failure.get_or_insert_with(|| {
                Arc::new(
                    error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source),
                )
            });
        }
        drop(state);
        if refreshed {
            self.changed.notify_waiters();
        }
    }
    fn deliver(&self, bytes: &[u8], mut source: DirectUdpResponseSource) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = source
            .session_id
            .and_then(|id| state.sessions.get(&SessionId(id)))
        else {
            return;
        };
        let logical = &entry.logical;
        if logical.cancel.is_cancelled() || !source.is_current() {
            return;
        }
        // Serialize accepted enqueue with actual cancellation drain. There can
        // be no still-reserved sender posting behind a completed drain.
        let _receiver = logical
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if logical.cancel.is_cancelled() {
            return;
        }
        let permit = match logical.responses.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.drop_queued(zero_traits::PacketDropReason::QueueFull);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.drop_queued(zero_traits::PacketDropReason::QueueClosed);
                return;
            }
        };
        let mut used = logical.bytes.load(Ordering::Acquire);
        let reserved = loop {
            let Some(next) = used.checked_add(bytes.len()) else {
                break false;
            };
            if self
                .queue
                .max_payload_bytes
                .is_some_and(|limit| next > limit)
            {
                break false;
            }
            match logical.bytes.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break true,
                Err(observed) => used = observed,
            }
        };
        if !reserved {
            self.drop_queued(zero_traits::PacketDropReason::QueueFull);
            return;
        }
        let Some(storage) = self.reserve_copy(bytes.len()) else {
            logical.bytes.fetch_sub(bytes.len(), Ordering::AcqRel);
            self.drop_queued(zero_traits::PacketDropReason::QueueFull);
            return;
        };
        source.guard = source
            .guard
            .map(|guard| guard.with_logical_cancel(logical.cancel.clone()));
        permit.send(Response {
            payload: bytes.to_vec(),
            source,
            owner: logical.clone(),
            _storage: storage,
        });
        logical.changed.notify_waiters();
    }
    fn drop_queued(&self, reason: zero_traits::PacketDropReason) {
        if let Some(observer) = &self.observer {
            observer.dropped_reason(reason);
        }
    }
}
pub(super) async fn run(
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
) -> Result<(), ComponentError> {
    let _lifetime = DriverGuard(shared.clone());
    // Session handles never lock this state. Cancelled receive polling releases
    // its inner receive guard before the same driver executes a send command.
    let mut sockets = IoGuard {
        sockets: shared.sockets.lock().await,
        shared: shared.clone(),
    };
    let mut watches: FuturesUnordered<CancelWatch> = FuturesUnordered::new();
    let mut bytes = vec![0; 65_535];
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        shared.maintain(&mut sockets, &mut watches);
        if shared.bound_mode {
            let source = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure
                .clone();
            if let Some(source) = source {
                return Err(
                    error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source)
                );
            }
        }
        if shared.scope_cancel.is_cancelled() {
            return Ok(());
        }
        tokio::select! {
            _ = shared.scope_cancel.cancelled() => return Ok(()),
            _ = &mut changed => {},
            Some((_session, _association)) = watches.next(), if !watches.is_empty() => {},
            incoming = sockets.recv_from_addr(&mut bytes) => {
                let (size, source) = incoming.map_err(|source| error(Stage::Receive, Reason::Io, DeliveryState::Unknown).with_source(source))?;
                shared.deliver(&bytes[..size], source);
            }
            Some(mut command) = commands.recv() => {
                if command.logical.cancel.is_cancelled() {
                    let _ = command.receipt.send(Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::NotSubmitted))));
                    continue;
                }
                if let Some(isolation) = command.logical.isolation {
                    sockets.isolate_association(command.logical.session.0, isolation);
                }
                sockets.bound = command.logical.binding.clone();
                let sent = tokio::select! {
                    _ = shared.scope_cancel.cancelled() => Err(legacy(error(Stage::Send, Reason::Revoked, DeliveryState::Unknown))),
                    _ = command.logical.cancel.cancelled() => Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::Unknown))),
                    sent = sockets.send_owned_packet(&command.target, std::mem::take(&mut command.candidates), &command.payload, command.logical.session.0, &command.logical.policy) => sent.map_err(|source| if shared.bound_mode { source } else { legacy(error(Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(source)) }),
                };
                sockets.bound = None;
                let logical_rejection = shared.bound_mode && sent.as_ref().err().is_some_and(failure::logical_rejection);
                if sent.is_err() && shared.retain_native_until_peer_exit && !logical_rejection
                    && !shared.scope_cancel.is_cancelled() && !command.logical.cancel.is_cancelled()
                {
                    let source = Arc::new(sent.err().expect("checked actual failure"));
                    let failure = Arc::new(error(Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(source));
                    shared.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                        .failure = Some(failure.clone());
                    command.logical.cancel.cancel();
                    let _ = command.receipt.send(Err(legacy(error(Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(failure.clone()))));
                    return Err(error(Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(failure));
                }
                if sent.is_err() { command.logical.cancel.cancel(); }
                // Losing the receiver cannot turn an actual submission into a
                // retryable error. The pending sender guard revokes this flow.
                let _ = command.receipt.send(sent);
            }
        }
    }
}
