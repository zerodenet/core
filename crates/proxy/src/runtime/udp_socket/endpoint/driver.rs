use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{future::Future, pin::Pin, sync::atomic::Ordering};
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
        if !state.closing {
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
        sockets.refresh_if_stale();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (session, entry) in state
            .sessions
            .extract_if(.., |_, entry| entry.logical.cancel.is_cancelled())
        {
            sockets.retire_session(session.0);
            // Logical completion means this exact mapping and every already-read
            // source guard were revoked. It says nothing about a shared fd.
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
        if let Some((_id, source)) = sockets.native_close_failure() {
            state.failure.get_or_insert_with(|| {
                Arc::new(
                    error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source),
                )
            });
        }
    }
    fn deliver(&self, bytes: &[u8], source: DirectUdpResponseSource) {
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
        if logical
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes.len())
                    .filter(|next| self.queue.max_payload_bytes.is_none_or(|max| *next <= max))
            })
            .is_err()
        {
            self.drop_queued(zero_traits::PacketDropReason::QueueFull);
            return;
        }
        permit.send(Response {
            payload: bytes.to_vec(),
            source,
            owner: logical.clone(),
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
    let mut sockets = shared.sockets.lock().await;
    let mut watches: FuturesUnordered<CancelWatch> = FuturesUnordered::new();
    let mut bytes = vec![0; 65_535];
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        shared.maintain(&mut sockets, &mut watches);
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
            Some(command) = commands.recv() => {
                if command.logical.cancel.is_cancelled() {
                    let _ = command.receipt.send(Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::NotSubmitted))));
                    continue;
                }
                let sent = tokio::select! {
                    _ = shared.scope_cancel.cancelled() => Err(legacy(error(Stage::Send, Reason::Revoked, DeliveryState::Unknown))),
                    _ = command.logical.cancel.cancelled() => Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::Unknown))),
                    sent = sockets.send_new_packet(&command.target, &command.candidates, &command.payload, command.logical.session.0, &command.logical.policy) => sent.map_err(|source| legacy(error(Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(source))),
                };
                if sent.is_err() { command.logical.cancel.cancel(); }
                // Losing the receiver cannot turn an actual submission into a
                // retryable error. The pending sender guard revokes this flow.
                let _ = command.receipt.send(sent);
            }
        }
    }
}
