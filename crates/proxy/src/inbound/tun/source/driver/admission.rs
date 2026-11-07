//! The same driver parks one native original while transmit admission is full.
//! No body completion/Drop is used as native or Scope closure evidence.
use super::*;
use zero_platform_tokio::contract::BoxFuture;
use zero_tun::packet_memory::PacketMemoryView;
pub(super) fn poll(
    shared: &Shared,
    memory: &PacketMemoryView,
    waiting: &mut Option<BoxFuture<'static, ()>>,
    cx: &mut std::task::Context<'_>,
) -> Poll<Result<(), ChannelError>> {
    if waiting.is_none() {
        *waiting = Some(changed(memory.clone()));
    }
    let mut signalled = waiting
        .as_mut()
        .is_some_and(|wait| wait.as_mut().poll(cx).is_ready());
    if signalled {
        *waiting = None;
    }
    let _notifications = memory.defer_notifications();
    let result = {
        let mut incoming = shared.incoming.lock().unwrap();
        incoming
            .as_mut()
            .ok_or_else(|| error(Stage::Receive, Reason::Internal))?
            .prepare_transmit_buffers(0, 0)
    };
    match result {
        Ok(()) => {
            *waiting = None;
            Poll::Ready(Ok(()))
        }
        Err(zero_traits::PacketDeriveError::Busy) => {
            if waiting.is_none() {
                *waiting = Some(changed(memory.clone()));
                if waiting
                    .as_mut()
                    .is_some_and(|wait| wait.as_mut().poll(cx).is_ready())
                {
                    *waiting = None;
                    signalled = true;
                } else {
                    signalled = false;
                }
            }
            // A ready notification may race another producer consuming that
            // capacity. Re-poll after unlocking; never retain a completed wait.
            if signalled {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
        Err(source) => {
            use zero_traits::PacketDeriveError;
            let reason = match source {
                PacketDeriveError::Revoked => Reason::Revoked,
                PacketDeriveError::Unreserved => Reason::Unsupported,
                PacketDeriveError::LayoutMismatch | PacketDeriveError::AlreadyDerived => {
                    Reason::Protocol
                }
                _ => Reason::Capacity,
            };
            Poll::Ready(Err(error(Stage::Receive, reason).with_source(source)))
        }
    }
}
fn changed(memory: PacketMemoryView) -> BoxFuture<'static, ()> {
    Box::pin(async move { memory.changed().await })
}
