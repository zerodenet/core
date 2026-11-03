use super::*;
use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use zero_core::{component::AssociationId, packet::Datagram};
use zero_platform_tokio::contract::{
    datagram::{DatagramRx, DatagramTx},
    lifecycle::ChannelControl,
    BoxFuture, CancellationToken, SendFailure, SendReceipt,
};
mod shared;
use shared::Shared;
struct PendingSend {
    target: zero_core::request::Destination,
    payload: Arc<[u8]>,
    future: BoxFuture<'static, Result<usize, SendFailure>>,
}
pub struct Tx {
    shared: Arc<Shared>,
    association: AssociationId,
    limits: DatagramLimits,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
    pending: Option<PendingSend>,
}
pub struct Rx {
    shared: Arc<Shared>,
    association: AssociationId,
    pending: Option<BoxFuture<'static, Result<(zero_core::Address, u16, Vec<u8>), ChannelError>>>,
}
pub struct Control {
    shared: Arc<Shared>,
    closed: bool,
}
pub(super) fn session(
    channel: OwnedUdpChannel,
    request: DatagramRequest,
    context: OpenContext,
    generation: Arc<Generation>,
    profile: usize,
) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
    let limits = DatagramLimits {
        max_payload: 65535,
        queued_bytes: 65535,
        targets: request.targets,
    };
    let shared = Shared::new(
        channel,
        &generation,
        profile,
        context.upstream.binding().clone(),
        context.cancel.child_token(),
    );
    Ok(DatagramSession {
        tx: Tx {
            shared: shared.clone(),
            association: request.association,
            limits: limits.clone(),
            observer: context.observer.clone(),
            session: context.session_identity()?,
            pending: None,
        },
        rx: Rx {
            shared: shared.clone(),
            association: request.association,
            pending: None,
        },
        control: Control {
            shared,
            closed: false,
        },
        limits,
    })
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        let reject =
            |reason| SendFailure::new(error(Stage::Send, reason), DeliveryState::NotSubmitted);
        if this.shared.stop.is_cancelled() {
            this.pending = None;
            return Poll::Ready(Err(reject(Reason::Cancelled)));
        }
        this.limits.validate(frame)?;
        if frame.association != this.association {
            return Poll::Ready(Err(reject(Reason::Protocol)));
        }
        if let Some(pending) = &this.pending {
            if pending.target != frame.address || pending.payload.as_ref() != frame.payload.as_ref()
            {
                this.shared.stop.cancel();
                return Poll::Ready(Err(reject(Reason::Protocol)));
            }
        } else {
            let shared = this.shared.clone();
            let target = frame.address.clone();
            let payload: Arc<[u8]> = frame.payload.as_ref().into();
            let queued = payload.clone();
            let selected = target.clone();
            this.pending = Some(PendingSend {
                target: selected,
                payload: queued,
                future: Box::pin(async move { shared.send(target, payload).await }),
            });
        }
        let result = std::task::ready!(this.pending.as_mut().unwrap().future.as_mut().poll(cx));
        this.pending = None;
        let sent = result?;
        // HY2's existing managed Flow accounting is payload-only. QUIC and
        // native framing remains counted once at the actual Outer provider.
        if let Some(observer) = &this.observer {
            observer.datagram_sent(this.session, sent as u64);
        }
        Poll::Ready(Ok(SendReceipt {
            accepted_bytes: sent,
            delivery: DeliveryState::LocallyAccepted,
        }))
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        let future = if let Some(pending) = &mut this.pending {
            pending
        } else {
            let shared = this.shared.clone();
            this.pending = Some(Box::pin(async move { shared.receive().await }));
            this.pending.as_mut().unwrap()
        };
        let result = std::task::ready!(future.as_mut().poll(cx));
        this.pending = None;
        let (address, port, payload) = result?;
        Poll::Ready(Ok(Some(Datagram {
            association: this.association,
            address: zero_core::request::Destination::new(address, port),
            payload: payload.into(),
        })))
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        self.pending = None;
        self.shared.returned();
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.pending = None;
        self.shared.returned();
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.stop.cancel();
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if self.closed {
            // The unique original report was already handed to this caller.
            return Ok(CloseReport::default());
        }
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shared.views.load(Ordering::Acquire) == 0 {
                let report = self.shared.release()?;
                self.closed = report.is_complete();
                return Ok(report);
            }
            tokio::time::timeout_at(end, changed)
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}
