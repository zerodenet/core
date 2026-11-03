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
struct Shared {
    channel: Mutex<Option<Arc<OwnedUdpChannel>>>,
    views: AtomicUsize,
    changed: tokio::sync::Notify,
    stop: CancellationToken,
}
impl Shared {
    fn returned(&self) {
        self.views.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
}
pub struct Tx {
    shared: Arc<Shared>,
    association: AssociationId,
    limits: DatagramLimits,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
}
pub struct Rx {
    shared: Arc<Shared>,
    association: AssociationId,
    pending: Option<BoxFuture<'static, Result<(zero_core::Address, u16, Vec<u8>), ChannelError>>>,
}
pub struct Control {
    shared: Arc<Shared>,
}
pub(super) fn session(
    channel: OwnedUdpChannel,
    request: DatagramRequest,
    context: OpenContext,
) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
    let limits = DatagramLimits {
        max_payload: 65535,
        queued_bytes: 0,
        targets: request.targets,
    };
    let shared = Arc::new(Shared {
        channel: Mutex::new(Some(Arc::new(channel))),
        views: AtomicUsize::new(2),
        changed: Default::default(),
        stop: context.cancel.child_token(),
    });
    Ok(DatagramSession {
        tx: Tx {
            shared: shared.clone(),
            association: request.association,
            limits: limits.clone(),
            observer: context.observer.clone(),
            session: context.session_identity()?,
        },
        rx: Rx {
            shared: shared.clone(),
            association: request.association,
            pending: None,
        },
        control: Control { shared },
        limits,
    })
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        let reject =
            |reason| SendFailure::new(error(Stage::Send, reason), DeliveryState::NotSubmitted);
        if this.shared.stop.is_cancelled() {
            return Poll::Ready(Err(reject(Reason::Cancelled)));
        }
        this.limits.validate(frame)?;
        if frame.association != this.association {
            return Poll::Ready(Err(reject(Reason::Protocol)));
        }
        let channel = this
            .shared
            .channel
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| reject(Reason::Revoked))?;
        let encoded = channel
            .send(&frame.address.address, frame.address.port, &frame.payload)
            .map_err(|mut error| {
                error.delivery = DeliveryState::Unknown;
                SendFailure::new(error, DeliveryState::Unknown)
            })?;
        if let Some(observer) = &this.observer {
            observer.datagram_sent(this.session, encoded as u64);
        }
        Poll::Ready(Ok(SendReceipt {
            accepted_bytes: frame.payload.len(),
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
            let channel = this
                .shared
                .channel
                .lock()
                .unwrap()
                .as_ref()
                .cloned()
                .ok_or_else(|| error(Stage::Receive, Reason::Revoked))?;
            let stop = this.shared.stop.clone();
            this.pending = Some(Box::pin(async move {
                tokio::select! {biased;_=stop.cancelled()=>Err(error(Stage::Receive,Reason::Cancelled)),result=channel.receive()=>result}
            }));
            this.pending.as_mut().unwrap()
        };
        let (address, port, payload) = std::task::ready!(future.as_mut().poll(cx))?;
        this.pending = None;
        Poll::Ready(Ok(Some(Datagram {
            association: this.association,
            address: zero_core::request::Destination::new(address, port),
            payload: payload.into(),
        })))
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
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
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shared.views.load(Ordering::Acquire) == 0 {
                drop(self.shared.channel.lock().unwrap().take());
                return Ok(CloseReport::default());
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
