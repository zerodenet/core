use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_core::{component::AssociationId, packet::Datagram, request::DatagramTargets};

use super::{
    context::OpenContext,
    lifecycle::{erase_control, ChannelControl, DynChannelControl},
    BoxFuture, ChannelError, CloseReason, SendFailure, SendReceipt,
};

pub trait DatagramTx: Send {
    /// While pending, retain framing state and require the same frame on re-poll.
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>>;
}
pub trait DatagramRx: Send {
    /// Cancellation preserves unconsumed state; Some(empty) is valid, None is EOF.
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>>;
}

pub struct DatagramSession<Tx, Rx, Control> {
    pub tx: Tx,
    pub rx: Rx,
    pub control: Control,
    pub limits: DatagramLimits,
}

pub type DatagramOpenResult<Tx, Rx, Control> =
    Result<DatagramSession<Tx, Rx, Control>, ChannelError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatagramLimits {
    pub max_payload: usize,
    pub targets: DatagramTargets,
    /// Maximum accepted-but-not-submitted bytes owned by this logical session.
    pub queued_bytes: usize,
}

impl DatagramLimits {
    pub fn validate(&self, frame: &Datagram) -> Result<(), SendFailure> {
        if frame.payload.len() > self.max_payload {
            return Err(SendFailure::new(
                ChannelError::new(
                    super::Stage::Send,
                    super::Reason::Capacity,
                    super::Impact::Datagram,
                ),
                super::DeliveryState::NotSubmitted,
            ));
        }
        if matches!(&self.targets, DatagramTargets::Fixed(target) if *target != frame.address) {
            return Err(SendFailure::new(
                ChannelError::new(
                    super::Stage::Send,
                    super::Reason::Protocol,
                    super::Impact::Datagram,
                ),
                super::DeliveryState::NotSubmitted,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DatagramRequest {
    pub association: AssociationId,
    pub targets: DatagramTargets,
}

pub trait DatagramOutbound: Send + Sync + 'static {
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;
    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> impl Future<Output = DatagramOpenResult<Self::Tx, Self::Rx, Self::Control>> + Send;
}

pub type BoxDatagramSession =
    DatagramSession<Pin<Box<dyn DatagramTx>>, Pin<Box<dyn DatagramRx>>, Box<dyn DynChannelControl>>;

impl<T: DatagramTx + ?Sized> DatagramTx for Pin<Box<T>> {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        self.get_mut().as_mut().poll_send(cx, frame)
    }
}
impl<T: DatagramRx + ?Sized> DatagramRx for Pin<Box<T>> {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        self.get_mut().as_mut().poll_recv(cx)
    }
}

pub trait DynDatagramOutbound: Send + Sync {
    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>>;
}
struct Erased<T>(T);
impl<T: DatagramOutbound> DynDatagramOutbound for Erased<T> {
    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async move {
            context.session_identity()?;
            let session = self.0.open(request, context).await?;
            Ok(erase_session(session))
        })
    }
}
pub fn erase_session<
    Tx: DatagramTx + 'static,
    Rx: DatagramRx + 'static,
    Control: ChannelControl + 'static,
>(
    session: DatagramSession<Tx, Rx, Control>,
) -> BoxDatagramSession {
    DatagramSession {
        tx: Box::pin(session.tx),
        rx: Box::pin(session.rx),
        control: erase_control(session.control),
        limits: session.limits,
    }
}
pub fn erase_datagram(outbound: impl DatagramOutbound) -> Arc<dyn DynDatagramOutbound> {
    Arc::new(Erased(outbound))
}

/// A dropped pending send has unknown delivery. Isolate this logical session;
/// do not reset a private codec and append a different frame to its residual bytes.
pub async fn send<T: DatagramTx + ?Sized>(
    mut tx: Pin<&mut T>,
    control: &mut dyn DynChannelControl,
    frame: &Datagram,
    limits: &DatagramLimits,
) -> Result<SendReceipt, SendFailure> {
    limits.validate(frame)?;
    let mut guard = PendingSend {
        control,
        armed: false,
    };
    poll_fn(|cx| {
        guard.armed = true;
        let result = tx.as_mut().poll_send(cx, frame);
        if result.is_ready() {
            guard.armed = false;
        }
        match result {
            Poll::Ready(Ok(receipt)) => {
                Poll::Ready(check_receipt(receipt, frame.payload.len(), guard.control))
            }
            Poll::Ready(Err(failure)) => Poll::Ready(Err(check_failure(failure, guard.control))),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

pub(super) fn check_receipt(
    receipt: SendReceipt,
    expected: usize,
    control: &mut dyn DynChannelControl,
) -> Result<SendReceipt, SendFailure> {
    if receipt.accepted_bytes == expected
        && receipt.delivery == super::DeliveryState::LocallyAccepted
    {
        return Ok(receipt);
    }
    control.cancel(CloseReason::DeliveryUnknown);
    Err(SendFailure::new(
        ChannelError::new(
            super::Stage::Send,
            super::Reason::Internal,
            super::Impact::Session,
        ),
        super::DeliveryState::Unknown,
    ))
}

pub(super) fn check_failure(
    mut failure: SendFailure,
    control: &mut dyn DynChannelControl,
) -> SendFailure {
    failure.error.delivery = failure.delivery;
    if failure.delivery == super::DeliveryState::Unknown {
        control.cancel(CloseReason::DeliveryUnknown);
    }
    failure
}

pub async fn recv<R: DatagramRx + ?Sized>(
    mut rx: Pin<&mut R>,
) -> Result<Option<Datagram>, ChannelError> {
    poll_fn(|cx| rx.as_mut().poll_recv(cx)).await
}

pub(super) struct PendingSend<'a> {
    pub control: &'a mut dyn DynChannelControl,
    pub armed: bool,
}
impl Drop for PendingSend<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel(CloseReason::DeliveryUnknown);
        }
    }
}
