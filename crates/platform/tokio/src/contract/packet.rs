use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_core::{
    component::EndpointId,
    packet::{IpFamily, IpPacket},
};

use super::{
    context::OpenContext,
    datagram::PendingSend,
    lifecycle::{erase_control, ChannelControl, DynChannelControl},
    BoxFuture, ChannelError, SendFailure, SendReceipt,
};

pub trait PacketTx: Send {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>>;
}
pub trait PacketRx: Send {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>>;
}

pub struct PacketSession<Tx, Rx, Control> {
    pub tx: Tx,
    pub rx: Rx,
    pub control: Control,
    pub limits: PacketLimits,
}

pub type PacketOpenResult<Tx, Rx, Control> = Result<PacketSession<Tx, Rx, Control>, ChannelError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAdaptation {
    Preserved,
    /// Concrete protocol and shared stack must validate the chosen addresses.
    ValidatedTranslation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnGuarantee {
    Endpoint,
    Peer,
    Flow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketLimits {
    pub mtu: usize,
    pub families: Vec<IpFamily>,
    pub source_adaptation: SourceAdaptation,
    pub return_association: ReturnGuarantee,
}

impl PacketLimits {
    pub fn validate(&self, packet: &IpPacket) -> Result<(), SendFailure> {
        let reason = if packet.payload().len() > self.mtu {
            Some(super::Reason::Capacity)
        } else if !self.families.contains(&packet.family()) {
            Some(super::Reason::Protocol)
        } else {
            None
        };
        match reason {
            Some(reason) => Err(SendFailure::new(
                ChannelError::new(super::Stage::Send, reason, super::Impact::Packet),
                super::DeliveryState::NotSubmitted,
            )),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PacketRequest {
    pub endpoint: EndpointId,
    pub limits: PacketLimits,
}

pub trait PacketOutbound: Send + Sync + 'static {
    type Tx: PacketTx + 'static;
    type Rx: PacketRx + 'static;
    type Control: ChannelControl + 'static;
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> impl Future<Output = PacketOpenResult<Self::Tx, Self::Rx, Self::Control>> + Send;
}

pub type BoxPacketSession =
    PacketSession<Pin<Box<dyn PacketTx>>, Pin<Box<dyn PacketRx>>, Box<dyn DynChannelControl>>;

impl<T: PacketTx + ?Sized> PacketTx for Pin<Box<T>> {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        self.get_mut().as_mut().poll_send(cx, packet)
    }
}
impl<T: PacketRx + ?Sized> PacketRx for Pin<Box<T>> {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        self.get_mut().as_mut().poll_recv(cx)
    }
}
pub trait DynPacketOutbound: Send + Sync {
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>>;
}
struct Erased<T>(T);
impl<T: PacketOutbound> DynPacketOutbound for Erased<T> {
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async move {
            context.packet_facts()?;
            self.0.open(request, context).await.map(erase_session)
        })
    }
}
pub fn erase_session<
    Tx: PacketTx + 'static,
    Rx: PacketRx + 'static,
    Control: ChannelControl + 'static,
>(
    session: PacketSession<Tx, Rx, Control>,
) -> BoxPacketSession {
    PacketSession {
        tx: Box::pin(session.tx),
        rx: Box::pin(session.rx),
        control: erase_control(session.control),
        limits: session.limits,
    }
}
pub fn erase_packet(outbound: impl PacketOutbound) -> Arc<dyn DynPacketOutbound> {
    Arc::new(Erased(outbound))
}

pub async fn send<T: PacketTx + ?Sized>(
    mut tx: Pin<&mut T>,
    control: &mut dyn DynChannelControl,
    packet: &IpPacket,
    limits: &PacketLimits,
) -> Result<SendReceipt, SendFailure> {
    limits.validate(packet)?;
    let mut guard = PendingSend {
        control,
        armed: false,
    };
    poll_fn(|cx| {
        guard.armed = true;
        let result = tx.as_mut().poll_send(cx, packet);
        if result.is_ready() {
            guard.armed = false;
        }
        match result {
            Poll::Ready(Ok(receipt)) => Poll::Ready(super::datagram::check_receipt(
                receipt,
                packet.payload().len(),
                guard.control,
            )),
            Poll::Ready(Err(failure)) => {
                Poll::Ready(Err(super::datagram::check_failure(failure, guard.control)))
            }
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}
pub async fn recv<R: PacketRx + ?Sized>(
    mut rx: Pin<&mut R>,
) -> Result<Option<IpPacket>, ChannelError> {
    poll_fn(|cx| rx.as_mut().poll_recv(cx)).await
}
