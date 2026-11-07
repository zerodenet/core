use std::future::Future;
use zero_core::{packet::PacketFacts, request::RequestFacts};

use super::super::{
    datagram::{DatagramRx, DatagramTx},
    lifecycle::ChannelControl,
    packet::{self, BoxPacketSession, PacketRx, PacketSession, PacketTx},
    stream::{BoxStream, StreamIo},
};
use super::{
    erase_datagram_session, BoxFuture, BoxInboundDatagramSession, ChannelError,
    InboundDatagramEvent, InboundDatagramReceipt, InboundDatagramResponse, InboundDatagramSession,
    ReplyOutcome,
};

pub trait AcceptedStream: Send {
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxStream>, ChannelError>>;
}

/// The response object owns its concrete stream and any pre-read bytes together.
pub trait StreamReply: Send + 'static {
    type Stream: StreamIo + 'static;
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self,
        outcome: ReplyOutcome,
    ) -> impl Future<Output = Result<Option<Self::Stream>, ChannelError>> + Send;
}
struct Stream<T>(T);
impl<T: StreamReply> AcceptedStream for Stream<T> {
    fn facts(&self) -> &RequestFacts {
        self.0.facts()
    }
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxStream>, ChannelError>> {
        Box::pin(async move {
            self.0
                .respond(outcome)
                .await
                .map(|stream| stream.map(|io| Box::pin(io) as BoxStream))
        })
    }
}
pub fn accept_stream(reply: impl StreamReply) -> Box<dyn AcceptedStream> {
    Box::new(Stream(reply))
}

/// Authenticated protocol identity and terminal signal, with no carrier owner.
#[derive(Clone, Debug)]
pub struct DatagramContinuity {
    pub key: zero_core::UdpContinuityKey,
    pub termination: zero_core::InboundMuxUdpTerminationProbe,
}
pub trait AcceptedDatagram: Send {
    fn continuity(&self) -> Option<DatagramContinuity> {
        None
    }

    fn facts(&self) -> &RequestFacts;
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxInboundDatagramSession>, ChannelError>>;
}
pub trait DatagramReply: Send + 'static {
    fn continuity(&self) -> Option<DatagramContinuity> {
        None
    }
    type Tx: for<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> + 'static;
    type Rx: DatagramRx<InboundDatagramEvent> + 'static;
    type Control: ChannelControl + 'static;
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self,
        outcome: ReplyOutcome,
    ) -> impl Future<Output = DatagramReplyResult<Self::Tx, Self::Rx, Self::Control>> + Send;
}
pub type DatagramReplyResult<Tx, Rx, Control> =
    Result<Option<InboundDatagramSession<Tx, Rx, Control>>, ChannelError>;
struct Datagram<T>(T);
impl<T: DatagramReply> AcceptedDatagram for Datagram<T> {
    fn continuity(&self) -> Option<DatagramContinuity> {
        self.0.continuity()
    }
    fn facts(&self) -> &RequestFacts {
        self.0.facts()
    }
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxInboundDatagramSession>, ChannelError>> {
        Box::pin(async move {
            self.0
                .respond(outcome)
                .await
                .map(|session| session.map(erase_datagram_session))
        })
    }
}
pub fn accept_datagram(reply: impl DatagramReply) -> Box<dyn AcceptedDatagram> {
    Box::new(Datagram(reply))
}

pub trait AcceptedPacket: Send {
    #[cfg(feature = "native-packet-origin")]
    fn packet_memory(&self) -> Option<super::super::packet::PacketSourceMemory> {
        None
    }

    fn link_mtu(&self) -> Option<u16> {
        None
    }
    fn respond_observed(
        self: Box<Self>,
        outcome: ReplyOutcome,
        _observer: std::sync::Arc<dyn super::super::packet::PacketObserver>,
    ) -> BoxFuture<'static, Result<Option<BoxPacketSession>, ChannelError>> {
        self.respond(outcome)
    }
    fn facts(&self) -> &PacketFacts;
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxPacketSession>, ChannelError>>;
}
pub trait PacketReply: Send + 'static {
    #[cfg(feature = "native-packet-origin")]
    fn packet_memory(&self) -> Option<super::super::packet::PacketSourceMemory> {
        None
    }

    fn link_mtu(&self) -> Option<u16> {
        None
    }
    fn respond_observed(
        self,
        outcome: ReplyOutcome,
        _observer: std::sync::Arc<dyn super::super::packet::PacketObserver>,
    ) -> impl Future<Output = PacketReplyResult<Self::Tx, Self::Rx, Self::Control>> + Send
    where
        Self: Sized,
    {
        self.respond(outcome)
    }
    type Tx: PacketTx + 'static;
    type Rx: PacketRx + 'static;
    type Control: ChannelControl + 'static;
    fn facts(&self) -> &PacketFacts;
    fn respond(
        self,
        outcome: ReplyOutcome,
    ) -> impl Future<Output = PacketReplyResult<Self::Tx, Self::Rx, Self::Control>> + Send;
}
pub type PacketReplyResult<Tx, Rx, Control> =
    Result<Option<PacketSession<Tx, Rx, Control>>, ChannelError>;
struct Packet<T>(T);
impl<T: PacketReply> AcceptedPacket for Packet<T> {
    #[cfg(feature = "native-packet-origin")]
    fn packet_memory(&self) -> Option<super::super::packet::PacketSourceMemory> {
        self.0.packet_memory()
    }

    fn link_mtu(&self) -> Option<u16> {
        self.0.link_mtu()
    }
    fn respond_observed(
        self: Box<Self>,
        outcome: ReplyOutcome,
        observer: std::sync::Arc<dyn super::super::packet::PacketObserver>,
    ) -> BoxFuture<'static, Result<Option<BoxPacketSession>, ChannelError>> {
        Box::pin(async move {
            self.0
                .respond_observed(outcome, observer)
                .await
                .map(|session| session.map(packet::erase_session))
        })
    }
    fn facts(&self) -> &PacketFacts {
        self.0.facts()
    }
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxPacketSession>, ChannelError>> {
        Box::pin(async move {
            self.0
                .respond(outcome)
                .await
                .map(|session| session.map(packet::erase_session))
        })
    }
}
pub fn accept_packet(reply: impl PacketReply) -> Box<dyn AcceptedPacket> {
    Box::new(Packet(reply))
}
