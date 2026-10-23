use std::future::Future;
use zero_core::{packet::PacketFacts, request::RequestFacts};

use super::super::{
    datagram::{self, BoxDatagramSession, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::ChannelControl,
    packet::{self, BoxPacketSession, PacketRx, PacketSession, PacketTx},
    stream::{BoxStream, StreamIo},
};
use super::{BoxFuture, ChannelError, ReplyOutcome};

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

pub trait AcceptedDatagram: Send {
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxDatagramSession>, ChannelError>>;
}
pub trait DatagramReply: Send + 'static {
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self,
        outcome: ReplyOutcome,
    ) -> impl Future<Output = DatagramReplyResult<Self::Tx, Self::Rx, Self::Control>> + Send;
}
pub type DatagramReplyResult<Tx, Rx, Control> =
    Result<Option<DatagramSession<Tx, Rx, Control>>, ChannelError>;
struct Datagram<T>(T);
impl<T: DatagramReply> AcceptedDatagram for Datagram<T> {
    fn facts(&self) -> &RequestFacts {
        self.0.facts()
    }
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxDatagramSession>, ChannelError>> {
        Box::pin(async move {
            self.0
                .respond(outcome)
                .await
                .map(|session| session.map(datagram::erase_session))
        })
    }
}
pub fn accept_datagram(reply: impl DatagramReply) -> Box<dyn AcceptedDatagram> {
    Box::new(Datagram(reply))
}

pub trait AcceptedPacket: Send {
    fn facts(&self) -> &PacketFacts;
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxPacketSession>, ChannelError>>;
}
pub trait PacketReply: Send + 'static {
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
