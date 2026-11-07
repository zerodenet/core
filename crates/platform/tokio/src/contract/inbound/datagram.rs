//! Inbound packet facts differ from outbound session frames. The accepted
//! channel is already the association; its packets need no invented identity.
use super::super::{
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{erase_control, ChannelControl},
    SendReceipt,
};
use std::pin::Pin;
use zero_core::{request::Destination, InboundUdpDispatch};

/// Protocol decoding preserves all route/sniff/transparent facts. The sender
/// is the actual packet source, distinct from the TCP control connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundDatagramEvent {
    Packet {
        dispatch: InboundUdpDispatch,
        sender: std::net::SocketAddr,
        protocol_bytes: u64,
    },
    /// Authentication belongs to this decoded packet, never the last packet
    /// observed on a shared listener. Runtime still owns policy and accounting.
    AuthenticatedPacket {
        dispatch: InboundUdpDispatch,
        sender: std::net::SocketAddr,
        protocol_bytes: u64,
        authentication: zero_core::SessionAuth,
    },
    /// An explicitly decoded local resolver operation, with no proxy flow.
    Resolve { domain: String },
    PeerResponse {
        sender: std::net::SocketAddr,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundDatagramResponse<'a> {
    pub source: Destination,
    /// Original protocol association from the actual admitted flow. This is
    /// not a host SessionId and must not be inferred from a response target.
    pub client_session_id: Option<u64>,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundDatagramReceipt {
    /// Full logical payload acceptance, including a zero-byte payload.
    pub payload: SendReceipt,
    /// Actual locally submitted protocol bytes; excludes carrier traffic.
    pub protocol_bytes: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InboundDatagramTraffic {
    pub read_bytes: u64,
    pub written_bytes: u64,
}

pub struct InboundDatagramSession<Tx, Rx, Control> {
    pub io: DatagramSession<Tx, Rx, Control>,
    /// Negotiation is charged once to the first successful business flow.
    /// An idle association must not create a dummy flow or statistics record.
    pub negotiation: InboundDatagramTraffic,
}

pub type DynInboundDatagramTx =
    dyn for<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt>;
pub type BoxInboundDatagramSession = InboundDatagramSession<
    Pin<Box<DynInboundDatagramTx>>,
    Pin<Box<dyn DatagramRx<InboundDatagramEvent>>>,
    crate::contract::lifecycle::BoxChannelControl,
>;

pub fn erase_datagram_session<
    Tx: for<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> + 'static,
    Rx: DatagramRx<InboundDatagramEvent> + 'static,
    Control: ChannelControl + 'static,
>(
    session: InboundDatagramSession<Tx, Rx, Control>,
) -> BoxInboundDatagramSession {
    InboundDatagramSession {
        io: DatagramSession {
            tx: Box::pin(session.io.tx),
            rx: Box::pin(session.io.rx),
            control: erase_control(session.io.control),
            limits: session.io.limits,
        },
        negotiation: session.negotiation,
    }
}

impl InboundDatagramResponse<'_> {
    pub fn validate(&self, limits: &DatagramLimits) -> bool {
        self.payload.len() <= limits.max_payload
            && match &limits.targets {
                zero_core::request::DatagramTargets::Multiple => true,
                zero_core::request::DatagramTargets::Fixed(source) => source == &self.source,
            }
    }
}
