//! Plain fixed-target datagrams; dispatch, policy and original cleanup are hosted.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    inbound::{
        accept_datagram,
        udp::{DatagramPeer, PeerControl, PeerRx, PeerTx},
        DatagramReply, InboundDatagramEvent, InboundDatagramReceipt, InboundDatagramResponse,
        InboundDatagramSession,
    },
    BoxFuture, DeliveryState, SendFailure, SendReceipt,
};
pub(super) struct Connection {
    peer: DatagramPeer,
    target: Option<Destination>,
}
impl Connection {
    pub fn new(peer: DatagramPeer, target: Option<Destination>) -> Self {
        Self { peer, target }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let target = self
            .target
            .ok_or_else(|| ChannelError::new(Stage::Handshake, Reason::Revoked, Impact::Session))?;
        let connection = self.peer.connection_facts().map_err(io)?;
        let sender = connection.effective_source.expect("actual UDP sender");
        let facts =
            connection.request(&context, ProtocolType::UNKNOWN, Some(target.clone()), None)?;
        Ok(AcceptedRequest::Datagram(accept_datagram(Reply {
            peer: self.peer,
            target,
            sender,
            facts,
        })))
    }
}
struct Reply {
    peer: DatagramPeer,
    target: Destination,
    sender: std::net::SocketAddr,
    facts: RequestFacts,
}
impl DatagramReply for Reply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = PeerControl;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(
        self,
        outcome: ReplyOutcome,
    ) -> Result<Option<InboundDatagramSession<Tx, Rx, PeerControl>>, ChannelError> {
        if !matches!(outcome, ReplyOutcome::LocalChannelReady) {
            return Ok(None);
        }
        let (tx, rx, control) = self.peer.split();
        Ok(Some(InboundDatagramSession {
            negotiation: Default::default(),
            io: DatagramSession {
                tx: Tx {
                    io: Some(tx),
                    pending: None,
                },
                rx: Rx {
                    io: rx,
                    target: self.target,
                    sender: self.sender,
                },
                control,
                limits: DatagramLimits {
                    max_payload: 65535,
                    // Runtime responses carry the actual resolved sender, which
                    // can differ from the configured domain. The raw reply has
                    // no address field; its fixed client peer is sealed in Tx.
                    targets: zero_core::request::DatagramTargets::Multiple,
                    queued_bytes: 65535,
                },
            },
        }))
    }
}
struct Tx {
    io: Option<PeerTx>,
    pending: Option<BoxFuture<'static, (PeerTx, Result<InboundDatagramReceipt, SendFailure>)>>,
}
impl<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.pending.is_none() {
            let tx = this.io.take().expect("one actual sender");
            let payload = frame.payload.to_vec();
            this.pending = Some(Box::pin(async move {
                let result = tx
                    .send(&payload)
                    .await
                    .map_err(|source| SendFailure::new(io(source), DeliveryState::NotSubmitted))
                    .and_then(|written| {
                        if written != payload.len() {
                            return Err(SendFailure::new(
                                ChannelError::new(Stage::Send, Reason::Internal, Impact::Session),
                                DeliveryState::Unknown,
                            ));
                        }
                        Ok(InboundDatagramReceipt {
                            payload: SendReceipt::accepted(written),
                            protocol_bytes: written,
                        })
                    });
                (tx, result)
            }));
        }
        match this.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((tx, result)) => {
                this.pending = None;
                this.io = Some(tx);
                Poll::Ready(result)
            }
        }
    }
}
struct Rx {
    io: PeerRx,
    target: Destination,
    sender: std::net::SocketAddr,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        this.io.poll_recv(cx).map(|result| {
            result.map(|packet| {
                packet.map(|payload| InboundDatagramEvent::Packet {
                    dispatch: zero_core::InboundUdpDispatch::new(
                        ProtocolType::UNKNOWN,
                        this.target.address.clone(),
                        this.target.port,
                        payload,
                        None,
                    ),
                    sender: this.sender,
                    protocol_bytes: 0,
                })
            })
        })
    }
}
fn io(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
