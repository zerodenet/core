//! One shared protocol codec state; peer views retain the original native grant.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::request::{DatagramTargets, RequestFacts};
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    inbound::udp::{DatagramPeer, PeerControl, PeerRx, PeerTx},
    BoxFuture, DeliveryState, SendFailure, SendReceipt,
};
type Responder = Arc<Mutex<crate::udp::ShadowsocksInboundUdpResponder>>;
pub(super) struct Connection {
    peer: DatagramPeer,
    responder: Responder,
    #[cfg(feature = "blake3")]
    pin_error: Option<zero_core::Error>,
}
impl Connection {
    pub fn new(peer: DatagramPeer, responder: Responder) -> Self {
        #[cfg(feature = "blake3")]
        let pin_error = peer
            .authenticated_association()
            .map(|association| {
                responder
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pin_original_peer(association, peer.retirement())
            })
            .transpose()
            .err();
        Self {
            peer,
            responder,
            #[cfg(feature = "blake3")]
            pin_error,
        }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        #[cfg(feature = "blake3")]
        if let Some(error) = self.pin_error {
            return Err(protocol(error));
        }
        let connection = self.peer.connection_facts().map_err(io)?;
        let facts = connection.request(&context, ProtocolType::new("shadowsocks"), None, None)?;
        Ok(AcceptedRequest::Datagram(accept_datagram(Reply {
            peer: self.peer,
            responder: self.responder,
            facts,
        })))
    }
}
struct Reply {
    peer: DatagramPeer,
    responder: Responder,
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
                    responder: self.responder.clone(),
                    pending: None,
                },
                rx: Rx { io: rx },
                control,
                limits: DatagramLimits {
                    max_payload: 65535,
                    targets: DatagramTargets::Multiple,
                    queued_bytes: 65535,
                },
            },
        }))
    }
}
struct Tx {
    io: Option<PeerTx>,
    responder: Responder,
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
            let prepared = frame
                .client_session_id
                .ok_or_else(|| invalid("missing original UDP association"))
                .and_then(|id| {
                    this.responder
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .encode_association_response(
                            id,
                            &frame.source.address,
                            frame.source.port,
                            frame.payload,
                        )
                        .map_err(protocol)
                });
            let (client, datagram) = match prepared {
                Ok(packet) => packet,
                Err(error) => {
                    return Poll::Ready(Err(SendFailure::new(error, DeliveryState::NotSubmitted)));
                }
            };
            let tx = this.io.take().expect("one actual native sender");
            let payload_len = frame.payload.len();
            this.pending = Some(Box::pin(async move {
                let result = tx
                    .send_to(&datagram, client)
                    .await
                    .map_err(|source| SendFailure::new(io(source), DeliveryState::NotSubmitted))
                    .and_then(|written| {
                        if written != datagram.len() {
                            return Err(SendFailure::new(
                                ChannelError::new(Stage::Send, Reason::Internal, Impact::Session),
                                DeliveryState::Unknown,
                            ));
                        }
                        Ok(InboundDatagramReceipt {
                            payload: SendReceipt::accepted(payload_len),
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
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        self.get_mut().io.poll_event(cx)
    }
}

fn protocol(source: zero_core::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Protocol, Impact::Datagram).with_source(source)
}
fn invalid(message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Send, Reason::Protocol, Impact::Datagram)
        .with_source(std::io::Error::other(message))
}
fn io(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
