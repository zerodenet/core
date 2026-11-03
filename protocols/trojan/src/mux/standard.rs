//! Standard requests over the same actual Trojan MUX reader, writer and queues.
use super::*;
use crate::component::inbound::datagram;
use std::num::NonZeroUsize;
use zero_core::{request::Destination, MuxUdpResponder};
use zero_platform_tokio::contract::{
    context::{AcceptContext, HandshakeContext, InboundConnectionFacts},
    datagram::{DatagramRx, DatagramSession, DatagramTx},
    inbound::*,
    scope::TaskClass,
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, SendFailure, SendReceipt, Stage,
};
pub(super) fn io_error(source: io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
pub(super) fn write_error(source: io::Error) -> ChannelError {
    ChannelError::new(Stage::Send, Reason::Io, Impact::Session).with_source(source)
}
pub(crate) fn source(
    stream: zero_platform_tokio::TcpRelayStream,
    auth: SessionAuth,
    policy: MuxResponseBacklogPolicy,
    context: &HandshakeContext,
    facts: InboundConnectionFacts,
) -> Result<AcceptedRequest, ChannelError> {
    let caller = context
        .accepted
        .as_ref()
        .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
    let (reader, writer) = tokio::io::split(stream);
    let (writer, task) = TrojanInboundMuxWriter::prepare(writer, policy);
    caller.spawn(TaskClass::Auxiliary, move |_| task)?;
    let mut server = TrojanInboundMuxServer {
        session: TrojanInboundMuxSession::new(),
        streams: TrojanInboundMuxStreams::new(),
        writer,
        auth: Some(auth.clone()),
    };
    let mut reader = reader;
    let (opened, requests) = mpsc::channel(256);
    let cancel = context.cancel.child_token();
    let stopping = cancel.clone();
    caller.spawn(TaskClass::Auxiliary, move |_| async move {
        let _cancel_on_exit = stopping.clone().drop_guard();
        loop {
            let route = tokio::select! { biased;
                _ = stopping.cancelled() => return Ok(()),
                route = server.next_opened_route(&mut reader) => route.map_err(datagram::failure)?,
            };
            if let Some(route) = route {
                if let Err(rejected) = opened.try_send(Request {
                    route,
                    facts: facts.clone(),
                }) {
                    match rejected.into_inner().route.state {
                        TrojanInboundMuxOpenedRouteState::Tcp { relay, .. } => {
                            server.streams.streams.remove(&relay.session_id);
                            let _ = relay.writer.end(relay.session_id);
                        }
                        TrojanInboundMuxOpenedRouteState::Udp { mut relay } => {
                            server.streams.streams.remove(&relay.session_id);
                            let _ = relay.end_inbound_stream();
                        }
                    }
                }
            }
            // Reclaim only protocol queue metadata. Actual child originals and
            // join debt stay charged independently in their host custody slots.
            let writer = server.writer.clone();
            server.streams.streams.retain(|sid, sender| {
                if sender.is_closed() {
                    let _ = writer.end(*sid);
                    false
                } else {
                    true
                }
            });
        }
    })?;
    Ok(AcceptedRequest::Multiplex(Box::new(Source {
        requests,
        auth,
        cancel,
    })))
}
struct Source {
    requests: mpsc::Receiver<Request>,
    auth: SessionAuth,
    cancel: zero_platform_tokio::contract::CancellationToken,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl RequestMultiplexer for Source {
    fn cancellation(&self) -> Option<zero_platform_tokio::contract::CancellationToken> {
        Some(self.cancel.clone())
    }
    fn auth(&self) -> Option<&SessionAuth> {
        Some(&self.auth)
    }
    fn close(&mut self, _: &str) {
        self.cancel.cancel();
    }
    fn max_in_flight(&self) -> NonZeroUsize {
        NonZeroUsize::new(256).unwrap()
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            tokio::select! { biased;
                _ = self.cancel.cancelled() => Ok(None),
                _ = context.cancel.cancelled() => Err(ChannelError::new(Stage::Receive, Reason::Cancelled, Impact::Session)),
                request = zero_platform_tokio::contract::context::with_deadline(context.deadline, self.requests.recv()) => request.map(|request| request.map(PendingInbound::new)).map_err(|source| io_error(io::Error::other(source))),
            }
        })
    }
}
struct Request {
    route: TrojanInboundMuxOpenedRoute,
    facts: InboundConnectionFacts,
}
impl InboundHandshake for Request {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        match self.route.state {
            TrojanInboundMuxOpenedRouteState::Tcp { session, relay } => {
                let facts = self.facts.request(
                    &context,
                    session.protocol,
                    Some(Destination::new(session.target, session.port)),
                    session.auth,
                )?;
                Ok(AcceptedRequest::Stream(accept_stream(
                    crate::component::inbound::TcpReply::new(relay.into_standard_stream(), facts)?,
                )))
            }
            TrojanInboundMuxOpenedRouteState::Udp { relay } => {
                let facts = self.facts.request(
                    &context,
                    ProtocolType::new("trojan_udp"),
                    None,
                    relay.auth.clone(),
                )?;
                let sender = self.facts.effective_source.ok_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Session)
                })?;
                Ok(AcceptedRequest::Datagram(accept_datagram(UdpReply {
                    relay,
                    facts,
                    sender,
                })))
            }
        }
    }
}
struct UdpReply {
    relay: TrojanInboundMuxUdpRelay,
    facts: zero_core::request::RequestFacts,
    sender: std::net::SocketAddr,
}
impl DatagramReply for UdpReply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = datagram::Control;
    fn facts(&self) -> &zero_core::request::RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> DatagramReplyResult<Tx, Rx, Self::Control> {
        if !matches!(outcome, ReplyOutcome::LocalChannelReady) {
            return Ok(None);
        }
        let (up, down, control) = datagram::views();
        let TrojanInboundMuxUdpRelay {
            up_rx, responder, ..
        } = self.relay;
        Ok(Some(InboundDatagramSession {
            negotiation: Default::default(),
            io: DatagramSession {
                tx: Tx {
                    responder: responder.clone(),
                    _usage: down,
                },
                rx: Rx {
                    packets: up_rx,
                    responder,
                    sender: self.sender,
                    _usage: up,
                },
                control,
                limits: datagram::limits(),
            },
        }))
    }
}
struct Tx {
    responder: crate::udp::TrojanInboundMuxUdpResponder,
    _usage: datagram::Use,
}
impl Drop for Tx {
    fn drop(&mut self) {
        let _ = self.responder.end_inbound_stream();
    }
}
impl<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        response: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        if this._usage.cancelled() {
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Revoked, Impact::Session),
                DeliveryState::NotSubmitted,
            )));
        }
        Poll::Ready(
            this.responder
                .write_response_for_target(
                    &response.source.address,
                    response.source.port,
                    response.payload,
                )
                .map(|bytes| InboundDatagramReceipt {
                    payload: SendReceipt::accepted(response.payload.len()),
                    protocol_bytes: bytes,
                })
                .map_err(|source| {
                    SendFailure::new(datagram::failure(source), DeliveryState::NotSubmitted)
                }),
        )
    }
}
struct Rx {
    packets: mpsc::UnboundedReceiver<Vec<u8>>,
    responder: crate::udp::TrojanInboundMuxUdpResponder,
    sender: std::net::SocketAddr,
    _usage: datagram::Use,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        if this._usage.cancelled() {
            return Poll::Ready(Ok(None));
        }
        match this.packets.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(bytes)) if !bytes.is_empty() => Poll::Ready(
                this.responder
                    .decode_inbound_dispatch(&bytes)
                    .map(|dispatch| {
                        Some(InboundDatagramEvent::Packet {
                            dispatch,
                            sender: this.sender,
                            protocol_bytes: 0,
                        })
                    })
                    .map_err(datagram::failure),
            ),
            Poll::Ready(_) => Poll::Ready(Ok(None)),
        }
    }
}
