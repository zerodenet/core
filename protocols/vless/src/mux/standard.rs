//! Standard logical requests over the original VLESS Mux.Cool driver.
use super::*;
use crate::component::inbound::datagram;
use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};
use zero_platform_tokio::{
    contract::{
        context::{AcceptContext, HandshakeContext, InboundConnectionFacts},
        inbound::*,
        scope::TaskClass,
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream,
};
mod driver;
mod udp;
#[expect(
    clippy::too_many_arguments,
    reason = "existing MUX admission passes actual wire, credentials, caller and task scope without an alternate runtime interface"
)]
pub(crate) async fn source<S>(
    mut stream: S,
    uuid: [u8; 16],
    flow: Option<&'static str>,
    testseed: [u32; 4],
    auth: SessionAuth,
    backlog: MuxResponseBacklogPolicy,
    context: &HandshakeContext,
    facts: InboundConnectionFacts,
) -> Result<AcceptedRequest, ChannelError>
where
    S: zero_platform_tokio::ClientStream + 'static,
{
    let caller = context
        .accepted
        .as_ref()
        .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
    zero_traits::AsyncSocket::write_all(&mut stream, &[crate::shared::VLESS_VERSION, 0])
        .await
        .map_err(datagram::failure)?;
    let vision = crate::flow::is_vision_flow(flow);
    let stream = if vision {
        crate::inbound::vision::VlessInboundTcpStream::vision_after_response(stream, uuid, testseed)
    } else {
        crate::inbound::vision::VlessInboundTcpStream::plain(stream)
    };
    let mut server =
        VlessInboundMuxServer::from_master_uuid_with_auth(uuid, Some(auth.clone()), backlog);
    if vision {
        server.require_udp_only();
    }
    let (opened, requests) = mpsc::channel(256);
    let cancel = context.cancel.child_token();
    let stopping = cancel.clone();
    caller.spawn(TaskClass::Auxiliary, move |_| {
        driver::run(stream, server, opened, stopping, facts, uuid)
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
    cancel: CancellationToken,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl RequestMultiplexer for Source {
    fn auth(&self) -> Option<&SessionAuth> {
        Some(&self.auth)
    }
    fn close(&mut self, _: &str) {
        self.cancel.cancel();
    }
    fn cancellation(&self) -> Option<CancellationToken> {
        Some(self.cancel.clone())
    }
    fn max_in_flight(&self) -> NonZeroUsize {
        NonZeroUsize::new(256).unwrap()
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            tokio::select! {biased;
                _=self.cancel.cancelled()=>Ok(None),
                _=context.cancel.cancelled()=>Err(ChannelError::new(Stage::Receive,Reason::Cancelled,Impact::Session)),
                next=zero_platform_tokio::contract::context::with_deadline(context.deadline,self.requests.recv())=>next.map(|v|v.map(PendingInbound::new)).map_err(datagram::failure),
            }
        })
    }
}
struct Request {
    uuid: [u8; 16],
    route: VlessInboundMuxOpenedRoute,
    facts: InboundConnectionFacts,
}
impl InboundHandshake for Request {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        match self.route.state {
            VlessInboundMuxOpenedRouteState::Tcp { session, relay } => {
                let facts = self.facts.request(
                    &context,
                    session.protocol,
                    Some(zero_core::request::Destination::new(
                        session.target,
                        session.port,
                    )),
                    session.auth,
                )?;
                Ok(AcceptedRequest::Stream(accept_stream(TcpReply {
                    facts,
                    stream: relay.into_standard_stream(),
                })))
            }
            VlessInboundMuxOpenedRouteState::Udp { relay } => {
                let facts = self.facts.request(
                    &context,
                    ProtocolType::new("vless_mux"),
                    None,
                    relay.auth.clone(),
                )?;
                let sender = self.facts.effective_source.ok_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Session)
                })?;
                Ok(AcceptedRequest::Datagram(accept_datagram(udp::Reply {
                    uuid: self.uuid,
                    relay,
                    facts,
                    sender,
                })))
            }
        }
    }
}
struct TcpReply {
    facts: zero_core::request::RequestFacts,
    stream: TcpRelayStream,
}
impl StreamReply for TcpReply {
    type Stream = BoxStream;
    fn facts(&self) -> &zero_core::request::RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        if outcome == ReplyOutcome::LocalChannelReady {
            Ok(Some(Box::pin(self.stream)))
        } else {
            Ok(None)
        }
    }
}
impl VlessInboundMuxTcpRelay {
    fn into_standard_stream(self) -> TcpRelayStream {
        let sid = self.session_id;
        let output = self.writer.clone();
        let sender = output.down_tx.clone();
        let mut waiter = None;
        let maximum = output.backlog.payload_limit(MUX_MAX_PAYLOAD, 0);
        TcpRelayStream::new(zero_transport::pooled_driver::ExclusiveIo::new(
            zero_transport::stream::QueuedChannelStream::new(
                self.up_rx,
                sender,
                move |cx, bytes: Option<&[u8]>| {
                    let credit = std::task::ready!(output.backlog.poll_charge(
                        cx,
                        bytes.map_or(0, <[u8]>::len),
                        &mut waiter
                    ))
                    .ok_or_else(|| {
                        std::io::Error::other("VLESS MUX frame exceeds its original byte budget")
                    })?;
                    std::task::Poll::Ready(Ok(credit.map(|()| match bytes {
                        Some(bytes) => VlessInboundMuxDownlink::data(sid, bytes.to_vec()),
                        None => VlessInboundMuxDownlink::end(sid),
                    })))
                },
                move || {
                    self.writer.request_end(sid);
                },
                maximum,
                self.deferred_upload.unwrap_or_default(),
            ),
        ))
    }
}

#[cfg(test)]
#[path = "standard/tests/half_close.rs"]
mod half_close;

#[cfg(test)]
#[path = "standard/tests/cancelled.rs"]
mod cancelled;
