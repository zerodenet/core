//! A single connection reader retains reassembly across cancelled receives.
use super::*;
use bytes::Bytes;
use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{
    context::{HandshakeContext, InboundConnectionFacts},
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{ChannelControl, CloseReport},
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason,
    SendFailure, SendReceipt, Stage,
};
use zero_transport::quic::owned::OwnedConnection;
pub(super) struct Incoming {
    pub connection: OwnedConnection,
    pub first: Bytes,
    pub auth: zero_core::SessionAuth,
    pub facts: InboundConnectionFacts,
}
impl InboundHandshake for Incoming {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let facts = self.facts.request(
            &context,
            zero_core::ProtocolType::new("hysteria2"),
            None,
            Some(self.auth),
        )?;
        Ok(AcceptedRequest::Datagram(accept_datagram(Reply {
            connection: self.connection,
            first: self.first,
            sender: self.facts.original_source.unwrap(),
            facts,
        })))
    }
}
struct Reply {
    connection: OwnedConnection,
    first: Bytes,
    sender: std::net::SocketAddr,
    facts: zero_core::request::RequestFacts,
}
struct Shared {
    cancel: CancellationToken,
    users: AtomicUsize,
    changed: tokio::sync::Notify,
}
struct Usage(Arc<Shared>);
impl Drop for Usage {
    fn drop(&mut self) {
        self.0.users.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct Control(Arc<Shared>);
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.cancel.cancel();
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.users.load(Ordering::Acquire) == 0 {
                return Ok(Default::default());
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Timeout, Impact::Session)
                        .with_source(source)
                })?;
        }
    }
}
impl DatagramReply for Reply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &zero_core::request::RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> DatagramReplyResult<Tx, Rx, Control> {
        if !matches!(outcome, ReplyOutcome::LocalChannelReady) {
            return Ok(None);
        }
        let shared = Arc::new(Shared {
            cancel: CancellationToken::new(),
            users: AtomicUsize::new(2),
            changed: tokio::sync::Notify::new(),
        });
        let tx = Tx {
            connection: self.connection.try_clone()?,
            usage: Usage(shared.clone()),
        };
        let rx = Rx {
            state: Some(State {
                connection: self.connection,
                session: crate::udp::Hysteria2InboundUdpSession::new(),
                first: Some(self.first),
                wire_bytes: 0,
            }),
            pending: None,
            sender: self.sender,
            usage: Usage(shared.clone()),
        };
        Ok(Some(InboundDatagramSession {
            negotiation: Default::default(),
            io: DatagramSession {
                tx,
                rx,
                control: Control(shared),
                limits: DatagramLimits {
                    max_payload: 65535,
                    targets: zero_core::request::DatagramTargets::Multiple,
                    queued_bytes: 65535,
                },
            },
        }))
    }
}
struct Tx {
    connection: OwnedConnection,
    usage: Usage,
}
impl<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        response: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        let prepare = || {
            if this.usage.0.cancel.is_cancelled() {
                return Err(ChannelError::new(
                    Stage::Send,
                    Reason::Revoked,
                    Impact::Session,
                ));
            }
            let id = response
                .client_session_id
                .and_then(|id| u32::try_from(id).ok())
                .ok_or_else(|| {
                    ChannelError::new(Stage::Send, Reason::InvalidConfig, Impact::Session)
                })?;
            let max = this.connection.max_datagram_size().ok_or_else(|| {
                ChannelError::new(Stage::Send, Reason::Unsupported, Impact::Session)
            })?;
            crate::udp::build_udp_fragments(
                id,
                &response.source.address,
                response.source.port,
                response.payload,
                max,
            )
            .map_err(|source| {
                ChannelError::new(Stage::Send, Reason::Protocol, Impact::Datagram)
                    .with_source(source)
            })
        };
        let fragments = match prepare() {
            Ok(f) => f,
            Err(err) => {
                return Poll::Ready(Err(SendFailure::new(err, DeliveryState::NotSubmitted)))
            }
        };
        let mut protocol_bytes = 0;
        for fragment in fragments {
            let size = fragment.len();
            if let Err(err) = this.connection.send_datagram(fragment.into()) {
                return Poll::Ready(Err(SendFailure::new(
                    err,
                    if protocol_bytes == 0 {
                        DeliveryState::NotSubmitted
                    } else {
                        DeliveryState::Unknown
                    },
                )));
            }
            protocol_bytes += size;
        }
        Poll::Ready(Ok(InboundDatagramReceipt {
            payload: SendReceipt::accepted(response.payload.len()),
            protocol_bytes,
        }))
    }
}
struct State {
    connection: OwnedConnection,
    session: crate::udp::Hysteria2InboundUdpSession,
    first: Option<Bytes>,
    wire_bytes: u64,
}
struct Rx {
    state: Option<State>,
    pending: Option<
        BoxFuture<
            'static,
            (
                State,
                Result<Option<(zero_core::InboundUdpDispatch, u64)>, ChannelError>,
            ),
        >,
    >,
    sender: std::net::SocketAddr,
    usage: Usage,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        if this.usage.0.cancel.is_cancelled() {
            return Poll::Ready(Ok(None));
        }
        if this.pending.is_none() {
            let mut state = this.state.take().unwrap();
            let cancel = this.usage.0.cancel.clone();
            this.pending = Some(Box::pin(async move {
                let result=async {loop {let bytes=match state.first.take(){Some(bytes)=>bytes,None=>tokio::select!{biased;_=cancel.cancelled()=>return Ok(None),bytes=state.connection.read_datagram()=>bytes?}};
                // Accumulate only one bounded scalar. The decoder itself owns its
                // finite 64-entry reassembly table; no session history is retained.
                state.wire_bytes=state.wire_bytes.saturating_add(bytes.len() as u64);
                let decoded=state.session.decode_dispatch_parts(&bytes).map_err(|source|ChannelError::new(Stage::Receive,Reason::Protocol,Impact::Datagram).with_source(source))?;
                if let Some(parts)=decoded {let wire=std::mem::take(&mut state.wire_bytes);return Ok(Some((parts.into_tracked_inbound_dispatch().dispatch().clone(),wire)));}
            }}.await;
                (state, result)
            }));
        }
        match this.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((state, result)) => {
                this.state = Some(state);
                this.pending = None;
                Poll::Ready(result.map(|packet| {
                    packet.map(|(dispatch, protocol_bytes)| InboundDatagramEvent::Packet {
                        dispatch,
                        sender: this.sender,
                        protocol_bytes,
                    })
                }))
            }
        }
    }
}
