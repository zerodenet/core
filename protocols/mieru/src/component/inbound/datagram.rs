//! Actual Mieru logical stream framing; cancellation retains incomplete packets.
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    sync::Notify,
    time::Instant,
};
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    inbound::*,
    lifecycle::{ChannelControl, CloseReport},
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason,
    SendFailure, SendReceipt, Stage,
};
use zero_platform_tokio::TcpRelayStream;

struct Shared {
    cancel: CancellationToken,
    users: AtomicUsize,
    changed: Notify,
}
pub(crate) struct Use(Arc<Shared>);
impl Drop for Use {
    fn drop(&mut self) {
        self.0.users.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
pub(crate) struct Control(Arc<Shared>);
pub(crate) fn views() -> (Use, Use, Control) {
    let shared = Arc::new(Shared {
        cancel: CancellationToken::new(),
        users: AtomicUsize::new(2),
        changed: Notify::new(),
    });
    (Use(shared.clone()), Use(shared.clone()), Control(shared))
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.users.load(Ordering::Acquire) == 0 {
                return Ok(Default::default());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| error(Stage::Close, source))?;
        }
    }
}
pub(crate) fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Receive, source)
}
fn error(stage: Stage, source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(stage, Reason::Io, Impact::Session).with_source(source)
}
pub(crate) fn limits() -> DatagramLimits {
    DatagramLimits {
        max_payload: u16::MAX as usize,
        targets: zero_core::request::DatagramTargets::Multiple,
        queued_bytes: u16::MAX as usize,
    }
}
pub(crate) struct Reply {
    pub stream: TcpRelayStream,
    pub facts: RequestFacts,
    pub sender: std::net::SocketAddr,
}
impl DatagramReply for Reply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> DatagramReplyResult<Tx, Rx, Control> {
        if !matches!(outcome, ReplyOutcome::LocalChannelReady) {
            return Ok(None);
        }
        let (reader, writer) = tokio::io::split(self.stream);
        let (up, down, control) = views();
        Ok(Some(InboundDatagramSession {
            negotiation: Default::default(),
            io: DatagramSession {
                tx: Tx {
                    writer: Some(writer),
                    pending: None,
                    _usage: down,
                },
                rx: Rx {
                    reader: Some(Input {
                        reader,
                        codec: crate::udp::MieruInboundUdpSession::new(),
                    }),
                    pending: None,
                    sender: self.sender,
                    _usage: up,
                },
                control,
                limits: limits(),
            },
        }))
    }
}
type PendingWrite = BoxFuture<
    'static,
    (
        WriteHalf<TcpRelayStream>,
        Result<InboundDatagramReceipt, SendFailure>,
    ),
>;

pub(crate) struct Tx {
    writer: Option<WriteHalf<TcpRelayStream>>,
    pending: Option<PendingWrite>,
    _usage: Use,
}
impl<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        response: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        if this._usage.0.cancel.is_cancelled() {
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Revoked, Impact::Session),
                DeliveryState::NotSubmitted,
            )));
        }
        if this.pending.is_none() {
            let packet = crate::udp::packet::encode_udp_flow_packet(
                &response.source.address,
                response.source.port,
                response.payload,
            )
            .map_err(|source| SendFailure::new(failure(source), DeliveryState::NotSubmitted));
            let packet = match packet {
                Ok(packet) => packet,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let payload = response.payload.len();
            let mut writer = this.writer.take().unwrap();
            this.pending = Some(Box::pin(async move {
                let result = async {
                    writer.write_all(&packet).await.map_err(|source| {
                        SendFailure::new(error(Stage::Send, source), DeliveryState::Unknown)
                    })?;
                    writer.flush().await.map_err(|source| {
                        SendFailure::new(error(Stage::Send, source), DeliveryState::Unknown)
                    })?;
                    Ok(InboundDatagramReceipt {
                        payload: SendReceipt::accepted(payload),
                        protocol_bytes: packet.len(),
                    })
                }
                .await;
                (writer, result)
            }));
        }
        match this.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((writer, result)) => {
                this.pending = None;
                this.writer = Some(writer);
                Poll::Ready(result)
            }
        }
    }
}
struct Input {
    reader: ReadHalf<TcpRelayStream>,
    codec: crate::udp::MieruInboundUdpSession,
}
type PendingInput = BoxFuture<
    'static,
    (
        Input,
        Result<Option<zero_core::InboundUdpDispatch>, zero_core::Error>,
    ),
>;

pub(crate) struct Rx {
    reader: Option<Input>,
    pending: Option<PendingInput>,
    sender: std::net::SocketAddr,
    _usage: Use,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        if this._usage.0.cancel.is_cancelled() {
            return Poll::Ready(Ok(None));
        }
        if this.pending.is_none() {
            let mut reader = this.reader.take().unwrap();
            this.pending = Some(Box::pin(async move {
                let packet = reader
                    .codec
                    .read_inbound_dispatch_tokio(&mut reader.reader, &mut [])
                    .await;
                (reader, packet)
            }));
        }
        match this.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((reader, packet)) => {
                this.pending = None;
                this.reader = Some(reader);
                Poll::Ready(
                    packet
                        .map(|packet| {
                            packet.map(|dispatch| InboundDatagramEvent::Packet {
                                dispatch,
                                sender: this.sender,
                                protocol_bytes: 0,
                            })
                        })
                        .map_err(failure),
                )
            }
        }
    }
}
