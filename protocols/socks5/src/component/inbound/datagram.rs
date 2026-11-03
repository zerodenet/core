use super::connection::{io_failure, wire_failure};
use crate::udp::Socks5InboundUdpAssociationSession;
use alloc::{boxed::Box, vec};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use zero_core::request::{DatagramTargets, RequestFacts};
use zero_platform_tokio::{
    contract::{
        component::ResourceAccess,
        datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
        inbound::{
            DatagramReply, DatagramReplyResult, InboundDatagramEvent, InboundDatagramReceipt,
            InboundDatagramResponse, InboundDatagramSession, InboundDatagramTraffic, ReplyOutcome,
        },
        lifecycle::{ChannelControl, CloseReport, Unfinished},
        BoxFuture, ChannelError, CloseReason, DeliveryState, Impact, Reason, SendFailure,
        SendReceipt, Stage,
    },
    controlled_datagram::listener::{ControlledDatagramListenerRx, ControlledDatagramListenerTx},
    AcceptedCaller, TcpRelayStream,
};
use zero_transport::MeteredStream;

pub(super) struct Reply {
    pub io: MeteredStream<TcpRelayStream>,
    pub facts: RequestFacts,
    pub caller: AcceptedCaller,
    pub resources: Arc<dyn ResourceAccess>,
    pub session: Socks5InboundUdpAssociationSession,
}
impl DatagramReply for Reply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> DatagramReplyResult<Tx, Rx, Control> {
        use crate::Socks5Reply;
        let mut socket = crate::component::socket::Socket::new(self.io);
        if outcome != ReplyOutcome::LocalChannelReady {
            let reply = if outcome == ReplyOutcome::Blocked {
                Socks5Reply::ConnectionNotAllowed
            } else {
                Socks5Reply::HostUnreachable
            };
            crate::Socks5Inbound
                .send_response(&mut socket, reply)
                .await
                .map_err(|wire| wire_failure(wire, socket.take_source()))?;
            return Ok(None);
        }
        let grant = self.resources.open_accepted_datagram(&self.caller)?;
        let bound = grant
            .rx
            .local_addr()
            .map_err(|source| io_failure(Stage::Prepare, source))?;
        let address = match bound.ip() {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        };
        crate::Socks5Inbound
            .send_success_response_with_bound(&mut socket, &address, bound.port())
            .await
            .map_err(|wire| wire_failure(wire, socket.take_source()))?;
        let mut io = socket.into_inner();
        let traffic = io.drain_traffic();
        let session = Arc::new(Mutex::new(self.session));
        let cancel = zero_platform_tokio::contract::CancellationToken::new();
        let uses = Arc::new(Uses {
            tx: std::sync::atomic::AtomicBool::new(true),
            rx: std::sync::atomic::AtomicBool::new(true),
            changed: tokio::sync::Notify::new(),
        });
        Ok(Some(InboundDatagramSession {
            negotiation: InboundDatagramTraffic {
                read_bytes: traffic.read_bytes,
                written_bytes: traffic.written_bytes,
            },
            io: DatagramSession {
                tx: Tx {
                    socket: grant.tx,
                    cancel: cancel.clone(),
                    session: session.clone(),
                    pending: None,
                    _usage: Use {
                        state: uses.clone(),
                        tx: true,
                    },
                },
                rx: Rx {
                    io: Some(Input {
                        tcp: io,
                        udp: grant.rx,
                        session,
                        cancel: cancel.clone(),
                        buffer: vec![0; 65536],
                    }),
                    pending: None,
                    _usage: Use {
                        state: uses.clone(),
                        tx: false,
                    },
                },
                control: Control { cancel, uses },
                limits: DatagramLimits {
                    max_payload: 65535,
                    targets: DatagramTargets::Multiple,
                    queued_bytes: 0,
                },
            },
        }))
    }
}

struct Uses {
    tx: std::sync::atomic::AtomicBool,
    rx: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
struct Use {
    state: Arc<Uses>,
    tx: bool,
}
impl Drop for Use {
    fn drop(&mut self) {
        if self.tx {
            &self.state.tx
        } else {
            &self.state.rx
        }
        .store(false, std::sync::atomic::Ordering::Release);
        self.state.changed.notify_waiters();
    }
}
pub(super) struct Control {
    cancel: zero_platform_tokio::contract::CancellationToken,
    uses: Arc<Uses>,
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        loop {
            let changed = self.uses.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let live = self.uses.tx.load(std::sync::atomic::Ordering::Acquire)
                || self.uses.rx.load(std::sync::atomic::Ordering::Acquire);
            if !live {
                return Ok(CloseReport {
                    released_resources: vec!["actual inbound Datagram views returned".into()],
                    ..Default::default()
                });
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "inbound Datagram views remain borrowed".into(),
                    )],
                    ..Default::default()
                });
            }
        }
    }
}

pub(super) struct Tx {
    socket: ControlledDatagramListenerTx,
    cancel: zero_platform_tokio::contract::CancellationToken,
    session: Arc<Mutex<Socks5InboundUdpAssociationSession>>,
    pending: Option<BoxFuture<'static, Result<InboundDatagramReceipt, SendFailure>>>,
    _usage: Use,
}
impl<'a> DatagramTx<InboundDatagramResponse<'a>, InboundDatagramReceipt> for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.cancel.is_cancelled() {
            this.pending = None;
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Revoked, Impact::Session),
                DeliveryState::NotSubmitted,
            )));
        }
        if this.pending.is_none() {
            let response = this.session.lock().unwrap().build_response_for_target(
                &frame.source.address,
                frame.source.port,
                frame.payload,
            );
            let socket = this.socket.clone();
            let payload_len = frame.payload.len();
            this.pending = Some(Box::pin(async move {
                let response = response
                    .map_err(|source| {
                        SendFailure::new(
                            ChannelError::new(Stage::Send, Reason::Protocol, Impact::Session)
                                .with_source(source),
                            DeliveryState::NotSubmitted,
                        )
                    })?
                    .ok_or_else(|| {
                        SendFailure::new(
                            ChannelError::new(Stage::Send, Reason::Protocol, Impact::Session),
                            DeliveryState::NotSubmitted,
                        )
                    })?;
                let (recipient, packet) = response.into_parts();
                let written = socket
                    .send_to_addr(
                        &packet,
                        zero_platform_tokio::socket_address_to_socket_addr(recipient),
                    )
                    .await
                    .map_err(|source| {
                        SendFailure::new(
                            io_failure(Stage::Send, source),
                            DeliveryState::NotSubmitted,
                        )
                    })?;
                if written != packet.len() {
                    return Err(SendFailure::new(
                        ChannelError::new(Stage::Send, Reason::Internal, Impact::Session),
                        DeliveryState::Unknown,
                    ));
                }
                Ok(InboundDatagramReceipt {
                    payload: SendReceipt::accepted(payload_len),
                    protocol_bytes: written,
                })
            }));
        }
        let result = this.pending.as_mut().unwrap().as_mut().poll(cx);
        if result.is_ready() {
            this.pending = None;
        }
        result
    }
}
struct Input {
    buffer: alloc::vec::Vec<u8>,
    tcp: MeteredStream<TcpRelayStream>,
    udp: ControlledDatagramListenerRx,
    session: Arc<Mutex<Socks5InboundUdpAssociationSession>>,
    cancel: zero_platform_tokio::contract::CancellationToken,
}
pub(super) struct Rx {
    io: Option<Input>,
    pending:
        Option<BoxFuture<'static, (Input, Result<Option<InboundDatagramEvent>, ChannelError>)>>,
    _usage: Use,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        if this.pending.is_none() {
            let mut io = this.io.take().expect("one receive operation");
            this.pending = Some(Box::pin(async move {
                let mut probe = [0];
                loop {
                    tokio::select! { biased;
                        _ = io.cancel.cancelled() => return (io, Ok(None)),
                        control = tokio::io::AsyncReadExt::read(&mut io.tcp, &mut probe) => { return (io, control.map(|_| None).map_err(|source| io_failure(Stage::Receive, source))); },
                        received = io.udp.recv_from_addr(&mut io.buffer) => {
                            let (read, sender) = match received { Ok(packet) => packet, Err(source) => return (io, Err(io_failure(Stage::Receive, source))) };
                            let event = io.session.lock().unwrap().decode_datagram_event(zero_platform_tokio::socket_addr_to_socket_address(sender), &io.buffer[..read]);
                            match event {
                                Ok(Some(event)) => return (io, Ok(Some(event))),
                                Ok(None) => {},
                                Err(error) => return (io, Err(ChannelError::new(Stage::Receive, Reason::Protocol, Impact::Datagram).with_source(error))),
                            }
                        }
                    }
                }
            }));
        }
        match this.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((io, event)) => {
                this.pending = None;
                this.io = Some(io);
                Poll::Ready(event)
            }
        }
    }
}
