//! A logical XUDP association keeps its original decoder, termination and response queue.
use super::*;
use zero_platform_tokio::contract::{
    datagram::{DatagramRx, DatagramSession, DatagramTx},
    DeliveryState, SendFailure, SendReceipt,
};
pub(super) struct Reply {
    pub uuid: [u8; 16],
    pub relay: VlessInboundMuxUdpRelay,
    pub facts: zero_core::request::RequestFacts,
    pub sender: std::net::SocketAddr,
}
impl DatagramReply for Reply {
    fn continuity(&self) -> Option<DatagramContinuity> {
        self.relay.continuity_key.as_ref().map(|key| {
            let mut identity = self.uuid.to_vec();
            identity.extend_from_slice(key.as_bytes());
            DatagramContinuity {
                key: zero_core::UdpContinuityKey::from_bytes(&identity)
                    .expect("bounded authenticated XUDP identity"),
                termination: self.relay.termination_probe.clone(),
            }
        })
    }
    type Tx = Tx;
    type Rx = Rx;
    type Control = datagram::Control;
    fn facts(&self) -> &zero_core::request::RequestFacts {
        &self.facts
    }
    async fn respond(
        mut self,
        outcome: ReplyOutcome,
    ) -> DatagramReplyResult<Tx, Rx, Self::Control> {
        if outcome != ReplyOutcome::LocalChannelReady {
            let _ = self.relay.end_inbound_stream();
            return Ok(None);
        }
        let (up, down, control) = datagram::views();
        Ok(Some(InboundDatagramSession {
            negotiation: Default::default(),
            io: DatagramSession {
                tx: Tx {
                    responder: self.relay.responder.clone(),
                    usage: down,
                },
                rx: Rx {
                    relay: self.relay,
                    sender: self.sender,
                    usage: up,
                },
                control,
                limits: {
                    let mut limits = datagram::limits();
                    limits.max_payload = MUX_MAX_PAYLOAD;
                    limits.queued_bytes = MUX_MAX_PAYLOAD;
                    limits
                },
            },
        }))
    }
}
pub(super) struct Tx {
    responder: crate::udp::VlessInboundMuxUdpResponder,
    usage: datagram::Use,
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
        packet: &InboundDatagramResponse<'a>,
    ) -> Poll<Result<InboundDatagramReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.usage.cancelled() {
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Revoked, Impact::Session),
                DeliveryState::NotSubmitted,
            )));
        }
        Poll::Ready(
            this.responder
                .write_standard_response(&packet.source.address, packet.source.port, packet.payload)
                .map(|bytes| InboundDatagramReceipt {
                    payload: SendReceipt::accepted(packet.payload.len()),
                    protocol_bytes: bytes,
                })
                .map_err(|source| {
                    SendFailure::new(datagram::failure(source), DeliveryState::NotSubmitted)
                }),
        )
    }
}
pub(super) struct Rx {
    relay: VlessInboundMuxUdpRelay,
    sender: std::net::SocketAddr,
    usage: datagram::Use,
}
impl DatagramRx<InboundDatagramEvent> for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        let this = self.get_mut();
        if this.usage.cancelled() {
            return Poll::Ready(Ok(None));
        }
        match this.relay.up_rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(bytes)) if !bytes.is_empty() => Poll::Ready(
                this.relay
                    .responder
                    .decode_inbound_dispatch(&bytes)
                    .map(|dispatch| {
                        Some(InboundDatagramEvent::Packet {
                            dispatch,
                            sender: this.sender,
                            protocol_bytes: 0,
                        })
                    })
                    .map_err(|source| {
                        let impact = match this.relay.responder.decode_failure() {
                            MuxUdpDecodeFailure::Continue => Impact::Datagram,
                            MuxUdpDecodeFailure::End => Impact::Session,
                        };
                        ChannelError::new(Stage::Receive, Reason::Io, impact).with_source(source)
                    }),
            ),
            Poll::Ready(_) => Poll::Ready(Ok(None)),
        }
    }
}
