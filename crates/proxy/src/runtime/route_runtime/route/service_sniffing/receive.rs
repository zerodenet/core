//! A cancelled receive waiter retains the actual in-progress inspection future,
//! original channel, queued sender/auth/accounting facts and consumed payloads.
use crate::runtime::sniff::udp::UdpSniffingState;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{
    datagram::{recv, DatagramRx},
    inbound::InboundDatagramEvent as Event,
    BoxFuture, ChannelError,
};
type Rx = Pin<Box<dyn DatagramRx<Event>>>;
type ResultEvent = Result<Option<Event>, ChannelError>;
struct Metadata {
    sender: std::net::SocketAddr,
    bytes: u64,
    auth: Option<zero_core::SessionAuth>,
}
struct Reading {
    rx: Rx,
    sniffing: UdpSniffingState,
    metadata: VecDeque<Metadata>,
    deferred: Option<ResultEvent>,
    ended: bool,
}
impl Reading {
    async fn next(&mut self) -> ResultEvent {
        loop {
            if let Some(dispatch) = self.sniffing.pop_ready() {
                let Metadata {
                    sender,
                    bytes,
                    auth,
                } = self
                    .metadata
                    .pop_front()
                    .expect("one original envelope per inspected packet");
                return Ok(Some(match auth {
                    Some(authentication) => Event::AuthenticatedPacket {
                        dispatch,
                        sender,
                        protocol_bytes: bytes,
                        authentication,
                    },
                    None => Event::Packet {
                        dispatch,
                        sender,
                        protocol_bytes: bytes,
                    },
                }));
            }
            if self.metadata.is_empty() {
                if let Some(event) = self.deferred.take() {
                    return event;
                }
            }
            if self.sniffing.release_for_capacity() {
                continue;
            }
            if self.sniffing.row_pressure() {
                self.sniffing.wait_capacity().await;
                continue;
            }
            if self.ended {
                return Ok(None);
            }
            // This is the same finite inspection backlog as the existing MUX
            // path. Flush old decisions before accepting more envelope rows.
            if self.metadata.len() >= 64 {
                self.sniffing.flush_all();
                continue;
            }
            let event = if let Some(deadline) = self.sniffing.next_deadline() {
                match tokio::time::timeout_at(deadline.into(), recv(self.rx.as_mut())).await {
                    Ok(event) => event,
                    Err(_) => {
                        self.sniffing.flush_expired(std::time::Instant::now());
                        continue;
                    }
                }
            } else {
                recv(self.rx.as_mut()).await
            };
            let (packet, metadata) = match event {
                Ok(Some(Event::Packet {
                    dispatch,
                    sender,
                    protocol_bytes,
                })) => (
                    dispatch,
                    Metadata {
                        sender,
                        bytes: protocol_bytes,
                        auth: None,
                    },
                ),
                Ok(Some(Event::AuthenticatedPacket {
                    dispatch,
                    sender,
                    protocol_bytes,
                    authentication,
                })) => (
                    dispatch,
                    Metadata {
                        sender,
                        bytes: protocol_bytes,
                        auth: Some(authentication),
                    },
                ),
                other => {
                    self.ended = matches!(&other, Ok(None));
                    self.deferred = Some(other);
                    self.sniffing.flush_all();
                    continue;
                }
            };
            self.metadata.push_back(metadata);
            self.sniffing.observe(packet).await;
        }
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "the same prepaid receive state retains original buffers across cancellation"
)]
enum State {
    Ready(Option<Reading>),
    Reading(BoxFuture<'static, (Reading, ResultEvent)>),
}
pub(super) struct InspectedRx {
    state: State,
}
impl InspectedRx {
    pub(super) fn new(rx: Rx, sniffing: UdpSniffingState) -> Self {
        Self {
            state: State::Ready(Some(Reading {
                rx,
                sniffing,
                metadata: VecDeque::new(),
                deferred: None,
                ended: false,
            })),
        }
    }
}
impl DatagramRx<Event> for InspectedRx {
    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ResultEvent> {
        loop {
            match &mut self.state {
                State::Ready(reading) => {
                    let mut reading = reading.take().expect("same unique inspection receiver");
                    self.state = State::Reading(Box::pin(async move {
                        let result = reading.next().await;
                        (reading, result)
                    }));
                }
                State::Reading(next) => {
                    let (reading, result) = std::task::ready!(next.as_mut().poll(cx));
                    self.state = State::Ready(Some(reading));
                    return Poll::Ready(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
