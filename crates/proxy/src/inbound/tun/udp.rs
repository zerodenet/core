use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use zero_core::{
    Address, DatagramUdpResponder, Error, InboundDatagramUdpRelay, InboundUdpDispatch,
    ProtocolType, SessionAuth,
};
use zero_engine::EngineError;
use zero_stack::UserUdpStack;
use zero_traits::{IpAddress, SocketAddress, UdpStack};

use crate::runtime::udp_ingress::UdpIngressRuntime;
use crate::runtime::Proxy;

mod association;
use association::{AdmissionRejection, AssociationRegistry, Delivery};

const ASSOCIATION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ASSOCIATION_QUEUE_CAPACITY: usize = 128;
const MAX_CONCURRENT_DNS_QUERIES: usize = 256;

pub(super) struct TunDatagram {
    destination: SocketAddress,
    payload: Vec<u8>,
}

struct TunUdpRelay {
    source: SocketAddress,
    receiver: mpsc::Receiver<TunDatagram>,
}

struct TunUdpResponder {
    source: SocketAddress,
    receiver: mpsc::Receiver<TunDatagram>,
    current_destination: Option<SocketAddress>,
    session_destinations: HashMap<u64, SocketAddress>,
}

struct AssociationStart {
    proxy: Proxy,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    source: SocketAddress,
    id: u64,
    first: TunDatagram,
}

impl InboundDatagramUdpRelay<Arc<UserUdpStack>> for TunUdpRelay {
    type Responder = TunUdpResponder;

    fn into_datagram_udp_parts(self) -> (Self::Responder, Option<SessionAuth>) {
        (
            TunUdpResponder {
                source: self.source,
                receiver: self.receiver,
                current_destination: None,
                session_destinations: HashMap::new(),
            },
            None,
        )
    }
}

#[async_trait::async_trait]
impl DatagramUdpResponder<Arc<UserUdpStack>> for TunUdpResponder {
    async fn read_inbound_dispatch(
        &mut self,
        _stack: &Arc<UserUdpStack>,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        let datagram =
            match tokio::time::timeout(ASSOCIATION_IDLE_TIMEOUT, self.receiver.recv()).await {
                Ok(Some(datagram)) => datagram,
                Ok(None) | Err(_) => return Ok(None),
            };
        self.current_destination = Some(datagram.destination);
        Ok(Some(InboundUdpDispatch::new(
            ProtocolType::UNKNOWN,
            socket_address_to_address(datagram.destination),
            datagram.destination.port,
            datagram.payload,
            None,
        )))
    }

    fn on_dispatch_success(&mut self, session_id: u64, _dispatch: &InboundUdpDispatch) {
        if let Some(destination) = self.current_destination.take() {
            self.session_destinations.insert(session_id, destination);
        }
    }

    async fn write_response_for_session(
        &mut self,
        stack: &Arc<UserUdpStack>,
        session_id: Option<u64>,
        _target: &Address,
        _port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        let Some(destination) = session_id.and_then(|id| self.session_destinations.get(&id)) else {
            return Ok(None);
        };
        stack.send_to(payload, *destination, self.source).await;
        Ok(Some(payload.len()))
    }
}

pub(super) async fn run(
    proxy: Proxy,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    dns_hijack: bool,
) -> Result<(), EngineError> {
    let mut buffer = vec![0_u8; 65_535];
    let mut next_id = 1_u64;
    let mut associations = AssociationRegistry::new();
    let mut tasks = JoinSet::new();
    let mut dns_tasks = JoinSet::new();
    let mut last_dns_pressure_log = None;

    loop {
        tokio::select! {
            received = stack.recv_from(&mut buffer) => {
                let Some((size, source, destination)) = received else {
                    return Ok(());
                };
                if dns_hijack && destination.port == 53 {
                    let now = Instant::now();
                    if dns_tasks.len() >= MAX_CONCURRENT_DNS_QUERIES {
                        if pressure_log_due(&mut last_dns_pressure_log, now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                active_dns_queries = dns_tasks.len(),
                                "dropping TUN DNS query at the concurrency limit"
                            );
                        }
                        let response = proxy.resolver.busy_response(&buffer[..size]);
                        stack.send_to(&response, destination, source).await;
                        continue;
                    }
                    let resolver = Arc::clone(&proxy.resolver);
                    let stack = Arc::clone(&stack);
                    let query = buffer[..size].to_vec();
                    dns_tasks.spawn(async move {
                        let response = resolver.answer_udp_query(&query).await?;
                        stack.send_to(&response, destination, source).await;
                        Ok::<(), std::io::Error>(())
                    });
                    continue;
                }
                let datagram = TunDatagram {
                    destination,
                    payload: buffer[..size].to_vec(),
                };
                let now = Instant::now();
                tracing::trace!(
                    ?source,
                    ?destination,
                    association_id = associations.association_id(source),
                    active_associations = associations.active_count(),
                    "TUN UDP ingress"
                );
                match associations.deliver(source, datagram) {
                    Delivery::Delivered => {}
                    Delivery::Missing(datagram) => match associations.admit(source, now) {
                        Ok(()) => {
                            let id = next_id;
                            spawn_association(
                                &mut tasks,
                                &mut associations,
                                AssociationStart {
                                    proxy: proxy.clone(),
                                    stack: Arc::clone(&stack),
                                    inbound_tag: inbound_tag.clone(),
                                    source,
                                    id,
                                    first: datagram,
                                },
                            );
                            tracing::trace!(
                                ?source,
                                ?destination,
                                association_id = id,
                                active_associations = associations.active_count(),
                                "TUN UDP association started"
                            );
                            next_id = next_id.wrapping_add(1).max(1);
                        }
                        Err(reason) => log_pressure_drop(
                            &mut associations,
                            now,
                            source,
                            destination,
                            reason,
                        ),
                    },
                    Delivery::Full => {
                        if associations.should_log_pressure(now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                active_associations = associations.active_count(),
                                "dropping TUN UDP datagram because its association queue is full"
                            );
                        }
                    }
                    Delivery::Closed(_datagram) => {
                        if associations.remove(source) {
                            associations.record_failure(source, now);
                        }
                        if associations.should_log_pressure(now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                active_associations = associations.active_count(),
                                "TUN UDP association closed; applying recreate backoff"
                            );
                        }
                    }
                }
            }
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => {
                match completed {
                    Ok((source, id, Ok(()))) => {
                        if associations.remove_matching(source, id) {
                            associations.clear_failure(source);
                        }
                    }
                    Ok((source, id, Err(error))) => {
                        if associations.remove_matching(source, id) {
                            associations.record_failure(source, Instant::now());
                        }
                        tracing::warn!(error = %error, ?source, "TUN UDP association failed");
                    }
                    Err(error) => tracing::warn!(error = %error, "TUN UDP association task panicked"),
                }
            }
            Some(completed) = dns_tasks.join_next(), if !dns_tasks.is_empty() => {
                match completed {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(error = %error, "TUN DNS query failed"),
                    Err(error) => tracing::warn!(error = %error, "TUN DNS task panicked"),
                }
            }
        }
    }
}

fn pressure_log_due(last: &mut Option<Instant>, now: Instant) -> bool {
    if last.is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(1)) {
        return false;
    }
    *last = Some(now);
    true
}

fn spawn_association(
    tasks: &mut JoinSet<(SocketAddress, u64, Result<(), EngineError>)>,
    associations: &mut AssociationRegistry,
    start: AssociationStart,
) {
    let AssociationStart {
        proxy,
        stack,
        inbound_tag,
        source,
        id,
        first,
    } = start;
    let (sender, receiver) = mpsc::channel(ASSOCIATION_QUEUE_CAPACITY);
    sender
        .try_send(first)
        .expect("new TUN UDP association receiver must be open");
    associations.insert(source, id, sender);
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services()).with_source_addr(Some(
        zero_platform_tokio::socket_address_to_socket_addr(source),
    ));
    tasks.spawn(async move {
        let result = crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
            runtime,
            stack,
            TunUdpRelay { source, receiver },
            &inbound_tag,
            true,
        )
        .await;
        (source, id, result)
    });
}

fn log_pressure_drop(
    associations: &mut AssociationRegistry,
    now: Instant,
    source: SocketAddress,
    destination: SocketAddress,
    reason: AdmissionRejection,
) {
    if associations.should_log_pressure(now) {
        tracing::warn!(
            ?source,
            ?destination,
            ?reason,
            active_associations = associations.active_count(),
            "dropping new TUN UDP association under admission pressure"
        );
    }
}

fn socket_address_to_address(address: SocketAddress) -> Address {
    match address.ip {
        IpAddress::V4(ip) => Address::Ipv4(ip),
        IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}

#[cfg(test)]
mod tests;
