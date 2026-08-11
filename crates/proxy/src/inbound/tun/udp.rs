use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

const ASSOCIATION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ASSOCIATION_QUEUE_CAPACITY: usize = 128;

struct TunDatagram {
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

struct Association {
    id: u64,
    sender: mpsc::Sender<TunDatagram>,
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
    let mut associations = HashMap::<SocketAddress, Association>::new();
    let mut tasks = JoinSet::new();
    let mut dns_tasks = JoinSet::new();

    loop {
        tokio::select! {
            received = stack.recv_from(&mut buffer) => {
                let Some((size, source, destination)) = received else {
                    return Ok(());
                };
                if dns_hijack && destination.port == 53 {
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
                let send_failed = match associations.get(&source) {
                    Some(association) => association.sender.send(datagram).await.is_err(),
                    None => {
                        spawn_association(
                            &mut tasks,
                            &mut associations,
                            AssociationStart {
                                proxy: proxy.clone(),
                                stack: Arc::clone(&stack),
                                inbound_tag: inbound_tag.clone(),
                                source,
                                id: next_id,
                                first: datagram,
                            },
                        ).await;
                        next_id = next_id.wrapping_add(1).max(1);
                        false
                    }
                };
                if send_failed {
                    associations.remove(&source);
                    spawn_association(
                        &mut tasks,
                        &mut associations,
                        AssociationStart {
                            proxy: proxy.clone(),
                            stack: Arc::clone(&stack),
                            inbound_tag: inbound_tag.clone(),
                            source,
                            id: next_id,
                            first: TunDatagram {
                                destination,
                                payload: buffer[..size].to_vec(),
                            },
                        },
                    ).await;
                    next_id = next_id.wrapping_add(1).max(1);
                }
            }
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => {
                match completed {
                    Ok((source, id, Ok(()))) => remove_matching(&mut associations, source, id),
                    Ok((source, id, Err(error))) => {
                        remove_matching(&mut associations, source, id);
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

async fn spawn_association(
    tasks: &mut JoinSet<(SocketAddress, u64, Result<(), EngineError>)>,
    associations: &mut HashMap<SocketAddress, Association>,
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
        .send(first)
        .await
        .expect("new TUN UDP association receiver must be open");
    associations.insert(
        source,
        Association {
            id,
            sender: sender.clone(),
        },
    );
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
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

fn remove_matching(
    associations: &mut HashMap<SocketAddress, Association>,
    source: SocketAddress,
    id: u64,
) {
    if associations
        .get(&source)
        .is_some_and(|association| association.id == id)
    {
        associations.remove(&source);
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
