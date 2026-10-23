use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

use super::sniff::udp::TunQuicSniffer;
use crate::runtime::udp_ingress::UdpIngressRuntime;

#[cfg(feature = "raw-ip-runtime")]
mod owned;
#[cfg(feature = "raw-ip-runtime")]
mod scoped;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use owned::OwnedTunUdpTasks;
mod association;
mod run;
mod task_set;
use association::{AdmissionRejection, AssociationRegistry, Delivery};
use run::run_inner;

#[cfg(feature = "raw-ip-runtime")]
type AssociationBinding = Arc<crate::runtime::inbound_owners::HostPeerBinding>;
#[cfg(not(feature = "raw-ip-runtime"))]
type AssociationBinding = ();

const ASSOCIATION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ASSOCIATION_QUEUE_CAPACITY: usize = 128;
const MAX_CONCURRENT_DNS_QUERIES: usize = 256;

pub(super) struct TunDatagram {
    pub(super) peer_identity: Option<Arc<str>>,
    pub(super) destination: SocketAddress,
    pub(super) payload: Vec<u8>,
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
    quic_sniffer: TunQuicSniffer,
}

struct AssociationStart {
    runtime: UdpIngressRuntime,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    source: SocketAddress,
    id: u64,
    first: TunDatagram,
    #[cfg(feature = "raw-ip-runtime")]
    native: Option<crate::runtime::inbound_operation::raw_ip::native_obligations::NativePermit>,
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
                quic_sniffer: TunQuicSniffer::default(),
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
        let datagram = match tokio::time::timeout(
            ASSOCIATION_IDLE_TIMEOUT,
            self.quic_sniffer.next(&mut self.receiver),
        )
        .await
        {
            Ok(Some(datagram)) => datagram,
            Ok(None) | Err(_) => return Ok(None),
        };
        self.current_destination = Some(datagram.original_destination);
        let original_target = socket_address_to_address(datagram.original_destination);
        let dispatch = InboundUdpDispatch::new(
            ProtocolType::UNKNOWN,
            datagram.target,
            datagram.original_destination.port,
            datagram.payload,
            None,
        )
        .with_peer_identity(datagram.peer_identity);
        Ok(Some(match datagram.host_source {
            Some(source) => dispatch.with_transparent_domain(original_target, source),
            None => dispatch.with_transparent_target(),
        }))
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

pub(crate) async fn run_with_runtime(
    runtime: UdpIngressRuntime,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
) -> Result<(), EngineError> {
    run_inner(
        runtime,
        stack,
        inbound_tag,
        dns_hijack,
        dns_hijacked_queries,
        #[cfg(feature = "raw-ip-runtime")]
        None,
    )
    .await
}

#[cfg(feature = "raw-ip-runtime")]
pub(crate) async fn run_with_owned_tasks(
    runtime: UdpIngressRuntime,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    owners: OwnedTunUdpTasks,
) -> Result<(), EngineError> {
    run_inner(
        runtime,
        stack,
        inbound_tag,
        false,
        Arc::new(AtomicU64::new(0)),
        Some(owners),
    )
    .await
}

fn pressure_log_due(last: &mut Option<Instant>, now: Instant) -> bool {
    if last.is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(1)) {
        return false;
    }
    *last = Some(now);
    true
}

fn spawn_association(
    tasks: &mut task_set::TunTasks<'_, (SocketAddress, u64, Result<(), EngineError>)>,
    associations: &mut AssociationRegistry,
    start: AssociationStart,
) -> Result<(), EngineError> {
    let AssociationStart {
        runtime,
        stack,
        inbound_tag,
        source,
        id,
        first,
        #[cfg(feature = "raw-ip-runtime")]
        mut native,
    } = start;
    let (sender, receiver) = mpsc::channel(ASSOCIATION_QUEUE_CAPACITY);
    sender
        .try_send(first)
        .expect("new TUN UDP association receiver must be open");
    associations.insert(source, id, sender);
    let runtime = runtime.with_source_addr(Some(
        zero_platform_tokio::socket_address_to_socket_addr(source),
    ));
    #[cfg(feature = "raw-ip-runtime")]
    let scope = tasks.scope();
    #[cfg(feature = "raw-ip-runtime")]
    let ingress = scope.as_ref().map(|_| runtime.peer_ingress()).transpose()?;
    let registered = tasks.spawn_bound(move |binding| async move {
        #[cfg(feature = "raw-ip-runtime")]
        let (runtime, driver_job) = match (binding, scope, ingress) {
            (Some(binding), Some(scope), Some(ingress)) => {
                if let Err(error) = binding.wait_bound().await {
                    return (source, id, Err(std::io::Error::other(error).into()));
                }
                if let Some(native) = &native {
                    if let Err(error) = native.bind_caller(binding.clone()) {
                        return (source, id, Err(std::io::Error::other(error).into()));
                    }
                }
                let (slot, job) =
                    crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot::channel();
                (
                    runtime
                        .with_peer_resources(scope.group.resource_context(binding, ingress))
                        .with_direct_peer_driver(slot),
                    Some(job),
                )
            }
            (None, None, None) => (runtime, None),
            _ => {
                return (
                    source,
                    id,
                    Err(std::io::Error::other("raw-IP association Scope origin mismatch").into()),
                )
            }
        };
        #[cfg(feature = "raw-ip-runtime")]
        let runtime = if native.is_some() {
            runtime
                .with_native(native.as_ref().map(|permit| permit.registration()))
                .with_bound_direct_budget(owned::direct_budget())
        } else {
            runtime
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = binding;
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(native) = &mut native {
            if let Err(error) = native.enter() {
                return (source, id, Err(std::io::Error::other(error).into()));
            }
        }
        let relay = crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
            runtime,
            stack,
            TunUdpRelay { source, receiver },
            &inbound_tag,
            true,
        );
        #[cfg(feature = "raw-ip-runtime")]
        let result = match driver_job {
            Some(job) => {
                crate::runtime::udp_socket::endpoint::peer::job::run_peer_preserving(relay, job)
                    .await
            }
            None => relay.await,
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let result = relay.await;
        #[cfg(feature = "raw-ip-runtime")]
        let result = result.map_err(|error| match &native {
            Some(native) => EngineError::Io(native.retain_error(error)),
            None => error,
        });
        (source, id, result)
    });
    if registered.is_err() {
        associations.remove_matching(source, id);
    }
    registered
}

fn log_pressure_drop(
    associations: &mut AssociationRegistry,
    now: Instant,
    source: SocketAddress,
    destination: SocketAddress,
    reason: AdmissionRejection,
    feedback_sent: bool,
) {
    if associations.should_log_pressure(now) {
        tracing::warn!(
            ?source,
            ?destination,
            ?reason,
            feedback_sent,
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
