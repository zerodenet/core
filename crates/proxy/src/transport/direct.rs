use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Error, Session};
use zero_dns::DnsSystem;
use zero_engine::{
    FlowAddressFamilyFallbackObservation, FlowConnectionAttemptObservation, FlowEgressObservation,
    FlowNetworkInterfaceObservation, FlowNetworkObservation, FlowRemoteEndpoint,
    FlowRouteLookupObservation, FlowSocketBindingObservation,
};
use zero_platform_tokio::{
    EgressBindingReason, EgressInterfaceControl, EgressSelection, TokioSocket,
};
use zero_traits::IpAddress;

use super::direct_dial::{dial_tcp_candidates, TcpDialAttempt, TcpDialFailure};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DirectConnector;

pub(crate) struct DirectTcpConnection {
    pub(crate) socket: TokioSocket,
    pub(crate) remote: SocketAddr,
    pub(crate) network: FlowNetworkObservation,
}

pub(crate) struct DirectTcpConnectFailure {
    pub(crate) stage: &'static str,
    pub(crate) error: Error,
    pub(crate) network: Box<FlowNetworkObservation>,
}

#[derive(Debug, Clone)]
pub(crate) struct DirectTargetResolution {
    pub(crate) candidates: Vec<SocketAddr>,
    address_family_policy: &'static str,
    fallback: Option<DirectAddressFamilyFallback>,
}

#[derive(Debug, Clone)]
struct DirectAddressFamilyFallback {
    trigger_egress_generation: u64,
    unavailable_reason: Option<String>,
    original_target: SocketAddr,
    domain: String,
    host_source: &'static str,
    reason: &'static str,
    preserve_original_ipv6: bool,
}

impl DirectConnector {
    pub(crate) fn validate(&self, session: &Session) -> Result<(), Error> {
        if session.port == 0 {
            return Err(Error::Config("target port is required"));
        }

        Ok(())
    }

    pub(crate) async fn connect(
        &self,
        session: &Session,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> Result<DirectTcpConnection, DirectTcpConnectFailure> {
        let resolution = match self.resolve_target_addrs(session, resolver, egress).await {
            Ok(resolution) => resolution,
            Err(error) => {
                return Err(DirectTcpConnectFailure {
                    stage: "resolve_direct_target",
                    error,
                    network: Box::new(
                        self.resolution_failure_observation(session, resolver, egress),
                    ),
                });
            }
        };

        match dial_tcp_candidates(resolution.candidates.clone(), egress).await {
            Ok(success) => {
                let socket = success.socket;
                let local = socket.local_addr().ok();
                let network = direct_network_observation(
                    &success.selection,
                    DirectNetworkDialObservation {
                        local,
                        remote: success.remote,
                        resolved_candidates: &success.resolved_candidates,
                        attempts: &success.attempts,
                        connect_stage: "connected",
                        interface_bound: success.selection.interface().is_some(),
                    },
                    &resolution,
                );
                if let Some(fallback) = resolution
                    .fallback
                    .as_ref()
                    .filter(|_| success.remote.is_ipv4())
                {
                    egress.record_ipv6_to_ipv4_fallback();
                    log_address_family_fallback_result(
                        "address_family_fallback_succeeded",
                        fallback,
                        "connected",
                        Some(success.remote),
                        None,
                    );
                }
                Ok(DirectTcpConnection {
                    socket,
                    remote: success.remote,
                    network,
                })
            }
            Err(failure) => {
                log_dial_failure("direct", &failure);
                if let Some(fallback) = resolution.fallback.as_ref() {
                    log_address_family_fallback_result(
                        "address_family_fallback_failed",
                        fallback,
                        failure.stage,
                        Some(failure.remote),
                        Some(&failure.error),
                    );
                }
                let error = dial_failure_error(&failure, "failed to connect direct target");
                Err(DirectTcpConnectFailure {
                    stage: "connect_direct",
                    network: Box::new(direct_network_observation(
                        &failure.selection,
                        DirectNetworkDialObservation {
                            local: failure.local_addr,
                            remote: failure.remote,
                            resolved_candidates: &failure.resolved_candidates,
                            attempts: &failure.attempts,
                            connect_stage: failure.stage,
                            interface_bound: failure.interface_bound,
                        },
                        &resolution,
                    )),
                    error,
                })
            }
        }
    }

    pub(crate) async fn resolve_target_addrs(
        &self,
        session: &Session,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> Result<DirectTargetResolution, Error> {
        self.validate(session)?;

        for attempt in 0..2 {
            let generation = egress.generation();
            let result = self
                .resolve_target_addrs_at_generation(session, resolver, egress)
                .await;
            let current_generation = egress.generation();
            if current_generation == generation {
                return result;
            }
            tracing::debug!(
                resolution_generation = generation,
                current_egress_generation = current_generation,
                retry = attempt == 0,
                "discarding direct resolution from an obsolete egress generation"
            );
        }

        Err(Error::Io(
            "TUN egress changed repeatedly during direct target resolution",
        ))
    }

    async fn resolve_target_addrs_at_generation(
        &self,
        session: &Session,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> Result<DirectTargetResolution, Error> {
        let (target, fallback) = direct_resolution_target(session, egress);
        if let Some(fallback) = fallback.as_ref() {
            tracing::info!(
                event_type = "address_family_fallback_started",
                original_target = %fallback.original_target,
                domain = %fallback.domain,
                host_source = fallback.host_source,
                from_address_family = "ipv6",
                to_address_family = "ipv4",
                reason = fallback.reason,
                egress_generation = fallback.trigger_egress_generation,
                egress_unavailable_reason = fallback.unavailable_reason.as_deref(),
                "address-family fallback started"
            );
        }
        let candidates = if let Some(fallback) = fallback.as_ref() {
            let Address::Domain(domain) = target else {
                return Err(Error::Io(
                    "direct IPv6 fallback requires a trusted domain target",
                ));
            };
            let ipv4_candidates = match resolve_direct_ipv4_host_addresses(
                domain,
                session.port,
                resolver,
                "failed to resolve direct target",
            )
            .await
            {
                Ok(candidates) => candidates,
                Err(error) if !fallback.preserve_original_ipv6 => {
                    log_address_family_fallback_result(
                        "address_family_fallback_failed",
                        fallback,
                        "resolve_ipv4",
                        None,
                        Some(&error),
                    );
                    return Err(error);
                }
                Err(error) => {
                    tracing::debug!(
                        domain,
                        error = %error,
                        "IPv4 connectivity fallback resolution failed; retaining original IPv6 candidate"
                    );
                    Vec::new()
                }
            };
            if ipv4_candidates.is_empty() && !fallback.preserve_original_ipv6 {
                log_address_family_fallback_result(
                    "address_family_fallback_failed",
                    fallback,
                    "resolve_ipv4",
                    None,
                    None,
                );
                return Err(Error::Io(
                    "direct IPv6 fallback resolved to no IPv4 addresses",
                ));
            }
            if fallback.preserve_original_ipv6 {
                let mut candidates = vec![fallback.original_target];
                candidates.extend(ipv4_candidates);
                candidates
            } else {
                ipv4_candidates
            }
        } else {
            self.resolve_addresses(
                target,
                session.port,
                resolver,
                "failed to resolve direct target",
            )
            .await?
        };
        Ok(DirectTargetResolution {
            candidates,
            address_family_policy: resolver.address_family_policy().as_str(),
            fallback,
        })
    }

    pub(crate) fn udp_network_observation(
        &self,
        resolution: &DirectTargetResolution,
        remote: SocketAddr,
        egress: &EgressInterfaceControl,
    ) -> FlowNetworkObservation {
        let selection = egress.select_for_peer(remote);
        direct_network_observation(
            &selection,
            DirectNetworkDialObservation {
                local: None,
                remote,
                resolved_candidates: &resolution.candidates,
                attempts: &[],
                connect_stage: "sent",
                interface_bound: selection.interface().is_some(),
            },
            resolution,
        )
    }

    pub(crate) fn resolution_failure_observation(
        &self,
        session: &Session,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> FlowNetworkObservation {
        let (_, fallback) = direct_resolution_target(session, egress);
        FlowNetworkObservation {
            address_family_policy: Some(resolver.address_family_policy().as_str().to_owned()),
            address_family_fallback: fallback.as_ref().map(fallback_observation),
            connect_stage: Some("resolve_target".to_owned()),
            ..FlowNetworkObservation::default()
        }
    }

    pub(crate) async fn connect_host(
        &self,
        host: &str,
        port: u16,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> Result<TokioSocket, Error> {
        if port == 0 {
            return Err(Error::Config("target port is required"));
        }

        let candidates =
            resolve_node_host_addresses(host, port, resolver, "failed to resolve upstream target")
                .await?;
        dial_tcp_candidates(candidates, egress)
            .await
            .map(|success| success.socket)
            .map_err(|failure| {
                log_dial_failure("upstream", &failure);
                dial_failure_error(&failure, "failed to connect upstream target")
            })
    }

    pub(crate) async fn resolve_address(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        error_message: &'static str,
    ) -> Result<SocketAddr, Error> {
        self.resolve_addresses(address, port, resolver, error_message)
            .await?
            .into_iter()
            .next()
            .ok_or(Error::Io("target resolved to no addresses"))
    }

    pub(crate) async fn resolve_node_address(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        error_message: &'static str,
    ) -> Result<SocketAddr, Error> {
        match address {
            Address::Domain(domain) => {
                resolve_node_host_addresses(domain, port, resolver, error_message).await
            }
            Address::Ipv4(bytes) => Ok(vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(*bytes)),
                port,
            )]),
            Address::Ipv6(bytes) => Ok(vec![SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(*bytes)),
                port,
            )]),
        }?
        .into_iter()
        .next()
        .ok_or(Error::Io("node resolved to no addresses"))
    }

    pub(crate) async fn resolve_addresses(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        error_message: &'static str,
    ) -> Result<Vec<SocketAddr>, Error> {
        match address {
            Address::Domain(domain) => {
                resolve_direct_host_addresses(domain, port, resolver, error_message).await
            }
            Address::Ipv4(bytes) => Ok(vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(*bytes)),
                port,
            )]),
            Address::Ipv6(bytes) => Ok(vec![SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(*bytes)),
                port,
            )]),
        }
    }
}

fn log_dial_failure(kind: &str, failure: &TcpDialFailure) {
    tracing::debug!(
        connect_kind = kind,
        target = %failure.remote,
        candidates = ?failure.resolved_candidates,
        egress_generation = failure.selection.generation(),
        tun_active = failure.selection.tun_active(),
        route_source = ?failure.selection.route_source(),
        route_lookup = failure.selection.route_lookup_status().as_str(),
        binding_reason = failure.selection.binding_reason().as_str(),
        egress_name = failure.selection.interface().map(|value| value.name()),
        egress_index = failure.selection.interface().map(|value| value.index()),
        configured_egress_name = failure
            .selection
            .configured_interface()
            .map(|value| value.name()),
        configured_egress_index = failure
            .selection
            .configured_interface()
            .map(|value| value.index()),
        egress_unavailable_reason = failure.selection.unavailable_reason(),
        connect_stage = failure.stage,
        error = %failure.error,
        "TCP candidate dial failed"
    );
}

fn dial_failure_error(failure: &TcpDialFailure, connect_error: &'static str) -> Error {
    if failure.stage == "select_egress" {
        if failure.remote.is_ipv6() {
            Error::Io("tun_ipv6_egress_unavailable")
        } else {
            Error::Io("tun_ipv4_egress_unavailable")
        }
    } else {
        Error::Io(connect_error)
    }
}

struct DirectNetworkDialObservation<'a> {
    local: Option<SocketAddr>,
    remote: SocketAddr,
    resolved_candidates: &'a [SocketAddr],
    attempts: &'a [TcpDialAttempt],
    connect_stage: &'a str,
    interface_bound: bool,
}

fn direct_network_observation(
    selection: &EgressSelection,
    dial: DirectNetworkDialObservation<'_>,
    resolution: &DirectTargetResolution,
) -> FlowNetworkObservation {
    FlowNetworkObservation {
        local_address: dial.local.map(|address| FlowRemoteEndpoint {
            host: address.ip().to_string(),
            port: address.port(),
        }),
        remote_address: Some(socket_endpoint(dial.remote)),
        resolved_candidates: dial
            .resolved_candidates
            .iter()
            .copied()
            .map(socket_endpoint)
            .collect(),
        connection_attempts: dial
            .attempts
            .iter()
            .map(|attempt| FlowConnectionAttemptObservation {
                remote_address: socket_endpoint(attempt.remote),
                local_address: attempt.local_addr.map(socket_endpoint),
                stage: attempt.stage.to_owned(),
                outcome: attempt.outcome.to_owned(),
                interface_bound: attempt.interface_bound,
                error_kind: attempt.error_kind.map(ToOwned::to_owned),
                os_error: attempt.os_error,
                error: attempt.error.clone(),
            })
            .collect(),
        address_family_policy: Some(resolution.address_family_policy.to_owned()),
        address_family_fallback: resolution
            .fallback
            .as_ref()
            .filter(|fallback| !fallback.preserve_original_ipv6 || dial.remote.is_ipv4())
            .map(fallback_observation),
        selected_interface: selection.interface().map(|interface| {
            FlowNetworkInterfaceObservation {
                name: interface.name().to_owned(),
                index: interface.index(),
            }
        }),
        egress: Some(FlowEgressObservation {
            generation: selection.generation(),
            address_family: if dial.remote.is_ipv6() {
                "ipv6"
            } else {
                "ipv4"
            }
            .to_owned(),
            tun_active: selection.tun_active(),
            configured_interface: selection.configured_interface().map(|interface| {
                FlowNetworkInterfaceObservation {
                    name: interface.name().to_owned(),
                    index: interface.index(),
                }
            }),
            unavailable_reason: selection.unavailable_reason().map(ToOwned::to_owned),
        }),
        route_lookup: Some(FlowRouteLookupObservation {
            status: selection.route_lookup_status().as_str().to_owned(),
            source_address: selection.route_source().map(|source| source.to_string()),
            error: selection.route_lookup_error().map(ToOwned::to_owned),
        }),
        socket_binding: Some(FlowSocketBindingObservation {
            mode: if selection.interface().is_some() {
                "interface"
            } else {
                "system"
            }
            .to_owned(),
            reason: selection.binding_reason().as_str().to_owned(),
            interface_bound: dial.interface_bound,
        }),
        connect_stage: Some(dial.connect_stage.to_owned()),
    }
}

fn direct_resolution_target<'a>(
    session: &'a Session,
    egress: &EgressInterfaceControl,
) -> (&'a Address, Option<DirectAddressFamilyFallback>) {
    let direct_target = session.effective_direct_target();
    let (Address::Domain(domain), Some(host_source)) =
        (&session.target, session.target_host_source)
    else {
        return (direct_target, None);
    };
    let original_ipv6 = match direct_target {
        Address::Ipv6(octets) => Some(*octets),
        Address::Domain(_) => match session.original_target.as_ref() {
            Some(Address::Ipv6(octets)) => Some(*octets),
            _ => None,
        },
        Address::Ipv4(_) => None,
    };
    let Some(original_ipv6) = original_ipv6 else {
        return (direct_target, None);
    };
    let peer = SocketAddr::new(IpAddr::V6(Ipv6Addr::from(original_ipv6)), session.port);
    let selection = egress.select_for_peer(peer);
    let (reason, preserve_original_ipv6) =
        if selection.binding_reason() == EgressBindingReason::TunEgressUnavailable {
            ("tun_ipv6_egress_unavailable", false)
        } else if selection.tun_active() {
            ("ipv6_connect_failed", true)
        } else {
            return (direct_target, None);
        };

    (
        &session.target,
        Some(DirectAddressFamilyFallback {
            trigger_egress_generation: selection.generation(),
            unavailable_reason: selection.unavailable_reason().map(ToOwned::to_owned),
            original_target: peer,
            domain: domain.clone(),
            host_source: host_source.as_str(),
            reason,
            preserve_original_ipv6,
        }),
    )
}

fn log_address_family_fallback_result(
    event_type: &'static str,
    fallback: &DirectAddressFamilyFallback,
    stage: &'static str,
    selected_target: Option<SocketAddr>,
    error: Option<&dyn std::fmt::Display>,
) {
    tracing::info!(
        event_type,
        original_target = %fallback.original_target,
        domain = %fallback.domain,
        host_source = fallback.host_source,
        from_address_family = "ipv6",
        to_address_family = "ipv4",
        reason = fallback.reason,
        egress_generation = fallback.trigger_egress_generation,
        egress_unavailable_reason = fallback.unavailable_reason.as_deref(),
        stage,
        selected_target = selected_target.map(|target| target.to_string()),
        error = error.map(ToString::to_string),
        "address-family fallback completed"
    );
}

fn fallback_observation(
    fallback: &DirectAddressFamilyFallback,
) -> FlowAddressFamilyFallbackObservation {
    FlowAddressFamilyFallbackObservation {
        from: "ipv6".to_owned(),
        to: "ipv4".to_owned(),
        reason: fallback.reason.to_owned(),
        trigger_egress_generation: fallback.trigger_egress_generation,
        unavailable_reason: fallback.unavailable_reason.clone(),
    }
}

fn socket_endpoint(address: SocketAddr) -> FlowRemoteEndpoint {
    FlowRemoteEndpoint {
        host: address.ip().to_string(),
        port: address.port(),
    }
}

fn socket_addr_from_ip(ip: IpAddress, port: u16) -> SocketAddr {
    match ip {
        IpAddress::V4(bytes) => SocketAddr::new(IpAddr::V4(Ipv4Addr::from(bytes)), port),
        IpAddress::V6(bytes) => SocketAddr::new(IpAddr::V6(Ipv6Addr::from(bytes)), port),
    }
}

async fn resolve_direct_host_addresses(
    host: &str,
    port: u16,
    resolver: &DnsSystem,
    error_message: &'static str,
) -> Result<Vec<SocketAddr>, Error> {
    let resolved = resolver.resolve_direct(host).await.map_err(|error| {
        tracing::debug!(
            domain = host,
            role = "direct",
            error = %error,
            "real DNS resolution failed"
        );
        Error::Io(error_message)
    })?;
    resolved_socket_addresses(resolved, port)
}

async fn resolve_direct_ipv4_host_addresses(
    host: &str,
    port: u16,
    resolver: &DnsSystem,
    error_message: &'static str,
) -> Result<Vec<SocketAddr>, Error> {
    let resolved = resolver.resolve_direct_ipv4(host).await.map_err(|error| {
        tracing::debug!(
            domain = host,
            role = "direct",
            address_family = "ipv4",
            error = %error,
            "real DNS IPv4 fallback resolution failed"
        );
        Error::Io(error_message)
    })?;
    resolved_socket_addresses(resolved, port)
}

async fn resolve_node_host_addresses(
    host: &str,
    port: u16,
    resolver: &DnsSystem,
    error_message: &'static str,
) -> Result<Vec<SocketAddr>, Error> {
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(address, port)]);
    }
    let resolved = resolver.resolve_node(host).await.map_err(|error| {
        tracing::debug!(
            domain = host,
            role = "node",
            error = %error,
            "real DNS resolution failed"
        );
        Error::Io(error_message)
    })?;
    resolved_socket_addresses(resolved, port)
}

fn resolved_socket_addresses(
    resolved: Vec<IpAddress>,
    port: u16,
) -> Result<Vec<SocketAddr>, Error> {
    if resolved.is_empty() {
        return Err(Error::Io("target resolved to no addresses"));
    }
    Ok(resolved
        .into_iter()
        .map(|ip| socket_addr_from_ip(ip, port))
        .collect())
}
