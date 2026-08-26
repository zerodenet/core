use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Error, Session};
use zero_dns::DnsSystem;
use zero_engine::{
    FlowNetworkInterfaceObservation, FlowNetworkObservation, FlowRemoteEndpoint,
    FlowRouteLookupObservation, FlowSocketBindingObservation,
};
use zero_platform_tokio::{EgressSelection, TokioSocket};
use zero_traits::IpAddress;

use super::direct_dial::{dial_tcp_candidates, TcpDialFailure};

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
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> Result<DirectTcpConnection, DirectTcpConnectFailure> {
        let candidates = self.resolve_target_addrs(session, resolver).await?;

        match dial_tcp_candidates(candidates, egress).await {
            Ok(success) => {
                let socket = success.socket;
                let local = socket.local_addr().ok();
                let network = direct_network_observation(
                    &success.selection,
                    local,
                    "connected",
                    success.selection.interface().is_some(),
                );
                Ok(DirectTcpConnection {
                    socket,
                    remote: success.remote,
                    network,
                })
            }
            Err(failure) => {
                log_dial_failure("direct", &failure);
                let error = dial_failure_error(&failure, "failed to connect direct target");
                Err(DirectTcpConnectFailure {
                    stage: "connect_direct",
                    network: Box::new(direct_network_observation(
                        &failure.selection,
                        failure.local_addr,
                        failure.stage,
                        failure.interface_bound,
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
    ) -> Result<Vec<SocketAddr>, Error> {
        self.validate(session)?;

        self.resolve_addresses(
            session.effective_direct_target(),
            session.port,
            resolver,
            "failed to resolve direct target",
        )
        .await
    }

    pub(crate) async fn connect_host(
        &self,
        host: &str,
        port: u16,
        resolver: &DnsSystem,
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> Result<TokioSocket, Error> {
        if port == 0 {
            return Err(Error::Config("target port is required"));
        }

        let candidates =
            resolve_host_addresses(host, port, resolver, "failed to resolve upstream target")
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

    pub(crate) async fn resolve_addresses(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        error_message: &'static str,
    ) -> Result<Vec<SocketAddr>, Error> {
        match address {
            Address::Domain(domain) => {
                resolve_host_addresses(domain, port, resolver, error_message).await
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
        route_source = ?failure.selection.route_source(),
        route_lookup = failure.selection.route_lookup_status().as_str(),
        binding_reason = failure.selection.binding_reason().as_str(),
        egress_name = failure.selection.interface().map(|value| value.name()),
        egress_index = failure.selection.interface().map(|value| value.index()),
        connect_stage = failure.stage,
        error = %failure.error,
        "TCP candidate dial failed"
    );
}

fn dial_failure_error(failure: &TcpDialFailure, connect_error: &'static str) -> Error {
    if failure.stage == "select_egress" {
        Error::Io("TUN physical egress is unavailable")
    } else {
        Error::Io(connect_error)
    }
}

fn direct_network_observation(
    selection: &EgressSelection,
    local: Option<SocketAddr>,
    connect_stage: &str,
    interface_bound: bool,
) -> FlowNetworkObservation {
    FlowNetworkObservation {
        local_address: local.map(|address| FlowRemoteEndpoint {
            host: address.ip().to_string(),
            port: address.port(),
        }),
        selected_interface: selection.interface().map(|interface| {
            FlowNetworkInterfaceObservation {
                name: interface.name().to_owned(),
                index: interface.index(),
            }
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
            interface_bound,
        }),
        connect_stage: Some(connect_stage.to_owned()),
    }
}

impl From<Error> for DirectTcpConnectFailure {
    fn from(error: Error) -> Self {
        Self {
            stage: "resolve_direct_target",
            error,
            network: Box::new(FlowNetworkObservation {
                connect_stage: Some("resolve_target".to_owned()),
                ..FlowNetworkObservation::default()
            }),
        }
    }
}

fn socket_addr_from_ip(ip: IpAddress, port: u16) -> SocketAddr {
    match ip {
        IpAddress::V4(bytes) => SocketAddr::new(IpAddr::V4(Ipv4Addr::from(bytes)), port),
        IpAddress::V6(bytes) => SocketAddr::new(IpAddr::V6(Ipv6Addr::from(bytes)), port),
    }
}

async fn resolve_host_addresses(
    host: &str,
    port: u16,
    resolver: &DnsSystem,
    error_message: &'static str,
) -> Result<Vec<SocketAddr>, Error> {
    let resolved = resolver.resolve_real(host).await.map_err(|error| {
        tracing::warn!(domain = host, error = %error, "real DNS resolution failed");
        Error::Io(error_message)
    })?;
    if resolved.is_empty() {
        return Err(Error::Io("target resolved to no addresses"));
    }
    Ok(resolved
        .into_iter()
        .map(|ip| socket_addr_from_ip(ip, port))
        .collect())
}
