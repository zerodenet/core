use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Error, Session};
use zero_dns::DnsSystem;
use zero_engine::{
    FlowNetworkInterfaceObservation, FlowNetworkObservation, FlowRemoteEndpoint,
    FlowRouteLookupObservation, FlowSocketBindingObservation,
};
use zero_platform_tokio::{EgressSelection, TokioSocket};
use zero_traits::IpAddress;

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
        let addr = self.resolve_target_addr(session, resolver).await?;

        let selection = egress.select_for_peer(addr);
        if let Err(error) = selection.ensure_connectable() {
            tracing::warn!(
                target = %addr,
                route_source = ?selection.route_source(),
                binding_reason = selection.binding_reason().as_str(),
                error = %error,
                "direct TCP connect rejected to prevent TUN self-capture"
            );
            return Err(DirectTcpConnectFailure {
                stage: "connect_direct",
                network: Box::new(direct_network_observation(
                    &selection,
                    None,
                    "select_egress",
                    false,
                )),
                error: Error::Io("TUN physical egress is unavailable"),
            });
        }
        match TokioSocket::connect_addr_on_observed(addr, selection.interface()).await {
            Ok(socket) => {
                let local = socket.local_addr().ok();
                let network = direct_network_observation(
                    &selection,
                    local,
                    "connected",
                    selection.interface().is_some(),
                );
                Ok(DirectTcpConnection {
                    socket,
                    remote: addr,
                    network,
                })
            }
            Err(error) => {
                tracing::debug!(
                    target = %addr,
                    route_source = ?selection.route_source(),
                    route_lookup = selection.route_lookup_status().as_str(),
                    binding_reason = selection.binding_reason().as_str(),
                    egress_name = selection.interface().map(|value| value.name()),
                    egress_index = selection.interface().map(|value| value.index()),
                    connect_stage = error.stage(),
                    error = %error.error(),
                    "direct TCP connect failed"
                );
                let stage = error.stage();
                let interface_bound = error.interface_bound();
                Err(DirectTcpConnectFailure {
                    stage: "connect_direct",
                    network: Box::new(direct_network_observation(
                        &selection,
                        error.local_addr(),
                        stage,
                        interface_bound,
                    )),
                    error: Error::Io("failed to connect direct target"),
                })
            }
        }
    }

    pub(crate) async fn resolve_target_addr(
        &self,
        session: &Session,
        resolver: &DnsSystem,
    ) -> Result<SocketAddr, Error> {
        self.validate(session)?;

        self.resolve_address(
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

        let addr = resolve_host(host, port, resolver, "failed to resolve upstream target").await?;

        let interface = egress.try_current_for_peer(addr).map_err(|error| {
            tracing::warn!(target = %addr, error = %error, "upstream TCP connect rejected to prevent TUN self-capture");
            Error::Io("TUN physical egress is unavailable")
        })?;
        TokioSocket::connect_addr_on(addr, interface.as_ref())
            .await
            .map_err(|error| {
                tracing::debug!(
                    target = %addr,
                    egress_name = interface.as_ref().map(|value| value.name()),
                    egress_index = interface.as_ref().map(|value| value.index()),
                    error = %error,
                    "upstream TCP connect failed"
                );
                Error::Io("failed to connect upstream target")
            })
    }

    pub(crate) async fn resolve_address(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        error_message: &'static str,
    ) -> Result<SocketAddr, Error> {
        match address {
            Address::Domain(domain) => resolve_host(domain, port, resolver, error_message).await,
            Address::Ipv4(bytes) => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(*bytes)), port)),
            Address::Ipv6(bytes) => Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(*bytes)), port)),
        }
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

async fn resolve_host(
    host: &str,
    port: u16,
    resolver: &DnsSystem,
    error_message: &'static str,
) -> Result<SocketAddr, Error> {
    let resolved = resolver.resolve_real(host).await.map_err(|error| {
        tracing::warn!(domain = host, error = %error, "real DNS resolution failed");
        Error::Io(error_message)
    })?;
    let ip = resolved
        .into_iter()
        .next()
        .ok_or(Error::Io("target resolved to no addresses"))?;

    Ok(socket_addr_from_ip(ip, port))
}
