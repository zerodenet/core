use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Error, Session};
use zero_dns::DnsSystem;
use zero_platform_tokio::TokioSocket;
use zero_traits::IpAddress;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DirectConnector;

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
    ) -> Result<(TokioSocket, SocketAddr), Error> {
        let addr = self.resolve_target_addr(session, resolver).await?;

        let interface = egress.current_for(addr.is_ipv6());
        TokioSocket::connect_addr_on(addr, interface.as_ref())
            .await
            .map(|socket| (socket, addr))
            .map_err(|_| Error::Io("failed to connect direct target"))
    }

    pub(crate) async fn resolve_target_addr(
        &self,
        session: &Session,
        resolver: &DnsSystem,
    ) -> Result<SocketAddr, Error> {
        self.validate(session)?;

        self.resolve_address(
            &session.target,
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

        let interface = egress.current_for(addr.is_ipv6());
        TokioSocket::connect_addr_on(addr, interface.as_ref())
            .await
            .map_err(|_| Error::Io("failed to connect upstream target"))
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
    let resolved = resolver
        .resolve_real(host)
        .await
        .map_err(|_| Error::Io(error_message))?;
    let ip = resolved
        .into_iter()
        .next()
        .ok_or(Error::Io("target resolved to no addresses"))?;

    Ok(socket_addr_from_ip(ip, port))
}
