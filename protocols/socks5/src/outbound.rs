use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec;

use zero_core::{Address, Error, ProtocolType, Session};
use zero_traits::{AsyncSocket, TcpTunnelProtocol, UdpRelayProtocol};

pub(crate) mod wire;
use crate::shared::{CMD_UDP_ASSOCIATE, SOCKS5_VERSION};
use crate::udp::{Socks5OwnedUdpAssociationConfig, Socks5UdpAssociationTarget};
use wire::{negotiate_auth, read_response};

#[derive(Debug, Default, Clone, Copy)]
pub struct Socks5Outbound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Socks5OutboundAuth<'a> {
    pub username: &'a str,
    pub password: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5OwnedOutboundAuth {
    username: alloc::string::String,
    password: alloc::string::String,
}

impl Socks5OwnedOutboundAuth {
    pub fn new(auth: Socks5OutboundAuth<'_>) -> Self {
        Self {
            username: auth.username.to_owned(),
            password: auth.password.to_owned(),
        }
    }

    pub fn as_ref(&self) -> Socks5OutboundAuth<'_> {
        Socks5OutboundAuth {
            username: &self.username,
            password: &self.password,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5UdpFlowResume {
    auth: Option<Socks5OwnedOutboundAuth>,
}

impl Socks5UdpFlowResume {
    pub fn new(auth: Option<Socks5OutboundAuth<'_>>) -> Self {
        Self {
            auth: auth.map(Socks5OwnedOutboundAuth::new),
        }
    }

    pub fn auth(&self) -> Option<Socks5OutboundAuth<'_>> {
        self.auth.as_ref().map(Socks5OwnedOutboundAuth::as_ref)
    }

    fn owned_association_config(&self) -> Socks5OwnedUdpAssociationConfig {
        Socks5OwnedUdpAssociationConfig::new(self.auth.clone())
    }

    pub fn association_target(
        &self,
        outbound_tag: impl Into<alloc::string::String>,
        server: impl Into<alloc::string::String>,
        port: u16,
    ) -> Socks5UdpAssociationTarget {
        Socks5UdpAssociationTarget::new(outbound_tag, server, port, self.owned_association_config())
    }
}

#[derive(Debug, Clone, Copy)]
struct Socks5TcpTunnelTarget<'a> {
    pub session: &'a Session,
    pub auth: Option<Socks5OutboundAuth<'a>>,
}

impl<'a> Socks5TcpTunnelTarget<'a> {
    pub fn new(session: &'a Session, username: Option<&'a str>, password: Option<&'a str>) -> Self {
        Self {
            session,
            auth: outbound_auth(username, password),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5TcpOutboundProfile {
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5TcpConnectSpec {
    server: String,
    port: u16,
    profile: Socks5TcpOutboundProfile,
}

impl Socks5TcpOutboundProfile {
    pub fn from_config_parts(username: Option<&str>, password: Option<&str>) -> Self {
        Self {
            username: username.map(ToOwned::to_owned),
            password: password.map(ToOwned::to_owned),
        }
    }

    pub async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        Socks5Outbound
            .establish_tcp_tunnel(
                stream,
                &Socks5TcpTunnelTarget::new(
                    session,
                    self.username.as_deref(),
                    self.password.as_deref(),
                ),
            )
            .await
    }
}

impl Socks5TcpConnectSpec {
    pub fn from_config_parts(
        server: impl Into<String>,
        port: u16,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Self {
        Self {
            server: server.into(),
            port,
            profile: Socks5TcpOutboundProfile::from_config_parts(username, password),
        }
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.profile.establish_tcp_tunnel(stream, session).await
    }
}

fn outbound_auth<'a>(
    username: Option<&'a str>,
    password: Option<&'a str>,
) -> Option<Socks5OutboundAuth<'a>> {
    username
        .zip(password)
        .map(|(username, password)| Socks5OutboundAuth { username, password })
}

impl Socks5Outbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("socks5")
    }

    pub async fn establish_tunnel<S>(&self, stream: &mut S, session: &Session) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.establish_tunnel_with_auth(stream, session, None).await
    }

    pub async fn establish_tunnel_with_auth<S>(
        &self,
        stream: &mut S,
        session: &Session,
        auth: Option<Socks5OutboundAuth<'_>>,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        if session.port == 0 {
            return Err(Error::Config("target port is required"));
        }

        wire::connect_target(stream, &session.target, session.port, auth).await
    }

    pub async fn establish_udp_association<S>(
        &self,
        stream: &mut S,
    ) -> Result<(Address, u16), Error>
    where
        S: AsyncSocket,
    {
        self.establish_udp_association_with_auth(stream, None).await
    }

    pub async fn establish_udp_association_with_auth<S>(
        &self,
        stream: &mut S,
        auth: Option<Socks5OutboundAuth<'_>>,
    ) -> Result<(Address, u16), Error>
    where
        S: AsyncSocket,
    {
        negotiate_auth(stream, auth).await?;

        let request = vec![
            SOCKS5_VERSION,
            CMD_UDP_ASSOCIATE,
            0x00,
            0x01,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("failed to write SOCKS5 outbound udp associate request"))?;

        read_response(stream).await
    }
}

impl<'a> TcpTunnelProtocol<Socks5TcpTunnelTarget<'a>> for Socks5Outbound {
    type Error = Error;

    async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        target: &Socks5TcpTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        self.establish_tunnel_with_auth(stream, target.session, target.auth)
            .await
    }
}

/// Target parameters for SOCKS5 UDP relay association.
#[derive(Debug, Clone, Copy)]
pub struct Socks5UdpRelayTarget<'a> {
    pub auth: Option<Socks5OutboundAuth<'a>>,
}

impl<'a> UdpRelayProtocol<Socks5UdpRelayTarget<'a>> for Socks5Outbound {
    type Error = Error;
    type RelayEndpoint = (Address, u16);

    async fn establish_udp_relay<S>(
        &self,
        control_stream: &mut S,
        target: &Socks5UdpRelayTarget<'a>,
    ) -> Result<Self::RelayEndpoint, Self::Error>
    where
        S: AsyncSocket,
    {
        self.establish_udp_association_with_auth(control_stream, target.auth)
            .await
    }
}
