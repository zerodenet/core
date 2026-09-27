use core::future::Future;

use alloc::vec::Vec;

use zero_core::{Address, Error};
use zero_traits::{AsyncSocket, DatagramSocket, IpAddress, SocketAddress, UdpRelayProtocol};

use crate::outbound::{Socks5Outbound, Socks5OutboundAuth, Socks5OwnedOutboundAuth};
use crate::udp::Socks5UdpRelayTarget;

use super::packet::{build_udp_packet, decode_udp_associate_response, Socks5UdpPacket};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Socks5UdpRelayEndpoint {
    address: IpAddress,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Socks5UdpAssociationConfig<'a> {
    auth: Option<Socks5OutboundAuth<'a>>,
}

impl<'a> Socks5UdpAssociationConfig<'a> {
    pub(crate) fn new(auth: Option<Socks5OutboundAuth<'a>>) -> Self {
        Self { auth }
    }

    pub(crate) fn auth(&self) -> Option<Socks5OutboundAuth<'a>> {
        self.auth
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Socks5OwnedUdpAssociationConfig {
    auth: Option<Socks5OwnedOutboundAuth>,
}

impl Socks5OwnedUdpAssociationConfig {
    pub(crate) fn new(auth: Option<Socks5OwnedOutboundAuth>) -> Self {
        Self { auth }
    }

    pub(crate) fn as_ref(&self) -> Socks5UdpAssociationConfig<'_> {
        Socks5UdpAssociationConfig::new(self.auth.as_ref().map(|auth| auth.as_ref()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5UdpAssociationTarget {
    outbound_tag: alloc::string::String,
    server: alloc::string::String,
    port: u16,
    config: Socks5OwnedUdpAssociationConfig,
}

impl Socks5UdpAssociationTarget {
    pub(crate) fn new(
        outbound_tag: impl Into<alloc::string::String>,
        server: impl Into<alloc::string::String>,
        port: u16,
        config: Socks5OwnedUdpAssociationConfig,
    ) -> Self {
        Self {
            outbound_tag: outbound_tag.into(),
            server: server.into(),
            port,
            config,
        }
    }

    pub fn outbound_tag(&self) -> &str {
        &self.outbound_tag
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    fn association_config(&self) -> Socks5UdpAssociationConfig<'_> {
        self.config.as_ref()
    }

    pub async fn establish_with_control<S>(
        &self,
        control_stream: &mut S,
    ) -> Result<(Address, u16), Error>
    where
        S: AsyncSocket,
    {
        establish_udp_relay_with_control(control_stream, self.association_config()).await
    }

    pub async fn establish_with_transport<
        C,
        S,
        E,
        OpenControl,
        OpenControlFut,
        ResolveRelay,
        ResolveRelayFut,
        RecordControl,
    >(
        &self,
        open_control: OpenControl,
        resolve_relay: ResolveRelay,
        record_control: RecordControl,
    ) -> Result<Socks5EstablishedUdpAssociation<C, S>, E>
    where
        C: AsyncSocket,
        S: DatagramSocket,
        E: From<Error>,
        OpenControl: FnOnce(&str, u16) -> OpenControlFut,
        OpenControlFut: Future<Output = Result<C, E>>,
        ResolveRelay: FnOnce(Address, u16) -> ResolveRelayFut,
        ResolveRelayFut: Future<Output = Result<(SocketAddress, S), E>>,
        RecordControl: FnOnce(&mut C),
    {
        let mut control = open_control(self.server(), self.port()).await?;
        let (relay_address, relay_port) = self
            .establish_with_control(&mut control)
            .await
            .map_err(E::from)?;
        record_control(&mut control);
        let (relay_endpoint, relay_socket) = resolve_relay(relay_address, relay_port).await?;
        Ok(Socks5EstablishedUdpAssociation::from_relay_socket_address(
            control,
            relay_socket,
            relay_endpoint,
        ))
    }

    pub fn log_parts(&self) -> (&str, &str, u16) {
        (&self.outbound_tag, &self.server, self.port)
    }

    pub fn into_log_parts(self) -> (alloc::string::String, alloc::string::String, u16) {
        (self.outbound_tag, self.server, self.port)
    }

    pub fn matches(&self, outbound_tag: &str, server: &str, port: u16) -> bool {
        self.outbound_tag == outbound_tag && self.server == server && self.port == port
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5UdpRelayError<E> {
    Socket(E),
    Protocol(Error),
}

#[derive(Debug)]
struct Socks5UdpRelay<S> {
    socket: S,
    endpoint: Socks5UdpRelayEndpoint,
}

#[derive(Debug)]
struct Socks5UdpAssociation<C, S> {
    _control: C,
    relay: Socks5UdpRelay<S>,
}

#[derive(Debug)]
pub struct Socks5EstablishedUdpAssociation<C, S> {
    association: Socks5UdpAssociation<C, S>,
}

impl<C, S> Socks5UdpAssociation<C, S> {
    fn new(control: C, relay: Socks5UdpRelay<S>) -> Self {
        Self {
            _control: control,
            relay,
        }
    }

    fn from_relay_socket(control: C, socket: S, endpoint: Socks5UdpRelayEndpoint) -> Self {
        Self::new(control, Socks5UdpRelay::new(socket, endpoint))
    }

    fn from_relay_endpoint(control: C, socket: S, address: IpAddress, port: u16) -> Self {
        Self::from_relay_socket(control, socket, Socks5UdpRelayEndpoint { address, port })
    }
}

impl<C, S> Socks5EstablishedUdpAssociation<C, S> {
    fn new(association: Socks5UdpAssociation<C, S>) -> Self {
        Self { association }
    }

    fn from_relay_endpoint(control: C, socket: S, address: IpAddress, port: u16) -> Self {
        Self::new(Socks5UdpAssociation::from_relay_endpoint(
            control, socket, address, port,
        ))
    }

    pub fn from_relay_socket_address(control: C, socket: S, endpoint: SocketAddress) -> Self {
        Self::from_relay_endpoint(control, socket, endpoint.ip, endpoint.port)
    }

    pub fn control(&self) -> &C {
        &self.association._control
    }
}

impl<C, S> Socks5EstablishedUdpAssociation<C, S>
where
    S: DatagramSocket,
{
    pub async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.association.send_packet(target, port, payload).await
    }

    pub async fn recv_packet(
        &self,
        buf: &mut [u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.association.recv_packet(buf).await
    }

    pub async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), Socks5UdpRelayError<S::Error>> {
        self.association.recv_response_parts(buf).await
    }

    pub async fn recv_payload(
        &self,
        buf: &mut [u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.association.recv_payload(buf).await
    }
}

impl<S> Socks5UdpRelay<S> {
    fn new(socket: S, endpoint: Socks5UdpRelayEndpoint) -> Self {
        Self { socket, endpoint }
    }
}

impl<C, S> Socks5UdpAssociation<C, S>
where
    S: DatagramSocket,
{
    async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.relay.send_packet(target, port, payload).await
    }

    async fn recv_packet(&self, buf: &mut [u8]) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.relay.recv_packet(buf).await
    }

    async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), Socks5UdpRelayError<S::Error>> {
        self.relay.recv_response_parts(buf).await
    }

    async fn recv_payload(&self, buf: &mut [u8]) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        self.relay.recv_payload(buf).await
    }
}

async fn establish_udp_relay_with_control<S>(
    control_stream: &mut S,
    config: Socks5UdpAssociationConfig<'_>,
) -> Result<(Address, u16), Error>
where
    S: AsyncSocket,
{
    let (address, port) = Socks5Outbound
        .establish_udp_relay(
            control_stream,
            &Socks5UdpRelayTarget {
                auth: config.auth(),
            },
        )
        .await?;
    Ok((address, port))
}

impl<S> Socks5UdpRelay<S>
where
    S: DatagramSocket,
{
    async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        let packet =
            build_udp_packet(target, port, payload).map_err(Socks5UdpRelayError::Protocol)?;
        self.socket
            .send_to(&packet, self.endpoint.address, self.endpoint.port)
            .await
            .map_err(Socks5UdpRelayError::Socket)?;

        Ok(packet.len())
    }

    async fn recv_packet(&self, buf: &mut [u8]) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        let (read, address, port) = self
            .socket
            .recv_from(buf)
            .await
            .map_err(Socks5UdpRelayError::Socket)?;

        if address != self.endpoint.address || port != self.endpoint.port {
            return Err(Socks5UdpRelayError::Protocol(Error::Protocol(
                "unexpected UDP sender from SOCKS5 upstream",
            )));
        }

        Ok(read)
    }

    async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), Socks5UdpRelayError<S::Error>> {
        let read = self.recv_packet(buf).await?;
        decode_udp_associate_response(&buf[..read])
            .map(Socks5UdpPacket::into_parts)
            .map_err(Socks5UdpRelayError::Protocol)
    }

    async fn recv_payload(&self, buf: &mut [u8]) -> Result<usize, Socks5UdpRelayError<S::Error>> {
        let (_, _, payload) = self.recv_response_parts(buf).await?;
        let payload_len = payload.len();
        buf[..payload_len].copy_from_slice(&payload);
        Ok(payload_len)
    }
}

impl<E> From<Error> for Socks5UdpRelayError<E> {
    fn from(error: Error) -> Self {
        Self::Protocol(error)
    }
}

impl<E> Socks5UdpRelayError<E> {
    pub fn into_mapped<M, F>(self, map_socket: F) -> M
    where
        M: From<Error>,
        F: FnOnce(E) -> M,
    {
        match self {
            Self::Socket(error) => map_socket(error),
            Self::Protocol(error) => M::from(error),
        }
    }
}
