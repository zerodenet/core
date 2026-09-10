use alloc::vec::Vec;
#[cfg(feature = "crypto")]
use std::collections::HashMap;
#[cfg(feature = "crypto")]
use std::net::SocketAddr;

use zero_core::{Address, InboundUdpDispatch, ProtocolType};
#[cfg(feature = "crypto")]
use zero_core::{Error, StreamUdpResponder};
#[cfg(feature = "crypto")]
use zero_traits::IpAddress;

#[cfg(feature = "crypto")]
use super::packet::{decode_udp_flow_packet, encode_udp_flow_packet, MieruInboundUdpPacket};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruInboundUdpRequest {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruInboundUdpDispatchParts {
    target: Address,
    port: u16,
    payload: Vec<u8>,
    client_session_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct MieruInboundUdpClientResponse<'a> {
    target: &'a Address,
    port: u16,
    payload: &'a [u8],
}

impl<'a> MieruInboundUdpClientResponse<'a> {
    pub fn new(target: &'a Address, port: u16, payload: &'a [u8]) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    fn target(&self) -> &'a Address {
        self.target
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

impl MieruInboundUdpDispatchParts {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("mieru")
    }

    pub fn pipe_parts(&self) -> (&Address, u16, &[u8], Option<u64>) {
        (
            &self.target,
            self.port,
            &self.payload,
            self.client_session_id,
        )
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        (self.target, self.port, self.payload, self.client_session_id)
    }

    pub fn into_pipe_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        self.into_parts()
    }

    pub fn into_inbound_dispatch(self) -> InboundUdpDispatch {
        InboundUdpDispatch::new(
            ProtocolType::new("mieru"),
            self.target,
            self.port,
            self.payload,
            self.client_session_id,
        )
    }
}

impl MieruInboundUdpRequest {
    #[cfg(feature = "crypto")]
    fn from_packet(packet: MieruInboundUdpPacket) -> Self {
        let (target, port, payload) = packet.into_parts();
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn target_endpoint(&self) -> (&Address, u16) {
        (&self.target, self.port)
    }

    pub fn into_dispatch_parts(self) -> MieruInboundUdpDispatchParts {
        MieruInboundUdpDispatchParts {
            target: self.target,
            port: self.port,
            payload: self.payload,
            client_session_id: None,
        }
    }

    #[cfg(feature = "crypto")]
    pub fn target_socket_addr(&self) -> Option<SocketAddr> {
        socket_addr_from_target(&self.target, self.port)
    }

    pub fn target_domain(&self) -> Option<(&str, u16)> {
        match &self.target {
            Address::Domain(domain) => Some((domain.as_str(), self.port)),
            _ => None,
        }
    }

    #[cfg(feature = "crypto")]
    pub fn resolved_target_socket_addr(&self, ip: IpAddress) -> SocketAddr {
        socket_addr_from_ip(ip, self.port)
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    #[cfg(feature = "crypto")]
    fn target_for_response(&self) -> (Address, u16) {
        (self.target.clone(), self.port)
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Default)]
pub struct MieruInboundUdpSession {
    targets_by_sender: HashMap<SocketAddr, (Address, u16)>,
    read_buf: tokio::sync::Mutex<Vec<u8>>,
}

#[cfg(feature = "crypto")]
#[derive(Debug)]
pub struct MieruInboundUdpResponder {
    session: MieruInboundUdpSession,
}

#[cfg(feature = "crypto")]
impl MieruInboundUdpSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn decode_request(&self, data: &[u8]) -> Result<MieruInboundUdpRequest, Error> {
        decode_udp_flow_packet(data).map(MieruInboundUdpRequest::from_packet)
    }

    pub fn decode_dispatch_parts(
        &self,
        data: &[u8],
    ) -> Result<MieruInboundUdpDispatchParts, Error> {
        self.decode_request(data)
            .map(MieruInboundUdpRequest::into_dispatch_parts)
    }

    /// Read one framed packet, retaining partial bytes if the future is cancelled.
    /// A session belongs to one logical input stream. The scratch buffer argument
    /// is retained for API compatibility; framing storage is owned by the session.
    pub async fn read_dispatch_parts_tokio<R>(
        &self,
        reader: &mut R,
        _buf: &mut [u8],
    ) -> Result<Option<MieruInboundUdpDispatchParts>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut pending = self.read_buf.lock().await;
        super::framing::read_packet(reader, &mut pending)
            .await?
            .map(|packet| self.decode_dispatch_parts(&packet))
            .transpose()
    }

    pub async fn read_inbound_dispatch_tokio<R>(
        &self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> Result<Option<InboundUdpDispatch>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.read_dispatch_parts_tokio(reader, buf)
            .await
            .map(|parts| parts.map(MieruInboundUdpDispatchParts::into_inbound_dispatch))
    }

    pub fn record_target(&mut self, sender: SocketAddr, target: Address, port: u16) {
        self.targets_by_sender.insert(sender, (target, port));
    }

    pub fn record_request_target(&mut self, sender: SocketAddr, request: &MieruInboundUdpRequest) {
        let (target, port) = request.target_for_response();
        self.record_target(sender, target, port);
    }

    pub async fn write_response_tokio<W>(
        &self,
        writer: &mut W,
        sender: SocketAddr,
        payload: &[u8],
    ) -> Result<Option<usize>, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let Some((target, port)) = self.targets_by_sender.get(&sender) else {
            return Ok(None);
        };
        write_response_tokio(writer, target, *port, payload)
            .await
            .map(Some)
    }

    pub async fn write_response_for_target_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        write_response_tokio(writer, target, port, payload).await
    }

    pub async fn write_client_response_tokio<W>(
        &self,
        writer: &mut W,
        response: MieruInboundUdpClientResponse<'_>,
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.write_response_for_target_tokio(
            writer,
            response.target(),
            response.port(),
            response.payload(),
        )
        .await
    }

    pub async fn write_client_response_for_target_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.write_client_response_tokio(
            writer,
            MieruInboundUdpClientResponse::new(target, port, payload),
        )
        .await
    }
}

#[cfg(feature = "crypto")]
impl MieruInboundUdpResponder {
    pub fn new(session: MieruInboundUdpSession) -> Self {
        Self { session }
    }

    pub async fn read_inbound_dispatch_tokio<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<InboundUdpDispatch>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.session
            .read_inbound_dispatch_tokio(reader, &mut [])
            .await
    }

    pub async fn write_response_for_target_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.session
            .write_client_response_for_target_tokio(writer, target, port, payload)
            .await
    }
}

#[cfg(feature = "crypto")]
#[async_trait::async_trait]
impl<S> StreamUdpResponder<S> for MieruInboundUdpResponder
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    async fn read_inbound_dispatch(
        &mut self,
        client: &mut S,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        self.read_inbound_dispatch_tokio(client).await
    }

    async fn write_response_for_target(
        &mut self,
        client: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.write_response_for_target_tokio(client, target, port, payload)
            .await
    }
}

#[cfg(feature = "crypto")]
impl Default for MieruInboundUdpResponder {
    fn default() -> Self {
        Self::new(MieruInboundUdpSession::default())
    }
}

#[cfg(feature = "crypto")]
async fn write_response_tokio<W>(
    writer: &mut W,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<usize, Error>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let frame = encode_udp_flow_packet(target, port, payload)?;
    let len = frame.len();
    tokio::io::AsyncWriteExt::write_all(writer, &frame)
        .await
        .map_err(|_| Error::Io("failed to write Mieru UDP response"))?;
    tokio::io::AsyncWriteExt::flush(writer)
        .await
        .map_err(|_| Error::Io("failed to flush Mieru UDP response"))?;
    Ok(len)
}

#[cfg(feature = "crypto")]
fn socket_addr_from_target(target: &Address, port: u16) -> Option<SocketAddr> {
    match target {
        Address::Ipv4(ip) => Some(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::from(*ip)),
            port,
        )),
        Address::Ipv6(ip) => Some(SocketAddr::new(
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(*ip)),
            port,
        )),
        Address::Domain(_) => None,
    }
}

#[cfg(feature = "crypto")]
fn socket_addr_from_ip(ip: IpAddress, port: u16) -> SocketAddr {
    match ip {
        IpAddress::V4(octets) => {
            SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)), port)
        }
        IpAddress::V6(octets) => {
            SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)), port)
        }
    }
}
