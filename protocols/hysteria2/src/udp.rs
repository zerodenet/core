// Hysteria2 UDP datagram — udp.rs

use alloc::borrow::ToOwned;
#[cfg(feature = "tokio")]
use alloc::collections::BTreeMap;
use alloc::string::String;
#[cfg(feature = "tokio")]
use alloc::vec;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, Ordering};

use zero_core::{
    Address, DatagramUdpResponder, Error, InboundDatagramUdpRelay, InboundUdpDispatch,
    ProtocolType, SessionAuth, UdpFlowPacket,
};
use zero_traits::DatagramCodec;

#[cfg(feature = "tokio")]
use alloc::sync::Arc;
#[cfg(feature = "tokio")]
use tokio::sync::{broadcast, mpsc};
#[cfg(all(feature = "tokio", feature = "crypto"))]
use zero_traits::AsyncSocket;

/// One plaintext UDP payload to encode into a Hysteria2 UDP datagram.
#[derive(Debug, Clone, Copy)]
pub struct Hysteria2UdpPacketTarget<'a> {
    pub session_id: u32,
    pub packet_id: u16,
    pub target: &'a Address,
    pub port: u16,
    pub payload: &'a [u8],
}

/// Parsed Hysteria2 UDP datagram.
#[derive(Debug, Clone)]
pub struct Hysteria2UdpPacket {
    session_id: u32,
    packet_id: u16,
    fragment_id: u8,
    fragment_count: u8,
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl Hysteria2UdpPacket {
    pub fn new(
        session_id: u32,
        packet_id: u16,
        target: Address,
        port: u16,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            session_id,
            packet_id,
            fragment_id: 0,
            fragment_count: 1,
            target,
            port,
            payload,
        }
    }

    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    pub fn packet_id(&self) -> u16 {
        self.packet_id
    }

    pub fn fragment_id(&self) -> u8 {
        self.fragment_id
    }

    pub fn fragment_count(&self) -> u8 {
        self.fragment_count
    }

    fn new_fragment(
        session_id: u32,
        packet_id: u16,
        fragment_id: u8,
        fragment_count: u8,
        target: Address,
        port: u16,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            session_id,
            packet_id,
            fragment_id,
            fragment_count,
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

    pub fn into_parts(self) -> (u32, u16, Address, u16, Vec<u8>) {
        (
            self.session_id,
            self.packet_id,
            self.target,
            self.port,
            self.payload,
        )
    }

    pub fn into_datagram_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

/// Protocol-owned decoded inbound UDP request.
pub struct Hysteria2InboundUdpRequest {
    session_id: u32,
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2InboundUdpDispatchParts {
    request_session_id: u32,
    target: Address,
    port: u16,
    payload: Vec<u8>,
    client_session_id: Option<u64>,
}

impl Hysteria2InboundUdpDispatchParts {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("hysteria2")
    }

    pub fn pipe_parts(&self) -> (&Address, u16, &[u8], Option<u64>) {
        (
            &self.target,
            self.port,
            &self.payload,
            self.client_session_id,
        )
    }

    pub fn into_pipe_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        (self.target, self.port, self.payload, self.client_session_id)
    }

    pub fn into_tracked_inbound_dispatch(self) -> Hysteria2InboundUdpTrackedDispatch {
        let request_session_id = self.request_session_id;
        Hysteria2InboundUdpTrackedDispatch {
            request_session_id,
            dispatch: InboundUdpDispatch::new(
                ProtocolType::new("hysteria2"),
                self.target,
                self.port,
                self.payload,
                self.client_session_id,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2InboundUdpTrackedDispatch {
    request_session_id: u32,
    dispatch: InboundUdpDispatch,
}

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2InboundUdpClientResponse<'a> {
    target: &'a Address,
    port: u16,
    payload: &'a [u8],
}

impl<'a> Hysteria2InboundUdpClientResponse<'a> {
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

impl Hysteria2InboundUdpTrackedDispatch {
    pub fn dispatch(&self) -> &InboundUdpDispatch {
        &self.dispatch
    }
}

impl Hysteria2InboundUdpRequest {
    fn from_packet(packet: Hysteria2UdpPacket) -> Self {
        let (session_id, _, target, port, payload) = packet.into_parts();
        Self {
            session_id,
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

    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }

    pub fn into_dispatch_parts(self) -> Hysteria2InboundUdpDispatchParts {
        let session_id = self.session_id;
        let (target, port, payload) = self.into_parts();
        Hysteria2InboundUdpDispatchParts {
            request_session_id: session_id,
            target,
            port,
            payload,
            client_session_id: None,
        }
    }
}

/// Stateful inbound UDP bridge for Hysteria2 datagram sessions.
#[cfg(feature = "tokio")]
pub struct Hysteria2InboundUdpSession {
    h2_sessions_by_proxy_session: BTreeMap<u64, u32>,
    reassembler: Hysteria2UdpReassembler,
}

#[cfg(feature = "tokio")]
pub struct Hysteria2InboundUdpResponder {
    session: Hysteria2InboundUdpSession,
    pending_dispatch: Option<Hysteria2InboundUdpTrackedDispatch>,
}

#[cfg(feature = "tokio")]
pub struct Hysteria2InboundUdpRelay {
    responder: Hysteria2InboundUdpResponder,
    auth: Option<SessionAuth>,
}

#[cfg(feature = "tokio")]
impl Hysteria2InboundUdpSession {
    pub fn new() -> Self {
        Self {
            h2_sessions_by_proxy_session: BTreeMap::new(),
            reassembler: Hysteria2UdpReassembler::default(),
        }
    }

    pub fn decode_request(
        &mut self,
        data: &[u8],
    ) -> Result<Option<Hysteria2InboundUdpRequest>, Error> {
        let packet = Hysteria2InboundUdpCodec.decode_datagram(data)?;
        self.reassembler
            .push(packet)
            .map(|packet| packet.map(Hysteria2InboundUdpRequest::from_packet))
    }

    pub fn decode_dispatch_parts(
        &mut self,
        data: &[u8],
    ) -> Result<Option<Hysteria2InboundUdpDispatchParts>, Error> {
        self.decode_request(data)
            .map(|request| request.map(Hysteria2InboundUdpRequest::into_dispatch_parts))
    }

    pub async fn read_dispatch_parts_from_datagram(
        &mut self,
        conn: &quinn::Connection,
    ) -> Result<Hysteria2InboundUdpDispatchParts, Error> {
        loop {
            let data = conn
                .read_datagram()
                .await
                .map_err(|_| Error::Io("failed to read Hysteria2 UDP datagram"))?;
            if let Some(dispatch) = self.decode_dispatch_parts(&data)? {
                return Ok(dispatch);
            }
        }
    }

    pub async fn read_inbound_dispatch_from_datagram(
        &mut self,
        conn: &quinn::Connection,
    ) -> Result<Hysteria2InboundUdpTrackedDispatch, Error> {
        self.read_dispatch_parts_from_datagram(conn)
            .await
            .map(Hysteria2InboundUdpDispatchParts::into_tracked_inbound_dispatch)
    }

    fn record_proxy_session(&mut self, proxy_session_id: u64, request_session_id: u32) {
        self.h2_sessions_by_proxy_session
            .insert(proxy_session_id, request_session_id);
    }

    fn record_proxy_session_for_tracked_dispatch(
        &mut self,
        proxy_session_id: u64,
        tracked: &Hysteria2InboundUdpTrackedDispatch,
    ) {
        self.record_proxy_session(proxy_session_id, tracked.request_session_id);
    }

    pub fn record_dispatch_success(
        &mut self,
        proxy_session_id: u64,
        tracked: &Hysteria2InboundUdpTrackedDispatch,
    ) {
        self.record_proxy_session_for_tracked_dispatch(proxy_session_id, tracked);
    }

    pub fn send_response(
        &self,
        conn: &quinn::Connection,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        let Some(&h2_session_id) = self.h2_sessions_by_proxy_session.get(&proxy_session_id) else {
            return Ok(None);
        };
        Hysteria2InboundUdpCodec
            .send_datagram(conn, h2_session_id, target, port, payload)
            .map(Some)
    }

    pub fn send_response_for_proxy_session(
        &self,
        conn: &quinn::Connection,
        proxy_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        let Some(proxy_session_id) = proxy_session_id else {
            return Ok(None);
        };
        self.send_response(conn, proxy_session_id, target, port, payload)
    }

    pub fn send_client_response_for_proxy_session(
        &self,
        conn: &quinn::Connection,
        proxy_session_id: Option<u64>,
        response: Hysteria2InboundUdpClientResponse<'_>,
    ) -> Result<Option<usize>, Error> {
        self.send_response_for_proxy_session(
            conn,
            proxy_session_id,
            response.target(),
            response.port(),
            response.payload(),
        )
    }

    pub fn send_client_response_for_target_proxy_session(
        &self,
        conn: &quinn::Connection,
        proxy_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        self.send_client_response_for_proxy_session(
            conn,
            proxy_session_id,
            Hysteria2InboundUdpClientResponse::new(target, port, payload),
        )
    }
}

#[cfg(feature = "tokio")]
impl Hysteria2InboundUdpResponder {
    pub fn new(session: Hysteria2InboundUdpSession) -> Self {
        Self {
            session,
            pending_dispatch: None,
        }
    }

    pub async fn read_tracked_inbound_dispatch_from_datagram(
        &mut self,
        conn: &quinn::Connection,
    ) -> Result<Hysteria2InboundUdpTrackedDispatch, Error> {
        let tracked = self
            .session
            .read_inbound_dispatch_from_datagram(conn)
            .await?;
        self.pending_dispatch = Some(tracked.clone());
        Ok(tracked)
    }

    pub async fn read_inbound_dispatch_from_datagram(
        &mut self,
        conn: &quinn::Connection,
    ) -> Result<InboundUdpDispatch, Error> {
        self.read_tracked_inbound_dispatch_from_datagram(conn)
            .await
            .map(|tracked| tracked.dispatch().clone())
    }

    pub fn record_dispatch_success(
        &mut self,
        proxy_session_id: u64,
        tracked: &Hysteria2InboundUdpTrackedDispatch,
    ) {
        self.session
            .record_dispatch_success(proxy_session_id, tracked);
    }

    pub fn record_pending_dispatch_success(&mut self, proxy_session_id: u64) {
        if let Some(tracked) = self.pending_dispatch.take() {
            self.record_dispatch_success(proxy_session_id, &tracked);
        }
    }

    pub fn send_response_for_target_proxy_session(
        &self,
        conn: &quinn::Connection,
        proxy_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        self.session.send_client_response_for_target_proxy_session(
            conn,
            proxy_session_id,
            target,
            port,
            payload,
        )
    }
}

#[cfg(feature = "tokio")]
impl Hysteria2InboundUdpRelay {
    pub fn new(responder: Hysteria2InboundUdpResponder) -> Self {
        Self {
            responder,
            auth: None,
        }
    }

    pub fn with_auth(responder: Hysteria2InboundUdpResponder, auth: SessionAuth) -> Self {
        Self {
            responder,
            auth: Some(auth),
        }
    }

    fn into_parts(self) -> (Hysteria2InboundUdpResponder, Option<SessionAuth>) {
        (self.responder, self.auth)
    }
}

#[cfg(feature = "tokio")]
impl InboundDatagramUdpRelay<Arc<quinn::Connection>> for Hysteria2InboundUdpRelay {
    type Responder = Hysteria2InboundUdpResponder;

    fn into_datagram_udp_parts(self) -> (Self::Responder, Option<SessionAuth>) {
        self.into_parts()
    }
}

#[cfg(feature = "tokio")]
#[async_trait::async_trait]
impl DatagramUdpResponder<Arc<quinn::Connection>> for Hysteria2InboundUdpResponder {
    async fn read_inbound_dispatch(
        &mut self,
        conn: &Arc<quinn::Connection>,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        self.read_inbound_dispatch_from_datagram(conn)
            .await
            .map(Some)
    }

    fn on_dispatch_success(&mut self, session_id: u64, _dispatch: &InboundUdpDispatch) {
        self.record_pending_dispatch_success(session_id);
    }

    async fn write_response_for_session(
        &mut self,
        conn: &Arc<quinn::Connection>,
        session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        self.send_response_for_target_proxy_session(conn, session_id, target, port, payload)
    }
}

#[cfg(feature = "tokio")]
impl Default for Hysteria2InboundUdpSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a standard unfragmented Hysteria2 UDPMessage.
/// Format: [session_id:4][packet_id:2][fragment_id:1][fragment_count:1]
///         [varint address length][host:port][payload].
pub(crate) fn build_udp_datagram(
    session_id: u32,
    packet_id: u16,
    address: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_fragment(session_id, packet_id, 0, 1, address, port, payload)
}

fn build_udp_fragment(
    session_id: u32,
    packet_id: u16,
    fragment_id: u8,
    fragment_count: u8,
    address: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    if fragment_count == 0 || fragment_id >= fragment_count {
        return Err(Error::Protocol("hysteria2: invalid UDP fragment index"));
    }
    let authority = crate::shared::encode_authority(address, port)?;
    let mut buf = Vec::with_capacity(16 + authority.len() + payload.len());
    buf.extend_from_slice(&session_id.to_be_bytes());
    buf.extend_from_slice(&packet_id.to_be_bytes());
    buf.push(fragment_id);
    buf.push(fragment_count);
    crate::shared::encode_varint(authority.len() as u64, &mut buf)?;
    buf.extend_from_slice(authority.as_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

static NEXT_UDP_PACKET_ID: AtomicU32 = AtomicU32::new(1);

fn build_udp_fragments(
    session_id: u32,
    address: &Address,
    port: u16,
    payload: &[u8],
    max_datagram_size: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let packet_id = NEXT_UDP_PACKET_ID.fetch_add(1, Ordering::Relaxed) as u16;
    let header_len = build_udp_fragment(session_id, packet_id, 0, 1, address, port, &[])?.len();
    let fragment_payload_size = max_datagram_size
        .checked_sub(header_len)
        .filter(|size| *size > 0)
        .ok_or(Error::Protocol(
            "hysteria2: QUIC datagram limit is too small",
        ))?;
    let fragment_count = payload.len().max(1).div_ceil(fragment_payload_size);
    if fragment_count > 64 {
        return Err(Error::Protocol(
            "hysteria2: UDP payload requires too many fragments",
        ));
    }
    let fragment_count = fragment_count as u8;
    let mut fragments = Vec::with_capacity(usize::from(fragment_count));
    if payload.is_empty() {
        fragments.push(build_udp_fragment(
            session_id, packet_id, 0, 1, address, port, payload,
        )?);
        return Ok(fragments);
    }
    for (fragment_id, chunk) in payload.chunks(fragment_payload_size).enumerate() {
        fragments.push(build_udp_fragment(
            session_id,
            packet_id,
            fragment_id as u8,
            fragment_count,
            address,
            port,
            chunk,
        )?);
    }
    Ok(fragments)
}

/// Parse a Hysteria2 UDP datagram.
pub(crate) fn parse_udp_datagram(data: &[u8]) -> Result<Hysteria2UdpPacket, Error> {
    if data.len() < 9 {
        return Err(Error::Protocol("hysteria2: truncated UDP datagram"));
    }
    let session_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let packet_id = u16::from_be_bytes([data[4], data[5]]);
    let fragment_id = data[6];
    let fragment_count = data[7];
    if fragment_count == 0 || fragment_id >= fragment_count {
        return Err(Error::Protocol("hysteria2: invalid UDP fragment index"));
    }
    let (address_len, address_len_len) = crate::shared::decode_varint(&data[8..])?;
    let address_start = 8 + address_len_len;
    let address_len = usize::try_from(address_len)
        .map_err(|_| Error::Protocol("hysteria2: UDP address length overflow"))?;
    let address_end = address_start
        .checked_add(address_len)
        .ok_or(Error::Protocol("hysteria2: UDP address length overflow"))?;
    if data.len() < address_end {
        return Err(Error::Protocol("hysteria2: truncated UDP address"));
    }
    let authority = core::str::from_utf8(&data[address_start..address_end])
        .map_err(|_| Error::Protocol("hysteria2: invalid UDP address"))?;
    let (target, port) = crate::shared::parse_authority(authority)?;
    let payload = data[address_end..].to_vec();

    Ok(Hysteria2UdpPacket::new_fragment(
        session_id,
        packet_id,
        fragment_id,
        fragment_count,
        target,
        port,
        payload,
    ))
}

#[cfg(feature = "tokio")]
struct FragmentAssembly {
    target: Address,
    port: u16,
    parts: Vec<Option<Vec<u8>>>,
    received: usize,
}

#[cfg(feature = "tokio")]
#[derive(Default)]
struct Hysteria2UdpReassembler {
    pending: BTreeMap<(u32, u16), FragmentAssembly>,
}

#[cfg(feature = "tokio")]
impl Hysteria2UdpReassembler {
    fn push(&mut self, packet: Hysteria2UdpPacket) -> Result<Option<Hysteria2UdpPacket>, Error> {
        if packet.fragment_count == 1 {
            return Ok(Some(packet));
        }
        if packet.fragment_count > 64 {
            return Err(Error::Protocol(
                "hysteria2: UDP fragment count exceeds limit",
            ));
        }
        let key = (packet.session_id, packet.packet_id);
        if !self.pending.contains_key(&key) && self.pending.len() >= 64 {
            if let Some(oldest) = self.pending.keys().next().copied() {
                self.pending.remove(&oldest);
            }
        }
        let assembly = self.pending.entry(key).or_insert_with(|| FragmentAssembly {
            target: packet.target.clone(),
            port: packet.port,
            parts: vec![None; usize::from(packet.fragment_count)],
            received: 0,
        });
        if assembly.parts.len() != usize::from(packet.fragment_count)
            || assembly.target != packet.target
            || assembly.port != packet.port
        {
            self.pending.remove(&key);
            return Err(Error::Protocol("hysteria2: inconsistent UDP fragments"));
        }
        let index = usize::from(packet.fragment_id);
        if assembly.parts[index].is_none() {
            assembly.parts[index] = Some(packet.payload);
            assembly.received += 1;
        }
        if assembly.received != assembly.parts.len() {
            return Ok(None);
        }
        let assembly = self
            .pending
            .remove(&key)
            .expect("completed Hysteria2 fragment assembly");
        let total_len = assembly
            .parts
            .iter()
            .filter_map(Option::as_ref)
            .map(Vec::len)
            .sum();
        let mut payload = Vec::with_capacity(total_len);
        for part in assembly.parts {
            payload.extend_from_slice(
                part.as_deref()
                    .ok_or(Error::Protocol("hysteria2: missing UDP fragment"))?,
            );
        }
        Ok(Some(Hysteria2UdpPacket::new(
            key.0,
            key.1,
            assembly.target,
            assembly.port,
            payload,
        )))
    }
}

pub(crate) fn encode_udp_flow_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_datagram(0, 0, target, port, payload)
}

pub(crate) fn decode_udp_flow_packet(data: &[u8]) -> Result<Hysteria2UdpPacket, Error> {
    parse_udp_datagram(data)
}

fn decode_inbound_udp_datagram(data: &[u8]) -> Result<Hysteria2UdpPacket, Error> {
    parse_udp_datagram(data)
}

fn encode_inbound_udp_datagram(
    session_id: u32,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_datagram(session_id, 0, target, port, payload)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Hysteria2InboundUdpCodec;

impl Hysteria2InboundUdpCodec {
    pub fn decode_datagram(&self, data: &[u8]) -> Result<Hysteria2UdpPacket, Error> {
        decode_inbound_udp_datagram(data)
    }

    pub fn encode_datagram(
        &self,
        session_id: u32,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_inbound_udp_datagram(session_id, target, port, payload)
    }

    #[cfg(feature = "tokio")]
    pub fn send_datagram(
        &self,
        conn: &quinn::Connection,
        session_id: u32,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        let max_datagram_size = conn
            .max_datagram_size()
            .ok_or(Error::Io("Hysteria2 peer does not support QUIC datagrams"))?;
        let fragments = build_udp_fragments(session_id, target, port, payload, max_datagram_size)?;
        let mut encoded_len = 0;
        for fragment in fragments {
            encoded_len += fragment.len();
            conn.send_datagram(fragment.into())
                .map_err(|_| Error::Io("failed to send Hysteria2 UDP datagram"))?;
        }
        Ok(encoded_len)
    }
}

fn udp_cache_key(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> String {
    let fingerprint = client_fingerprint
        .map(|value| alloc::format!("|fp:{value}"))
        .unwrap_or_default();
    alloc::format!("hysteria2|{tag}|{server}:{port}|{password}{fingerprint}")
}

pub struct Hysteria2UdpFlowConfig<'a> {
    tag: &'a str,
    server: &'a str,
    port: u16,
    password: &'a str,
    client_fingerprint: Option<&'a str>,
}

impl<'a> Hysteria2UdpFlowConfig<'a> {
    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        password: &'a str,
        client_fingerprint: Option<&'a str>,
    ) -> Self {
        Self {
            tag,
            server,
            port,
            password,
            client_fingerprint,
        }
    }

    pub fn cache_key(&self) -> String {
        udp_cache_key(
            self.tag,
            self.server,
            self.port,
            self.password,
            self.client_fingerprint,
        )
    }

    pub fn flow_resume(&self) -> Hysteria2UdpFlowResume {
        Hysteria2UdpFlowResume::new(self.password, self.client_fingerprint)
    }

    pub fn connector_profile(&self) -> Hysteria2UdpConnectorProfile {
        self.flow_resume().connector_profile()
    }

    pub fn packet_path_spec(&self) -> Hysteria2UdpPacketPathSpec {
        Hysteria2UdpPacketPathSpec {
            cache_key: self.cache_key(),
            resume: self.flow_resume(),
        }
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        udp_flow_codec()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpPacketPathSpec {
    cache_key: String,
    resume: Hysteria2UdpFlowResume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpPacketPathCarrierBuild {
    cache_key: String,
    server: String,
    port: u16,
    connector_profile: Hysteria2UdpConnectorProfile,
    codec: Hysteria2DatagramCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpPacketPathCarrierBuildParts {
    server: String,
    port: u16,
    connector_profile: Hysteria2UdpConnectorProfile,
    codec: Hysteria2DatagramCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpPacketPathCarrierDescriptor {
    cache_key: String,
    server: String,
    port: u16,
}

impl Hysteria2UdpPacketPathSpec {
    pub fn carrier_build(&self, server: &str, port: u16) -> Hysteria2UdpPacketPathCarrierBuild {
        Hysteria2UdpPacketPathCarrierBuild {
            cache_key: self.cache_key.clone(),
            server: server.to_owned(),
            port,
            connector_profile: self.resume.connector_profile(),
            codec: Hysteria2DatagramCodec,
        }
    }

    pub fn carrier_descriptor(
        &self,
        server: &str,
        port: u16,
    ) -> Hysteria2UdpPacketPathCarrierDescriptor {
        Hysteria2UdpPacketPathCarrierDescriptor {
            cache_key: self.cache_key.clone(),
            server: server.to_owned(),
            port,
        }
    }
}

impl Hysteria2UdpPacketPathCarrierBuild {
    pub fn into_connection_parts(self) -> Hysteria2UdpPacketPathCarrierBuildParts {
        Hysteria2UdpPacketPathCarrierBuildParts {
            server: self.server,
            port: self.port,
            connector_profile: self.connector_profile,
            codec: self.codec,
        }
    }
}

impl Hysteria2UdpPacketPathCarrierBuildParts {
    pub fn into_parts(
        self,
    ) -> (
        alloc::string::String,
        u16,
        Hysteria2UdpConnectorProfile,
        Hysteria2DatagramCodec,
    ) {
        (self.server, self.port, self.connector_profile, self.codec)
    }

    pub fn into_shared_codec_parts(
        self,
    ) -> (
        alloc::string::String,
        u16,
        Hysteria2UdpConnectorProfile,
        Arc<dyn DatagramCodec<Address, Error = Error>>,
    ) {
        let (server, port, connector_profile, codec) = self.into_parts();
        (server, port, connector_profile, Arc::new(codec))
    }
}

impl Hysteria2UdpPacketPathCarrierDescriptor {
    pub fn into_parts(self) -> (String, String, u16) {
        (self.cache_key, self.server, self.port)
    }
}

pub fn udp_packet_path_spec_from_config(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> Hysteria2UdpPacketPathSpec {
    Hysteria2UdpFlowConfig::new(tag, server, port, password, client_fingerprint).packet_path_spec()
}

pub fn udp_packet_path_carrier_descriptor_from_config(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> Hysteria2UdpPacketPathCarrierDescriptor {
    udp_packet_path_spec_from_config(tag, server, port, password, client_fingerprint)
        .carrier_descriptor(server, port)
}

pub fn udp_packet_path_carrier_build_from_config(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> Hysteria2UdpPacketPathCarrierBuild {
    udp_packet_path_spec_from_config(tag, server, port, password, client_fingerprint)
        .carrier_build(server, port)
}

pub fn udp_flow_resume_from_config(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> Hysteria2UdpFlowResume {
    Hysteria2UdpFlowConfig::new(tag, server, port, password, client_fingerprint).flow_resume()
}

pub fn connector_flow_from_resume(
    resume: &Hysteria2UdpFlowResume,
    server: &str,
    port: u16,
) -> Hysteria2UdpConnectorFlow {
    resume.connector_flow(server, port)
}

/// Codec state for a Hysteria2 UDP datagram chain hop.
///
/// Hysteria2 UDP flow framing has no negotiated per-flow crypto state once the
/// QUIC connection is established, so this codec is stateless.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Hysteria2DatagramCodec;

pub(crate) fn udp_flow_codec() -> impl DatagramCodec<Address, Error = Error> {
    Hysteria2DatagramCodec
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpFlowPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl Hysteria2UdpFlowPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn from_parts(target: &Address, port: u16, payload: &[u8]) -> Self {
        Self::new(target.clone(), port, payload.to_vec())
    }

    pub fn encode_with(&self, resume: &Hysteria2UdpFlowResume) -> Result<Vec<u8>, Error> {
        resume.encode_packet(&self.target, self.port, &self.payload)
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

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

static NEXT_UDP_SESSION_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2UdpFlowIo {
    session_id: u32,
}

impl Hysteria2UdpFlowIo {
    fn new() -> Self {
        let session_id = NEXT_UDP_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            session_id: session_id.max(1),
        }
    }
}

impl Default for Hysteria2UdpFlowIo {
    fn default() -> Self {
        Self::new()
    }
}

impl Hysteria2UdpFlowIo {
    pub fn encode_packet(&self, packet: &UdpFlowPacket) -> Result<Vec<u8>, Error> {
        build_udp_datagram(
            self.session_id,
            0,
            &packet.target,
            packet.port,
            &packet.payload,
        )
    }

    pub fn encode_initial_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        build_udp_datagram(self.session_id, 0, target, port, payload)
    }

    pub fn decode_packet(&self, data: &[u8]) -> Option<UdpFlowPacket> {
        let decoded = decode_udp_flow_packet(data).ok()?;
        let (target, port, payload) = decoded.into_datagram_parts();
        Some(UdpFlowPacket::new(target, port, payload))
    }

    fn encode_fragments(
        &self,
        packet: &UdpFlowPacket,
        max_datagram_size: usize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        build_udp_fragments(
            self.session_id,
            &packet.target,
            packet.port,
            &packet.payload,
            max_datagram_size,
        )
    }
}

#[cfg(feature = "tokio")]
pub type Hysteria2UdpFlowResponse = (Address, u16, Vec<u8>);

#[cfg(feature = "tokio")]
type Hysteria2UdpFlowResponses = broadcast::Sender<Hysteria2UdpFlowResponse>;

#[cfg(feature = "tokio")]
pub type Hysteria2UdpFlowResponseReceiver = broadcast::Receiver<Hysteria2UdpFlowResponse>;

#[cfg(feature = "tokio")]
#[derive(Clone)]
struct Hysteria2UdpFlowSender {
    send_tx: mpsc::Sender<UdpFlowPacket>,
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct Hysteria2InitialUdpFlowPacket {
    packet: UdpFlowPacket,
}

#[cfg(feature = "tokio")]
impl Hysteria2InitialUdpFlowPacket {
    pub fn from_parts(target: &Address, port: u16, payload: &[u8]) -> Self {
        Self {
            packet: UdpFlowPacket::from_parts(target, port, payload),
        }
    }
}

#[cfg(feature = "tokio")]
pub struct Hysteria2UdpFlowHandle {
    sender: Hysteria2UdpFlowSender,
    responses: Hysteria2UdpFlowResponses,
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct Hysteria2UdpFlowSession {
    sender: Hysteria2UdpFlowSender,
    responses: Hysteria2UdpFlowResponses,
}

#[cfg(feature = "tokio")]
impl Hysteria2UdpFlowSession {
    pub fn new(handle: Hysteria2UdpFlowHandle) -> Self {
        Self {
            sender: handle.sender,
            responses: handle.responses,
        }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.sender.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> Hysteria2UdpFlowResponseReceiver {
        self.responses.subscribe()
    }
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct Hysteria2UdpFlowConnection {
    session: Hysteria2UdpFlowSession,
}

#[cfg(feature = "tokio")]
impl Hysteria2UdpFlowConnection {
    pub fn new(session: Hysteria2UdpFlowSession) -> Self {
        Self { session }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.session.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> Hysteria2UdpFlowResponseReceiver {
        self.session.subscribe_responses()
    }
}

#[cfg(feature = "tokio")]
impl Hysteria2UdpFlowSender {
    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        let packet = UdpFlowPacket::from_parts(target, port, payload);
        let packet_len = packet.payload.len();
        self.send_tx
            .send(packet)
            .await
            .map_err(|_| Error::Io("hysteria2 udp flow closed"))?;
        Ok(packet_len)
    }
}

#[cfg(feature = "runtime")]
pub fn spawn_udp_flow(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    initial_packet: Hysteria2InitialUdpFlowPacket,
    flow_io: Hysteria2UdpFlowIo,
) -> Hysteria2UdpFlowHandle {
    let (send_tx, send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel::<Hysteria2UdpFlowResponse>(32);

    spawn_send_task(conn.clone(), initial_packet, flow_io, send_rx);
    spawn_recv_task(conn, flow_io, responses.clone());

    Hysteria2UdpFlowHandle {
        sender: Hysteria2UdpFlowSender { send_tx },
        responses,
    }
}

#[cfg(feature = "runtime")]
pub fn start_udp_flow_with_initial_packet(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    target: &Address,
    port: u16,
    payload: &[u8],
    resume: Hysteria2UdpFlowResume,
) -> Hysteria2UdpFlowConnection {
    let flow_io = resume.flow_io();
    let initial_packet = Hysteria2InitialUdpFlowPacket::from_parts(target, port, payload);
    Hysteria2UdpFlowConnection::new(Hysteria2UdpFlowSession::new(spawn_udp_flow(
        conn,
        initial_packet,
        flow_io,
    )))
}

#[cfg(feature = "runtime")]
fn spawn_send_task(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    initial_packet: Hysteria2InitialUdpFlowPacket,
    flow_io: Hysteria2UdpFlowIo,
    mut send_rx: mpsc::Receiver<UdpFlowPacket>,
) {
    tokio::spawn(async move {
        let Some(max_datagram_size) = conn.connection().max_datagram_size() else {
            return;
        };
        let Ok(fragments) = flow_io.encode_fragments(&initial_packet.packet, max_datagram_size)
        else {
            return;
        };
        for fragment in fragments {
            if conn.connection().send_datagram(fragment.into()).is_err() {
                return;
            }
        }
        while let Some(packet) = send_rx.recv().await {
            let Ok(fragments) = flow_io.encode_fragments(&packet, max_datagram_size) else {
                break;
            };
            for fragment in fragments {
                if conn.connection().send_datagram(fragment.into()).is_err() {
                    return;
                }
            }
        }
    });
}

#[cfg(feature = "runtime")]
fn spawn_recv_task(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    flow_io: Hysteria2UdpFlowIo,
    responses: Hysteria2UdpFlowResponses,
) {
    tokio::spawn(async move {
        let mut reassembler = Hysteria2UdpReassembler::default();
        while let Ok(data) = conn.connection().read_datagram().await {
            let Ok(fragment) = parse_udp_datagram(&data) else {
                continue;
            };
            if fragment.session_id != flow_io.session_id {
                continue;
            }
            let Ok(Some(packet)) = reassembler.push(fragment) else {
                continue;
            };
            let (_, _, target, port, payload) = packet.into_parts();
            if responses.send((target, port, payload)).is_err() {
                break;
            }
        }
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpFlowResume {
    password: String,
    client_fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpConnectorFlow {
    cache_key: String,
    connector_profile: Hysteria2UdpConnectorProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpConnectorFlowParts {
    connector_profile: Hysteria2UdpConnectorProfile,
}

impl Hysteria2UdpConnectorFlow {
    pub fn into_cache_key(self) -> String {
        self.cache_key
    }

    pub fn into_connection_parts(self) -> Hysteria2UdpConnectorFlowParts {
        Hysteria2UdpConnectorFlowParts {
            connector_profile: self.connector_profile,
        }
    }
}

impl Hysteria2UdpConnectorFlowParts {
    pub fn into_profile(self) -> Hysteria2UdpConnectorProfile {
        self.connector_profile
    }
}

impl Hysteria2UdpFlowResume {
    pub fn new(password: &str, client_fingerprint: Option<&str>) -> Self {
        Self {
            password: password.to_owned(),
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
        }
    }

    fn peer_config(&self) -> Hysteria2UdpPeerConfig<'_> {
        Hysteria2UdpPeerConfig {
            password: &self.password,
        }
    }

    fn leaf_cache_key(&self, server: &str, port: u16) -> Hysteria2UdpLeafKey {
        self.peer_config().leaf_cache_key(server, port)
    }

    fn flow_key(&self, server: &str, port: u16) -> Hysteria2UdpFlowKey {
        Hysteria2UdpFlowKey::Leaf(self.leaf_cache_key(server, port))
    }

    fn cache_key(&self, server: &str, port: u16) -> Hysteria2UdpCacheKey {
        Hysteria2UdpCacheKey::from_flow_key(self.flow_key(server, port))
    }

    pub fn flow_cache_key(&self, server: &str, port: u16) -> String {
        alloc::format!(
            "leaf|{server}:{port}|password:{}",
            self.peer_config().password
        )
    }

    pub fn connector_flow(&self, server: &str, port: u16) -> Hysteria2UdpConnectorFlow {
        Hysteria2UdpConnectorFlow {
            cache_key: self.flow_cache_key(server, port),
            connector_profile: self.connector_profile(),
        }
    }

    pub fn connector_profile(&self) -> Hysteria2UdpConnectorProfile {
        Hysteria2UdpConnectorProfile {
            password: self.password.clone(),
            client_fingerprint: self.client_fingerprint.clone(),
        }
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        udp_flow_codec()
    }

    pub fn flow_io(&self) -> Hysteria2UdpFlowIo {
        Hysteria2UdpFlowIo::new()
    }

    pub fn encode_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    pub fn encode_flow_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.encode_packet(target, port, payload)
    }

    pub fn decode_packet(&self, data: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        let decoded = decode_udp_flow_packet(data).ok()?;
        Some(decoded.into_datagram_parts())
    }

    pub fn decode_flow_packet(&self, data: &[u8]) -> Option<Hysteria2UdpFlowPacket> {
        let (target, port, payload) = self.decode_packet(data)?;
        Some(Hysteria2UdpFlowPacket::new(target, port, payload))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Hysteria2UdpFlowKey {
    Leaf(Hysteria2UdpLeafKey),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Hysteria2UdpCacheKey(Hysteria2UdpLeafKey);

impl Hysteria2UdpCacheKey {
    fn from_flow_key(flow_key: Hysteria2UdpFlowKey) -> Self {
        match flow_key {
            Hysteria2UdpFlowKey::Leaf(leaf_key) => Self(leaf_key),
        }
    }
}

pub struct Hysteria2UdpFlowStore<T> {
    entries: alloc::collections::BTreeMap<Hysteria2UdpCacheKey, T>,
}

impl<T> Default for Hysteria2UdpFlowStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Hysteria2UdpFlowStore<T> {
    pub fn new() -> Self {
        Self {
            entries: alloc::collections::BTreeMap::new(),
        }
    }

    pub fn get(&self, resume: &Hysteria2UdpFlowResume, server: &str, port: u16) -> Option<&T> {
        let key = resume.cache_key(server, port);
        self.entries.get(&key)
    }

    pub fn insert(
        &mut self,
        resume: &Hysteria2UdpFlowResume,
        server: &str,
        port: u16,
        value: T,
    ) -> Option<T> {
        let key = resume.cache_key(server, port);
        self.entries.insert(key, value)
    }
}

#[cfg(feature = "tokio")]
#[derive(Default)]
pub struct Hysteria2UdpFlowSessions {
    entries: Hysteria2UdpFlowStore<Hysteria2UdpFlowConnection>,
}

#[cfg(feature = "tokio")]
impl Hysteria2UdpFlowSessions {
    pub fn new() -> Self {
        Self {
            entries: Hysteria2UdpFlowStore::new(),
        }
    }

    pub fn get(
        &self,
        resume: &Hysteria2UdpFlowResume,
        server: &str,
        port: u16,
    ) -> Option<&Hysteria2UdpFlowConnection> {
        self.entries.get(resume, server, port)
    }

    pub fn insert(
        &mut self,
        resume: &Hysteria2UdpFlowResume,
        server: &str,
        port: u16,
        connection: Hysteria2UdpFlowConnection,
    ) -> Option<Hysteria2UdpFlowConnection> {
        self.entries.insert(resume, server, port, connection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2UdpConnectorProfile {
    password: String,
    client_fingerprint: Option<String>,
}

impl Hysteria2UdpConnectorProfile {
    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    pub fn client_fingerprint(&self) -> Option<&str> {
        self.client_fingerprint.as_deref()
    }

    #[cfg(all(feature = "tokio", feature = "crypto"))]
    pub async fn authenticate_connection<S>(
        &self,
        conn: &quinn::Connection,
        stream: &mut S,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let mut salt = [0u8; 32];
        conn.export_keying_material(&mut salt, b"hysteria2 auth", &[])
            .map_err(|_| Error::Io("hysteria2 key export failed"))?;

        crate::Hysteria2Outbound
            .authenticate_with_salt(stream, &self.password, &salt)
            .await
    }
}

#[derive(Debug, Clone, Copy)]
struct Hysteria2UdpPeerConfig<'a> {
    password: &'a str,
}

impl<'a> Hysteria2UdpPeerConfig<'a> {
    fn leaf_cache_key(&self, server: &str, port: u16) -> Hysteria2UdpLeafKey {
        Hysteria2UdpLeafKey {
            server: server.to_owned(),
            port,
            password: self.password.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Hysteria2UdpLeafKey {
    server: String,
    port: u16,
    password: String,
}

impl DatagramCodec<Address> for Hysteria2DatagramCodec {
    type Error = Error;

    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    fn decode(&self, data: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        let decoded = decode_udp_flow_packet(data).ok()?;
        Some(decoded.into_datagram_parts())
    }
}
