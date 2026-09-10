// Mieru protocol outbound handler — outbound.rs

use alloc::vec::Vec;
use std::io;
use std::pin::Pin;
use std::string::String;
use std::task::{ready, Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{broadcast, mpsc};
use zero_core::{Address, Error, Session};
use zero_traits::AsyncSocket;

use crate::crypto::{derive_key, MieruCipher};
use crate::metadata::{
    DataMetadata, SessionMetadata, CLOSE_SESSION_REQUEST, DATA_CLIENT_TO_SERVER, METADATA_LEN,
    OPEN_SESSION_REQUEST, OPEN_SESSION_RESPONSE,
};
use crate::segment::{
    build_data_segment, build_session_segment_with_padding, parse_segment, Segment, MAX_FRAGMENT,
};
use crate::session::MieruSession;
use crate::traffic_pattern::{
    data_padding_lengths, session_padding, write_socket_with_fragmentation, FragmentedWriteState,
    TcpFragmentPattern, TrafficPattern,
};
use crate::udp;

/// Mieru outbound connection.
pub struct MieruOutbound {
    pub mieru_session: MieruSession,
    pub client_cipher: MieruCipher,
    pub server_cipher: MieruCipher,
    pub c2s_nonce_sent: bool,
    pub s2c_nonce_recv: bool,
    pub tcp_fragment: TcpFragmentPattern,
}

/// Target parameters for a Mieru TCP tunnel.
///
/// The outer Mieru session is established with credentials, then the final
/// proxy target is conveyed by a SOCKS5 CONNECT request inside the encrypted
/// session.
#[derive(Debug, Clone, Copy)]
pub struct MieruTcpTunnelTarget<'a> {
    pub username: &'a str,
    pub password: &'a str,
    pub target: &'a Address,
    pub port: u16,
}

impl<'a> MieruTcpTunnelTarget<'a> {
    pub fn new(session: &'a Session, username: &'a str, password: &'a str) -> Self {
        Self {
            username,
            password,
            target: &session.target,
            port: session.port,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruTcpOutboundProfile {
    username: String,
    password: String,
}

impl MieruTcpOutboundProfile {
    pub fn from_config_parts(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    pub async fn establish_tcp_tunnel<S>(
        &self,
        stream: S,
        session: &Session,
    ) -> Result<MieruTcpStream<S>, Error>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
    {
        establish_tcp_tunnel(
            stream,
            &MieruTcpTunnelTarget::new(session, &self.username, &self.password),
        )
        .await
    }
}

pub fn tcp_outbound_profile_from_config(
    username: impl Into<String>,
    password: impl Into<String>,
) -> MieruTcpOutboundProfile {
    MieruTcpOutboundProfile::from_config_parts(username, password)
}

pub struct MieruTcpStream<S> {
    inner: S,
    outbound: MieruOutbound,
    write_state: FragmentedWriteState,
    raw_read_buf: Vec<u8>,
    read_buf: Vec<u8>,
    read_pos: usize,
}

impl<S> MieruTcpStream<S> {
    pub fn new(inner: S, outbound: MieruOutbound) -> Self {
        let write_state = FragmentedWriteState::new(outbound.tcp_fragment);
        Self {
            inner,
            outbound,
            write_state,
            raw_read_buf: Vec::new(),
            read_buf: Vec::new(),
            read_pos: 0,
        }
    }

    fn poll_write_encrypted(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if !self.write_state.is_idle() {
            ready!(self
                .write_state
                .poll_ready_for_write(Pin::new(&mut self.inner), cx))?;
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let plain = &buf[..buf.len().min(MAX_FRAGMENT)];
        let wire = self
            .outbound
            .encrypt_client_data(plain)
            .map_err(|e| io::Error::other(format!("mieru encrypt: {e}")))?;
        self.write_state.start(wire, plain.len());
        Poll::Ready(Ok(plain.len()))
    }

    fn poll_read_decrypted(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if !self.write_state.is_idle() {
            match self.write_state.poll_drain(Pin::new(&mut self.inner), cx) {
                Poll::Ready(Ok(())) => {
                    self.write_state.finish();
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }

        if self.read_pos < self.read_buf.len() {
            let remaining = &self.read_buf[self.read_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.read_pos += n;
            if self.read_pos >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        loop {
            match self
                .outbound
                .decrypt_server_data_with_consumed(&self.raw_read_buf)
            {
                Ok((segment, consumed)) => {
                    self.raw_read_buf.drain(..consumed);
                    let payload = segment.payload;
                    if payload.is_empty() {
                        continue;
                    }
                    let n = payload.len().min(buf.remaining());
                    buf.put_slice(&payload[..n]);
                    if n < payload.len() {
                        self.read_buf = payload[n..].to_vec();
                        self.read_pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                Err(Error::Protocol("mieru: need more data")) => {
                    let mut scratch = [0u8; 4096];
                    let mut read_buf = ReadBuf::new(&mut scratch);
                    match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
                        Poll::Ready(Ok(())) => {
                            let filled = read_buf.filled();
                            if filled.is_empty() {
                                return Poll::Ready(Ok(()));
                            }
                            self.raw_read_buf.extend_from_slice(filled);
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(e) => return Poll::Ready(Err(io::Error::other(format!("mieru decrypt: {e}")))),
            }
        }
    }
}

impl<S> AsyncRead for MieruTcpStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::into_inner(self).poll_read_decrypted(cx, buf)
    }
}

impl<S> AsyncWrite for MieruTcpStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::into_inner(self).poll_write_encrypted(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        this.write_state.poll_flush(Pin::new(&mut this.inner), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        this.write_state
            .poll_shutdown(Pin::new(&mut this.inner), cx)
    }
}

pub async fn establish_tcp_tunnel<S>(
    mut stream: S,
    target: &MieruTcpTunnelTarget<'_>,
) -> Result<MieruTcpStream<S>, Error>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
{
    let outbound = MieruOutbound::connect(&mut stream, target.username, target.password).await?;
    let mut mieru_stream = MieruTcpStream::new(stream, outbound);
    super::tunnel::request_tcp_connect(&mut mieru_stream, target.target, target.port).await?;
    Ok(mieru_stream)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruUdpFlowPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl MieruUdpFlowPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn encode_with(&self, flow_io: &mut MieruUdpFlowIo) -> Result<Vec<u8>, Error> {
        flow_io.encrypt_packet(&self.target, self.port, &self.payload)
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

pub struct MieruUdpFlowIo {
    outbound: MieruOutbound,
    recv_raw: Vec<u8>,
}

pub type MieruUdpFlowResponse = (Address, u16, Vec<u8>);

type MieruUdpFlowResponses = broadcast::Sender<MieruUdpFlowResponse>;

pub type MieruUdpFlowResponseReceiver = broadcast::Receiver<MieruUdpFlowResponse>;

#[derive(Clone)]
struct MieruUdpFlowSender {
    send_tx: mpsc::Sender<zero_core::UdpFlowPacket>,
}

pub struct MieruUdpFlowHandle {
    sender: MieruUdpFlowSender,
    responses: MieruUdpFlowResponses,
}

#[derive(Clone)]
pub struct MieruUdpFlowSession {
    sender: MieruUdpFlowSender,
    responses: MieruUdpFlowResponses,
}

impl MieruUdpFlowSession {
    pub fn new(handle: MieruUdpFlowHandle) -> Self {
        Self {
            sender: handle.sender,
            responses: handle.responses,
        }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.sender.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> MieruUdpFlowResponseReceiver {
        self.responses.subscribe()
    }
}

#[derive(Clone)]
pub struct MieruUdpFlowConnection {
    session: MieruUdpFlowSession,
}

impl MieruUdpFlowConnection {
    pub fn new(session: MieruUdpFlowSession) -> Self {
        Self { session }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.session.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> MieruUdpFlowResponseReceiver {
        self.session.subscribe_responses()
    }
}

impl MieruUdpFlowSender {
    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        let packet = zero_core::UdpFlowPacket::from_parts(target, port, payload);
        let packet_len = packet.payload.len();
        self.send_tx
            .send(packet)
            .await
            .map_err(|_| Error::Io("mieru udp flow closed"))?;
        Ok(packet_len)
    }
}

impl MieruUdpFlowIo {
    pub async fn establish<S: AsyncSocket>(
        stream: &mut S,
        username: &str,
        password: &str,
    ) -> Result<Self, Error> {
        let mut outbound = MieruOutbound::connect(stream, username, password).await?;
        let assoc_req = super::tunnel::build_udp_associate_request()?;
        let assoc_seg = outbound.encrypt_client_data(&assoc_req)?;
        stream
            .write_all(&assoc_seg)
            .await
            .map_err(|_| Error::Io("mieru udp assoc write"))?;

        let mut assoc_raw = Vec::new();
        let assoc_resp = loop {
            match outbound.decrypt_server_data_with_consumed(&assoc_raw) {
                Ok((segment, consumed)) => {
                    assoc_raw.drain(..consumed);
                    break segment.payload;
                }
                Err(Error::Protocol("mieru: need more data")) => {
                    let mut scratch = [0u8; 4096];
                    let n = stream
                        .read(&mut scratch)
                        .await
                        .map_err(|_| Error::Io("mieru udp assoc read"))?;
                    if n == 0 {
                        return Err(Error::Protocol("mieru udp assoc: connection closed"));
                    }
                    assoc_raw.extend_from_slice(&scratch[..n]);
                }
                Err(error) => return Err(error),
            }
        };
        super::tunnel::validate_success_response(&assoc_resp)?;
        Ok(Self {
            outbound,
            recv_raw: Vec::new(),
        })
    }

    pub async fn establish_with_resume<S>(
        stream: &mut S,
        resume: &crate::udp::MieruUdpFlowResume,
    ) -> Result<Self, Error>
    where
        S: AsyncSocket,
    {
        Self::establish(stream, resume.username(), resume.password()).await
    }

    pub fn encrypt_packet(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let packet = udp::encode_udp_flow_packet(target, port, payload)?;
        self.encrypt_payload(&packet)
    }

    pub fn encrypt_payload(&mut self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        self.outbound.encrypt_client_data(payload)
    }

    pub fn push_encrypted_response(&mut self, data: &[u8]) {
        self.recv_raw.extend_from_slice(data);
    }

    pub fn next_packet(&mut self) -> Result<Option<MieruUdpFlowPacket>, Error> {
        match self
            .outbound
            .decrypt_server_data_with_consumed(&self.recv_raw)
        {
            Ok((segment, consumed)) => {
                self.recv_raw.drain(..consumed);
                if segment.payload.is_empty() {
                    return Ok(None);
                }
                let packet = udp::decode_udp_flow_packet(&segment.payload)?;
                let (target, port, payload) = packet.into_parts();
                Ok(Some(MieruUdpFlowPacket::new(target, port, payload)))
            }
            Err(Error::Protocol("mieru: need more data")) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn decode_encrypted_response(
        &mut self,
        data: &[u8],
    ) -> Result<Vec<MieruUdpFlowPacket>, Error> {
        self.push_encrypted_response(data);

        let mut packets = Vec::new();
        while let Some(packet) = self.next_packet()? {
            packets.push(packet);
        }

        Ok(packets)
    }

    pub async fn write_packet<S>(
        &mut self,
        stream: &mut S,
        packet: &MieruUdpFlowPacket,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let encrypted = packet.encode_with(self)?;
        stream
            .write_all(&encrypted)
            .await
            .map_err(|_| Error::Io("mieru udp flow write"))
    }

    pub async fn write_flow_packet<S>(
        &mut self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let encrypted = self.encrypt_packet(target, port, payload)?;
        stream
            .write_all(&encrypted)
            .await
            .map_err(|_| Error::Io("mieru udp flow write"))
    }

    pub async fn read_packets<S>(
        &mut self,
        stream: &mut S,
        scratch: &mut [u8],
    ) -> Result<Option<Vec<MieruUdpFlowPacket>>, Error>
    where
        S: AsyncSocket,
    {
        let n = stream
            .read(scratch)
            .await
            .map_err(|_| Error::Io("mieru udp flow read"))?;
        if n == 0 {
            return Ok(None);
        }

        self.push_encrypted_response(&scratch[..n]);

        let mut packets = Vec::new();
        while let Some(packet) = self.next_packet()? {
            packets.push(packet);
        }

        Ok(Some(packets))
    }

    pub async fn read_flow_packets<S>(
        &mut self,
        stream: &mut S,
        scratch: &mut [u8],
    ) -> Result<Option<Vec<(Address, u16, Vec<u8>)>>, Error>
    where
        S: AsyncSocket,
    {
        let Some(packets) = self.read_packets(stream, scratch).await? else {
            return Ok(None);
        };
        Ok(Some(
            packets
                .into_iter()
                .map(MieruUdpFlowPacket::into_parts)
                .collect(),
        ))
    }
}

pub fn spawn_udp_flow<S>(stream: S, flow_io: MieruUdpFlowIo) -> MieruUdpFlowHandle
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (send_tx, send_rx) = mpsc::channel::<zero_core::UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel::<MieruUdpFlowResponse>(32);
    spawn_udp_flow_task(stream, flow_io, send_rx, responses.clone());
    MieruUdpFlowHandle {
        sender: MieruUdpFlowSender { send_tx },
        responses,
    }
}

pub async fn establish_udp_flow_with_resume<S>(
    mut stream: S,
    resume: &crate::udp::MieruUdpFlowResume,
) -> Result<MieruUdpFlowConnection, Error>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + Send + 'static,
{
    let flow_io = MieruUdpFlowIo::establish_with_resume(&mut stream, resume).await?;
    Ok(MieruUdpFlowConnection::new(MieruUdpFlowSession::new(
        spawn_udp_flow(stream, flow_io),
    )))
}

fn spawn_udp_flow_task<S>(
    mut stream: S,
    mut flow_io: MieruUdpFlowIo,
    mut send_rx: mpsc::Receiver<zero_core::UdpFlowPacket>,
    responses: MieruUdpFlowResponses,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut scratch = [0u8; 4096];
        loop {
            tokio::select! {
                to_send = send_rx.recv() => {
                    match to_send {
                        Some(packet) => {
                            let Ok(encrypted) = flow_io.encrypt_packet(
                                &packet.target,
                                packet.port,
                                &packet.payload,
                            ) else {
                                break;
                            };
                            if stream.write_all(&encrypted).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                read = stream.read(&mut scratch) => {
                    match read {
                        Ok(0) => break,
                        Ok(n) => {
                            let Ok(packets) = flow_io.decode_encrypted_response(&scratch[..n]) else {
                                break;
                            };
                            for packet in packets {
                                let _ = responses.send(packet.into_parts());
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
}

impl MieruOutbound {
    /// Perform the mieru outbound handshake.
    ///
    /// Establishes the encrypted mieru session only. The session is a raw
    /// encrypted tunnel and does NOT carry a target — upstream mieru conveys
    /// the proxy target via socks5 running inside the tunnel (mita runs a
    /// socks5 server on the decrypted session). Callers must perform that
    /// socks5 handshake over the resulting stream to bind a target.
    pub async fn connect<S: AsyncSocket>(
        stream: &mut S,
        username: &str,
        password: &str,
    ) -> Result<Self, Error> {
        let pattern = TrafficPattern::from_config(None)?;
        Self::connect_with_pattern(stream, username, password, &pattern).await
    }

    pub async fn connect_with_pattern<S: AsyncSocket>(
        stream: &mut S,
        username: &str,
        password: &str,
        pattern: &TrafficPattern,
    ) -> Result<Self, Error> {
        let unix_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Protocol("mieru: time"))?
            .as_secs();

        let key = derive_key(username, password, unix_now);
        let nc = pattern.nonce_config(username, true);
        let mut client_cipher = MieruCipher::with_config(&key, &nc);
        let mut server_cipher = MieruCipher::with_config(&key, &nc);
        let session = MieruSession::new();

        // openSessionRequest carries only the session ID — no target payload.
        let padding = session_padding(u8::MAX as usize, 64, username);
        let open_meta = SessionMetadata {
            protocol_type: OPEN_SESSION_REQUEST,
            timestamp: MieruSession::timestamp_minutes(),
            session_id: session.session_id,
            sequence_number: 0,
            status_code: 0,
            payload_length: 0,
            suffix_length: padding.len() as u8,
        };
        let open_seg = build_session_segment_with_padding(
            &open_meta,
            &[],
            &mut client_cipher,
            true,
            &padding,
        )?;
        write_socket_with_fragmentation(stream, &open_seg, pattern.tcp_fragment)
            .await
            .map_err(|_| Error::Io("mieru: send open"))?;

        // Read openSessionResponse. Upstream emits no leading padding0, so the
        // nonce + encrypted metadata is exactly CORE_LEN bytes at offset 0.
        const CORE_LEN: usize = 24 + METADATA_LEN + 16; // nonce + meta + tag
        let mut resp = vec![0u8; CORE_LEN];
        read_exact(stream, &mut resp, CORE_LEN).await?;
        let mut probe = server_cipher.clone();
        let plain = probe.decrypt(true, &resp)?;
        let meta = SessionMetadata::decode(&plain);
        if meta.protocol_type != OPEN_SESSION_RESPONSE
            || meta.session_id != session.session_id
            || meta.status_code != 0
        {
            return Err(Error::Protocol("mieru: invalid open response"));
        }
        let tail = meta.payload_length as usize
            + if meta.payload_length > 0 { 16 } else { 0 }
            + meta.suffix_length as usize;
        if tail > 0 {
            let mut remaining = vec![0; tail];
            read_exact(stream, &mut remaining, tail).await?;
            resp.extend_from_slice(&remaining);
        }
        parse_segment(&resp, &mut server_cipher, true, true)?;

        Ok(Self {
            mieru_session: session,
            client_cipher,
            server_cipher,
            c2s_nonce_sent: true,
            s2c_nonce_recv: true,
            tcp_fragment: pattern.tcp_fragment,
        })
    }

    /// Encrypt data for client→server.
    pub fn encrypt_client_data(&mut self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.len() > u16::MAX as usize {
            return Err(Error::Protocol("mieru: data fragment too large"));
        }
        let (prefix_length, suffix_length) = data_padding_lengths(u8::MAX as usize);
        let meta = DataMetadata {
            protocol_type: DATA_CLIENT_TO_SERVER,
            timestamp: MieruSession::timestamp_minutes(),
            session_id: self.mieru_session.session_id,
            sequence_number: self.mieru_session.next_send_seq(),
            unack_sequence: self.mieru_session.peer_unack,
            window_size: self.mieru_session.peer_window,
            fragment_number: 0,
            prefix_length,
            payload_length: data.len() as u16,
            suffix_length,
        };
        let include_nonce = !self.c2s_nonce_sent;
        let seg = build_data_segment(&meta, data, &mut self.client_cipher, include_nonce)?;
        self.c2s_nonce_sent = true;
        Ok(seg)
    }

    /// Decrypt data from server→client.
    pub fn decrypt_server_data(&mut self, data: &[u8]) -> Result<Segment, Error> {
        self.decrypt_server_data_with_consumed(data)
            .map(|(segment, _)| segment)
    }

    pub fn decrypt_server_data_with_consumed(
        &mut self,
        data: &[u8],
    ) -> Result<(Segment, usize), Error> {
        let incl = !self.s2c_nonce_recv;
        let mut server_cipher = self.server_cipher.clone();
        let (seg, consumed) = parse_segment(data, &mut server_cipher, incl, false)?;
        self.server_cipher = server_cipher;
        self.s2c_nonce_recv = true;
        let consumed = consumed.max(segment_wire_len(&seg, incl));
        Ok((seg, consumed))
    }

    /// Build closeSessionRequest.
    pub fn close_request(&mut self) -> Result<Vec<u8>, Error> {
        let padding = session_padding(u8::MAX as usize, 64, self.client_cipher.username());
        let meta = SessionMetadata {
            protocol_type: CLOSE_SESSION_REQUEST,
            timestamp: MieruSession::timestamp_minutes(),
            session_id: self.mieru_session.session_id,
            sequence_number: self.mieru_session.next_send_seq(),
            status_code: 0,
            payload_length: 0,
            suffix_length: padding.len() as u8,
        };
        build_session_segment_with_padding(&meta, &[], &mut self.client_cipher, false, &padding)
    }
}

fn segment_wire_len(segment: &Segment, has_nonce: bool) -> usize {
    let nonce_len = if has_nonce { 24 } else { 0 };
    let meta_len = METADATA_LEN + 16;
    if let Some(meta) = segment.data_meta.as_ref() {
        nonce_len
            + meta_len
            + meta.prefix_length as usize
            + meta.payload_length as usize
            + if meta.payload_length > 0 { 16 } else { 0 }
            + meta.suffix_length as usize
    } else if let Some(meta) = segment.session_meta.as_ref() {
        nonce_len
            + meta_len
            + meta.payload_length as usize
            + if meta.payload_length > 0 { 16 } else { 0 }
    } else {
        nonce_len + meta_len
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

async fn read_exact<S: AsyncSocket>(
    stream: &mut S,
    buf: &mut [u8],
    len: usize,
) -> Result<(), Error> {
    let mut off = 0;
    while off < len {
        let n = stream
            .read(&mut buf[off..len])
            .await
            .map_err(|_| Error::Io("mieru out read"))?;
        if n == 0 {
            return Err(Error::Protocol("mieru out: conn closed"));
        }
        off += n;
    }
    Ok(())
}

/// Credential parameters for a Mieru outbound session.
///
/// The mieru session is target-agnostic (it is an encrypted tunnel); the
/// proxy target is conveyed by a socks5 handshake the caller runs over the
/// established session, matching upstream mieru.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MieruTcpTarget<'a> {
    pub username: &'a str,
    pub password: &'a str,
}

pub(crate) mod logical_udp;
