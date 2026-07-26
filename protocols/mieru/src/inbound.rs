// Mieru protocol inbound handler — inbound.rs

use alloc::vec::Vec;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_core::{
    Error, InboundClientResponse, InboundStreamUdpRelay, Network, ProtocolType, Session,
    SessionAuth,
};
use zero_traits::AsyncSocket;

use crate::crypto::{try_derive_keys, MieruCipher};
use crate::metadata::{
    DataMetadata, SessionMetadata, DATA_SERVER_TO_CLIENT, METADATA_LEN, OPEN_SESSION_REQUEST,
    OPEN_SESSION_RESPONSE,
};
use crate::segment::{build_data_segment, build_session_segment, parse_segment, Segment};
use crate::session::MieruSession;

/// Mieru inbound handler.
#[derive(Debug, Default, Clone)]
pub struct MieruInbound;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruInboundProfile {
    users: Vec<(String, String)>,
}

impl MieruInboundProfile {
    pub fn from_config(users: Vec<(String, String)>) -> Self {
        Self { users }
    }

    pub fn from_config_parts<I>(users: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self::from_config(users.into_iter().collect())
    }

    pub fn from_config_users<I, U>(users: I) -> Self
    where
        I: IntoIterator<Item = U>,
        U: IntoMieruInboundUserConfig,
    {
        Self::from_config_parts(users.into_iter().map(U::into_mieru_inbound_user_config))
    }

    pub fn inbound_auth(&self) -> SessionAuth {
        MieruInbound.inbound_auth()
    }

    pub async fn accept_request<S: AsyncSocket>(
        &self,
        stream: &mut S,
    ) -> Result<MieruAccept, Error> {
        MieruInbound.accept_request(stream, &self.users).await
    }

    pub async fn accept_tunneled_stream<S>(
        &self,
        mut stream: S,
    ) -> Result<(Session, MieruInboundStream<S>), Error>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
    {
        let accept = self.accept_request(&mut stream).await?;
        let mut client = MieruInboundStream::new(stream, accept);
        let mut session = client.accept_tunneled_socks5_session().await?;
        session.apply_auth(self.inbound_auth());
        Ok((session, client))
    }

    pub async fn accept_client<S>(
        &self,
        stream: S,
    ) -> Result<MieruInboundAcceptedSession<MieruInboundStream<S>>, Error>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
    {
        let (session, client) = self.accept_tunneled_stream(stream).await?;
        Ok(MieruInboundAcceptedSession::from_session_stream(
            session, client,
        ))
    }
}

pub fn inbound_profile_from_config_users<I, U>(users: I) -> MieruInboundProfile
where
    I: IntoIterator<Item = U>,
    U: IntoMieruInboundUserConfig,
{
    MieruInboundProfile::from_config_users(users)
}

pub trait IntoMieruInboundUserConfig {
    fn into_mieru_inbound_user_config(self) -> (String, String);
}

impl IntoMieruInboundUserConfig for (String, String) {
    fn into_mieru_inbound_user_config(self) -> (String, String) {
        self
    }
}

impl IntoMieruInboundUserConfig for (&str, &str) {
    fn into_mieru_inbound_user_config(self) -> (String, String) {
        (self.0.to_owned(), self.1.to_owned())
    }
}

/// Result of accepting a mieru TCP connection.
///
/// The mieru session is target-agnostic: it is an encrypted tunnel. The proxy
/// target is conveyed by a socks5 request that the client sends inside the
/// tunnel after the handshake, so the caller must read that request over the
/// decrypted stream to obtain the target (mirroring the upstream mieru model).
pub struct MieruAccept {
    mieru_session: MieruSession,
    client_cipher: MieruCipher,
    server_cipher: MieruCipher,
    /// Bytes already decrypted from the first segment beyond its metadata
    /// (usually empty for socks5-in-tunnel clients).
    remaining_payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MieruInboundSessionKind {
    Tcp,
    Udp,
}

#[allow(clippy::large_enum_variant)]
pub enum MieruInboundAcceptedSession<S> {
    Tcp {
        session: Session,
        stream: S,
    },
    Udp {
        session: Session,
        relay: MieruInboundUdpRelay<S>,
    },
}

pub struct MieruInboundUdpRelay<S> {
    auth: Option<SessionAuth>,
    responder: crate::udp::MieruInboundUdpResponder,
    stream: S,
}

pub fn classify_inbound_session(session: &Session) -> MieruInboundSessionKind {
    match session.network {
        Network::Udp => MieruInboundSessionKind::Udp,
        Network::Tcp => MieruInboundSessionKind::Tcp,
    }
}

impl<S> MieruInboundAcceptedSession<S> {
    pub fn from_session_stream(session: Session, stream: S) -> Self {
        match classify_inbound_session(&session) {
            MieruInboundSessionKind::Tcp => Self::Tcp { session, stream },
            MieruInboundSessionKind::Udp => {
                let auth = session.auth.clone();
                Self::Udp {
                    session,
                    relay: MieruInboundUdpRelay::new(
                        stream,
                        MieruInbound.accept_udp_session(),
                        auth,
                    ),
                }
            }
        }
    }
}

impl<S> MieruInboundUdpRelay<S> {
    fn new(
        stream: S,
        responder: crate::udp::MieruInboundUdpResponder,
        auth: Option<SessionAuth>,
    ) -> Self {
        Self {
            auth,
            responder,
            stream,
        }
    }

    fn into_parts(self) -> (S, crate::udp::MieruInboundUdpResponder, Option<SessionAuth>) {
        (self.stream, self.responder, self.auth)
    }
}

impl<S> InboundStreamUdpRelay for MieruInboundUdpRelay<S>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
{
    type Stream = S;
    type Responder = crate::udp::MieruInboundUdpResponder;

    fn into_stream_udp_parts(self) -> (Self::Stream, Self::Responder, Option<SessionAuth>) {
        self.into_parts()
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

/// Mieru inbound data-phase codec.
///
/// This type owns the protocol state for decrypting client-to-server data and
/// encrypting server-to-client data after the inbound handshake. Runtime code
/// should wrap it in an I/O adapter instead of touching ciphers or segment
/// metadata directly.
pub struct MieruInboundDataCodec {
    mieru_session: MieruSession,
    client_cipher: MieruCipher,
    server_cipher: MieruCipher,
    c2s_nonce_recv: bool,
    s2c_nonce_sent: bool,
}

impl MieruInboundDataCodec {
    pub fn new(accept: MieruAccept) -> (Self, Vec<u8>) {
        (
            Self {
                mieru_session: accept.mieru_session,
                client_cipher: accept.client_cipher,
                server_cipher: accept.server_cipher,
                c2s_nonce_recv: true,
                s2c_nonce_sent: true,
            },
            accept.remaining_payload,
        )
    }

    pub fn decrypt_client_data_with_consumed(
        &mut self,
        data: &[u8],
    ) -> Result<(Segment, usize), Error> {
        let include_nonce = !self.c2s_nonce_recv;
        let mut client_cipher = self.client_cipher.clone();
        let (segment, consumed) = parse_segment(data, &mut client_cipher, include_nonce, false)?;
        self.client_cipher = client_cipher;
        self.c2s_nonce_recv = true;
        let consumed = consumed.max(segment_wire_len(&segment, include_nonce));
        Ok((segment, consumed))
    }

    pub fn encrypt_server_data(&mut self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let metadata = DataMetadata {
            protocol_type: DATA_SERVER_TO_CLIENT,
            timestamp: MieruSession::timestamp_minutes(),
            session_id: self.mieru_session.session_id,
            sequence_number: self.mieru_session.next_send_seq(),
            unack_sequence: 0,
            window_size: 1024,
            fragment_number: 0,
            prefix_length: 0,
            payload_length: data.len() as u16,
            suffix_length: 0,
        };
        let include_nonce = !self.s2c_nonce_sent;
        let segment = build_data_segment(&metadata, data, &mut self.server_cipher, include_nonce)?;
        self.s2c_nonce_sent = true;
        Ok(segment)
    }
}

/// Async stream for the Mieru inbound data phase.
///
/// The stream owns Mieru's data-phase encryption/decryption buffers and exposes
/// plain tunnel bytes to the caller.
pub struct MieruInboundStream<S> {
    inner: S,
    codec: MieruInboundDataCodec,
    read_buf: Vec<u8>,
    read_pos: usize,
    raw_read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    write_pos: usize,
    write_plain_len: usize,
}

impl<S> MieruInboundStream<S> {
    pub fn new(inner: S, accept: MieruAccept) -> Self {
        let (codec, read_buf) = MieruInboundDataCodec::new(accept);
        Self {
            inner,
            codec,
            read_buf,
            read_pos: 0,
            raw_read_buf: Vec::new(),
            write_buf: Vec::new(),
            write_pos: 0,
            write_plain_len: 0,
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub async fn accept_tunneled_socks5_session(&mut self) -> Result<Session, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        super::tunnel::accept_tunneled_session(self).await
    }
}

impl<S> AsyncRead for MieruInboundStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = Pin::into_inner(self);

        if this.read_pos < this.read_buf.len() {
            let remaining = &this.read_buf[this.read_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.read_pos += n;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        loop {
            match this
                .codec
                .decrypt_client_data_with_consumed(&this.raw_read_buf)
            {
                Ok((segment, consumed)) => {
                    this.raw_read_buf.drain(..consumed);

                    let payload = segment.payload;
                    if payload.is_empty() {
                        continue;
                    }

                    let n = payload.len().min(buf.remaining());
                    buf.put_slice(&payload[..n]);
                    if n < payload.len() {
                        this.read_buf = payload[n..].to_vec();
                        this.read_pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                Err(Error::Protocol("mieru: need more data")) => {}
                Err(error) => {
                    return Poll::Ready(Err(io::Error::other(format!("mieru decrypt: {error}"))));
                }
            }

            let before = this.raw_read_buf.len();
            this.raw_read_buf.resize(before + 8192, 0);
            let mut read_buf = ReadBuf::new(&mut this.raw_read_buf[before..]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    let filled = read_buf.filled().len();
                    this.raw_read_buf.truncate(before + filled);
                    if filled == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(error)) => {
                    this.raw_read_buf.truncate(before);
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => {
                    this.raw_read_buf.truncate(before);
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<S> AsyncWrite for MieruInboundStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = Pin::into_inner(self);

        if this.write_buf.is_empty() {
            match this.codec.encrypt_server_data(buf) {
                Ok(segment) => {
                    this.write_buf = segment;
                    this.write_pos = 0;
                    this.write_plain_len = buf.len();
                }
                Err(_) => return Poll::Ready(Err(io::Error::other("mieru encrypt failed"))),
            }
        }

        while this.write_pos < this.write_buf.len() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buf[this.write_pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "mieru write zero",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_pos += n;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let written = this.write_plain_len;
        this.write_buf.clear();
        this.write_pos = 0;
        this.write_plain_len = 0;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> AsyncSocket for MieruInboundStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    type Error = io::Error;

    async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl<S> InboundClientResponse<MieruInboundStream<S>> for MieruInbound
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn send_ok(&self, _client: &mut MieruInboundStream<S>) -> Result<(), Error> {
        Ok(())
    }

    async fn send_blocked(&self, _client: &mut MieruInboundStream<S>) -> Result<(), Error> {
        Ok(())
    }

    async fn send_upstream_failure(&self, client: &mut MieruInboundStream<S>) -> Result<(), Error> {
        self.send_blocked(client).await
    }
}

impl MieruInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("mieru")
    }

    pub fn inbound_auth(&self) -> SessionAuth {
        let mut auth = SessionAuth::new("mieru");
        auth.principal_key = Some("mieru".to_owned());
        auth
    }

    pub fn udp_session(&self) -> crate::udp::MieruInboundUdpSession {
        crate::udp::MieruInboundUdpSession::new()
    }

    pub fn udp_responder(&self) -> crate::udp::MieruInboundUdpResponder {
        crate::udp::MieruInboundUdpResponder::new(self.udp_session())
    }

    pub fn accept_udp_session(&self) -> crate::udp::MieruInboundUdpResponder {
        self.udp_responder()
    }

    /// Accept a mieru TCP connection — perform the mieru handshake only.
    ///
    /// Establishes the encrypted session and replies with openSessionResponse.
    /// The proxy target is NOT known here; the caller reads a socks5 request
    /// over the decrypted stream to obtain it.
    pub async fn accept_request<S: AsyncSocket>(
        &self,
        stream: &mut S,
        users: &[(String, String)],
    ) -> Result<MieruAccept, Error> {
        // Read first segment: nonce(24) + encrypted_meta(32) + tag(16) = 72 bytes.
        // Upstream mieru (and Zero's outbound) emit no leading padding0, so the
        // nonce is at offset 0.
        const SEGMENT_CORE: usize = 24 + 32 + 16;
        let mut first = vec![0u8; SEGMENT_CORE];
        read_exact(stream, &mut first, SEGMENT_CORE).await?;

        let unix_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Protocol("mieru: system time error"))?
            .as_secs();

        // Try each user's key to decrypt the openSessionRequest metadata.
        let mut matched: Option<(MieruCipher, MieruCipher, SessionMetadata)> = None;

        for (username, password) in users {
            let keys = try_derive_keys(username, password, unix_now);
            for key in &keys {
                let mut c = MieruCipher::new(key);
                if let Ok(pt) = c.decrypt(true, &first) {
                    if pt.len() >= METADATA_LEN {
                        let meta = SessionMetadata::decode(&pt[..METADATA_LEN]);
                        if meta.protocol_type == OPEN_SESSION_REQUEST {
                            matched = Some((c, MieruCipher::new(key), meta));
                            break;
                        }
                    }
                }
            }
            if matched.is_some() {
                break;
            }
        }

        let (mut client_cipher, mut server_cipher, open_req) =
            matched.ok_or(Error::Protocol("mieru: no valid user key found"))?;

        // socks5-in-tunnel clients send no target in openSessionRequest. Consume
        // any declared payload defensively; the target arrives via a socks5
        // request in the data phase, read by the proxy handler.
        let remaining_payload = if open_req.payload_length > 0 {
            let plen = open_req.payload_length as usize;
            let mut payload_ct = vec![0u8; plen + 16]; // ciphertext + tag
            read_exact(stream, &mut payload_ct, plen + 16).await?;
            client_cipher.decrypt(false, &payload_ct)?
        } else {
            Vec::new()
        };

        // Send openSessionResponse.
        let session = MieruSession::with_id(open_req.session_id);
        let resp_meta = SessionMetadata {
            protocol_type: OPEN_SESSION_RESPONSE,
            timestamp: MieruSession::timestamp_minutes(),
            session_id: open_req.session_id,
            sequence_number: 0,
            status_code: 0,
            payload_length: 0,
            suffix_length: 0,
        };
        let resp_seg = build_session_segment(&resp_meta, &[], &mut server_cipher, true)?;
        stream
            .write_all(&resp_seg)
            .await
            .map_err(|_| Error::Io("mieru: write response"))?;

        Ok(MieruAccept {
            mieru_session: session,
            client_cipher,
            server_cipher,
            remaining_payload,
        })
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

async fn read_exact<S: AsyncSocket>(
    stream: &mut S,
    buf: &mut [u8],
    len: usize,
) -> Result<(), Error> {
    let mut offset = 0;
    while offset < len {
        let n = stream
            .read(&mut buf[offset..len])
            .await
            .map_err(|_| Error::Io("mieru read"))?;
        if n == 0 {
            return Err(Error::Protocol("mieru: connection closed"));
        }
        offset += n;
    }
    Ok(())
}
