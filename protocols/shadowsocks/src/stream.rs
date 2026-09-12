mod read;
mod write;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    decrypt_tcp_chunk_length, decrypt_tcp_chunk_payload, derive_download_key, encrypt_tcp_chunk,
    CipherKind, ShadowsocksAccept, ShadowsocksOutboundSession, TCP_CHUNK_SIZE_LEN,
};

enum ReadState {
    Legacy,
    Salt {
        buf: Vec<u8>,
        pos: usize,
    },
    /// 2022 outbound: read the response fixed-length header chunk after the
    /// salt (it carries the request salt and the first payload length).
    ResponseHeader2022 {
        buf: Vec<u8>,
        pos: usize,
    },
    /// 2022 outbound: read the first payload chunk whose length came from the
    /// response header. Subsequent chunks use the normal Length/Payload loop.
    FirstPayload2022 {
        expected_len: usize,
        buf: Vec<u8>,
        pos: usize,
    },
    Length {
        buf: Vec<u8>,
        pos: usize,
    },
    Payload {
        expected_len: usize,
        buf: Vec<u8>,
        pos: usize,
    },
    Eof,
}

pub struct ShadowsocksAeadStream<S> {
    inner: S,
    legacy_read: Option<crate::shared::legacy::LegacyCipherState>,
    legacy_write: Option<crate::shared::legacy::LegacyCipherState>,
    replay: crate::shared::legacy_replay::LegacyReplay,
    legacy_response_salt: Option<Vec<u8>>,
    modern_response_salt: Option<Vec<u8>>,
    cipher: CipherKind,
    read_key: Option<Vec<u8>>,
    read_password: Option<Vec<u8>>,
    read_nonce: u128,
    read_state: ReadState,
    read_plain: Vec<u8>,
    read_plain_pos: usize,
    write_key: Vec<u8>,
    write_nonce: u128,
    write_buf: Vec<u8>,
    write_pos: usize,
    /// True for 2022 edition streams.
    is_2022: bool,
    /// Inbound: the request salt echoed in the response fixed header.
    /// Outbound: the request salt we sent, verified against the response header.
    request_salt: Vec<u8>,
    /// Inbound 2022: the response salt, emitted with the first response header chunk.
    response_salt: Vec<u8>,
    /// Inbound 2022: true until the first write emits the response header chunk.
    write_response_header_pending: bool,
}

impl<S> ShadowsocksAeadStream<S> {
    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.replay = replay;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn inbound(
        inner: S,
        cipher: CipherKind,
        upload_key: Vec<u8>,
        next_upload_nonce: u128,
        download_key: Vec<u8>,
        response_salt: Vec<u8>,
        remaining_payload: Vec<u8>,
        is_2022: bool,
        request_salt: Vec<u8>,
    ) -> Self {
        let is_2022_enabled = is_2022 && cipher.is_blake3();
        // For 2022 the response salt is emitted together with the first
        // response header chunk, so write_buf starts empty and the first
        // write builds salt+header+payload. For legacy, write the salt up
        // front exactly as before.
        let write_buf = if is_2022_enabled {
            Vec::new()
        } else {
            response_salt.clone()
        };
        Self {
            inner,
            legacy_read: None,
            legacy_write: None,
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
            legacy_response_salt: None,
            modern_response_salt: None,
            cipher,
            read_key: Some(upload_key),
            read_password: None,
            read_nonce: next_upload_nonce,
            read_state: ReadState::Length {
                buf: vec![0_u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()],
                pos: 0,
            },
            read_plain: remaining_payload,
            read_plain_pos: 0,
            write_key: download_key,
            write_nonce: 0,
            write_buf,
            write_pos: 0,
            is_2022: is_2022_enabled,
            request_salt,
            response_salt,
            write_response_header_pending: is_2022_enabled,
        }
    }

    pub fn outbound(inner: S, session: ShadowsocksOutboundSession, password: Vec<u8>) -> Self {
        let cipher = session.cipher;
        let is_2022 = cipher.is_blake3();
        Self {
            inner,
            legacy_read: None,
            legacy_write: session.legacy,
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
            legacy_response_salt: None,
            modern_response_salt: None,
            cipher,
            read_key: None,
            read_password: Some(password),
            read_nonce: 0,
            read_state: ReadState::Salt {
                buf: vec![0_u8; cipher.salt_len()],
                pos: 0,
            },
            read_plain: Vec::new(),
            read_plain_pos: 0,
            write_key: session.session_key,
            write_nonce: session.next_upload_nonce,
            write_buf: Vec::new(),
            write_pos: 0,
            is_2022,
            request_salt: session.request_salt,
            response_salt: Vec::new(),
            write_response_header_pending: false,
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl ShadowsocksAccept {
    /// Wrap an accepted inbound TCP stream with Shadowsocks AEAD framing.
    ///
    /// The protocol crate owns the server-to-client response salt and download
    /// key derivation. The runtime only provides the already accepted transport.
    pub fn into_aead_stream<S>(
        self,
        inner: S,
        password: &[u8],
    ) -> Result<ShadowsocksAeadStream<S>, zero_core::Error> {
        let mut response_salt = vec![0_u8; self.cipher.salt_len()];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut response_salt)
            .map_err(|_| zero_core::Error::Protocol("ss: response salt random failed"))?;
        self.into_aead_stream_with_response_salt(inner, password, response_salt)
    }

    /// Wrap an accepted inbound TCP stream with an explicit response salt.
    ///
    /// This is primarily useful for deterministic protocol tests.
    pub fn into_aead_stream_with_response_salt<S>(
        mut self,
        inner: S,
        password: &[u8],
        response_salt: Vec<u8>,
    ) -> Result<ShadowsocksAeadStream<S>, zero_core::Error> {
        let legacy_read = self.legacy.take();
        let legacy_write = if self.cipher.is_stream() {
            Some(crate::shared::legacy::LegacyCipherState::new(
                self.cipher,
                password,
                &response_salt,
            )?)
        } else {
            None
        };
        let download_key = if self.cipher.is_stream() {
            vec![]
        } else {
            derive_download_key(self.cipher, password, &response_salt)?
        };
        let is_2022 = self.cipher.is_blake3();
        let mut stream = ShadowsocksAeadStream::inbound(
            inner,
            self.cipher,
            self.session_key,
            self.next_upload_nonce,
            download_key,
            response_salt,
            self.remaining_payload,
            is_2022,
            self.request_salt,
        );
        if legacy_read.is_some() {
            stream.read_state = ReadState::Legacy;
        }
        stream.legacy_read = legacy_read;
        stream.legacy_write = legacy_write;
        Ok(stream)
    }
}

impl<S> AsyncRead for ShadowsocksAeadStream<S>
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

impl<S> AsyncWrite for ShadowsocksAeadStream<S>
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = Pin::into_inner(self);
        match this.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.inner).poll_flush(cx),
            other => other,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = Pin::into_inner(self);
        match this.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.inner).poll_shutdown(cx),
            other => other,
        }
    }
}
