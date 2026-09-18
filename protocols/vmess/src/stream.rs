use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::crypto::{
    open_xray_response_header_length, open_xray_response_header_payload, BodyAead, BodyAeadConfig,
    GCM_TAG_LEN, MAX_BODY_PAYLOAD_SIZE,
};
use crate::inbound::VmessAccept;
use crate::shared::VmessOutboundSession;
use crate::VmessCipher;

enum ReadState {
    ResponseHeaderLength {
        expected_header: u8,
        buf: Vec<u8>,
        pos: usize,
    },
    ResponseHeaderPayload {
        expected_header: u8,
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
    Raw,
    Eof,
}

pub struct VmessAeadStream<S> {
    inner: S,
    reader: BodyCodec,
    writer: BodyCodec,
    chunk_stream: bool,
    read_plain: Vec<u8>,
    read_plain_pos: usize,
    read_state: ReadState,
    response_header_key: Option<Vec<u8>>,
    response_header_nonce: Option<Vec<u8>>,
    write_buf: Vec<u8>,
    write_pos: usize,
    pending_write_len: Option<usize>,
    write_shutdown_sent: bool,
}

enum BodyCodec {
    Aead(BodyAead),
    Plain(PlainBodyCodec),
}

struct PlainBodyCodec {
    inner: BodyAead,
}

struct BodyCodecConfig {
    key: Vec<u8>,
    nonce: Vec<u8>,
    length_key_source: Vec<u8>,
    length_nonce_source: Vec<u8>,
    cipher: VmessCipher,
    authenticated_length: bool,
    chunk_masking: bool,
    global_padding: bool,
}

struct VmessAeadStreamConfig<S> {
    inner: S,
    read_key: Vec<u8>,
    read_nonce: Vec<u8>,
    read_length_key_source: Vec<u8>,
    read_length_nonce_source: Vec<u8>,
    write_key: Vec<u8>,
    write_nonce: Vec<u8>,
    write_length_key_source: Vec<u8>,
    write_length_nonce_source: Vec<u8>,
    cipher: VmessCipher,
    chunk_stream: bool,
    authenticated_length: bool,
    chunk_masking: bool,
    global_padding: bool,
    response_header: Option<u8>,
}

impl BodyCodec {
    fn new(config: BodyCodecConfig) -> Result<Self, zero_core::Error> {
        if config.cipher.uses_plain_body() {
            Ok(Self::Plain(PlainBodyCodec {
                inner: BodyAead::new_with_length_source(BodyAeadConfig {
                    key: config.key,
                    nonce_prefix: config.nonce,
                    length_key_source: config.length_key_source,
                    length_nonce_prefix: config.length_nonce_source,
                    cipher: VmessCipher::None,
                    authenticated_length: false,
                    chunk_masking: config.chunk_masking,
                    global_padding: config.global_padding,
                })?,
            }))
        } else {
            Ok(Self::Aead(BodyAead::new_with_length_source(
                BodyAeadConfig {
                    key: config.key,
                    nonce_prefix: config.nonce,
                    length_key_source: config.length_key_source,
                    length_nonce_prefix: config.length_nonce_source,
                    cipher: config.cipher,
                    authenticated_length: config.authenticated_length,
                    chunk_masking: config.chunk_masking,
                    global_padding: config.global_padding,
                },
            )?))
        }
    }

    fn is_plain(&self) -> bool {
        matches!(self, Self::Plain(_))
    }

    fn max_plain_payload(&self) -> usize {
        if self.is_plain() {
            MAX_BODY_PAYLOAD_SIZE - 63
        } else {
            MAX_BODY_PAYLOAD_SIZE - GCM_TAG_LEN - 63
        }
    }

    fn seal_chunk(&mut self, payload: &[u8]) -> Result<Vec<u8>, zero_core::Error> {
        match self {
            Self::Aead(codec) => codec.seal_chunk(payload),
            Self::Plain(codec) => codec.seal_chunk(payload),
        }
    }

    fn open_length(&mut self, encrypted_len: &[u8]) -> Result<usize, zero_core::Error> {
        match self {
            Self::Aead(codec) => codec.open_length(encrypted_len),
            Self::Plain(codec) => codec.open_length(encrypted_len),
        }
    }

    fn open_payload(
        &mut self,
        expected_len: usize,
        payload: &[u8],
    ) -> Result<Vec<u8>, zero_core::Error> {
        match self {
            Self::Aead(codec) => codec.open_payload(expected_len, payload),
            Self::Plain(codec) => codec.open_payload(expected_len, payload),
        }
    }

    fn length_frame_size(&self) -> usize {
        match self {
            Self::Aead(codec) => codec.length_frame_size(),
            Self::Plain(codec) => codec.length_frame_size(),
        }
    }
}

impl PlainBodyCodec {
    fn seal_chunk(&mut self, payload: &[u8]) -> Result<Vec<u8>, zero_core::Error> {
        self.inner.seal_plain_chunk(payload)
    }

    fn open_length(&mut self, frame: &[u8]) -> Result<usize, zero_core::Error> {
        self.inner.open_length(frame)
    }

    fn open_payload(
        &mut self,
        expected_len: usize,
        payload: &[u8],
    ) -> Result<Vec<u8>, zero_core::Error> {
        self.inner.open_plain_payload(expected_len, payload)
    }

    fn length_frame_size(&self) -> usize {
        self.inner.length_frame_size()
    }
}

impl<S> VmessAeadStream<S> {
    pub fn outbound(inner: S, session: VmessOutboundSession) -> Result<Self, zero_core::Error> {
        Self::new(VmessAeadStreamConfig {
            inner,
            read_key: session.download_key,
            read_nonce: session.download_nonce,
            read_length_key_source: session.length_key_source,
            read_length_nonce_source: session.length_nonce_source,
            write_key: session.upload_key,
            write_nonce: session.upload_nonce,
            write_length_key_source: Vec::new(),
            write_length_nonce_source: Vec::new(),
            cipher: session.cipher,
            chunk_stream: session.chunk_stream,
            authenticated_length: session.authenticated_length,
            chunk_masking: session.chunk_masking,
            global_padding: session.global_padding,
            response_header: session.response_header,
        })
    }

    pub(crate) fn inbound(inner: S, accept: VmessAccept) -> Result<Self, zero_core::Error> {
        let stream_state = accept.into_stream_state();
        let read_length_key_source = stream_state.upload_key.clone();
        let read_length_nonce_source = stream_state.upload_nonce.clone();
        Self::new(VmessAeadStreamConfig {
            inner,
            read_key: stream_state.upload_key,
            read_nonce: stream_state.upload_nonce,
            read_length_key_source,
            read_length_nonce_source,
            write_key: stream_state.download_key,
            write_nonce: stream_state.download_nonce,
            write_length_key_source: stream_state.length_key_source,
            write_length_nonce_source: stream_state.length_nonce_source,
            cipher: stream_state.cipher,
            chunk_stream: stream_state.chunk_stream,
            authenticated_length: stream_state.authenticated_length,
            chunk_masking: stream_state.chunk_masking,
            global_padding: stream_state.global_padding,
            response_header: None,
        })
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    fn new(config: VmessAeadStreamConfig<S>) -> Result<Self, zero_core::Error> {
        let VmessAeadStreamConfig {
            inner,
            read_key,
            read_nonce,
            mut read_length_key_source,
            mut read_length_nonce_source,
            write_key,
            write_nonce,
            mut write_length_key_source,
            mut write_length_nonce_source,
            cipher,
            chunk_stream,
            authenticated_length,
            chunk_masking,
            global_padding,
            response_header,
        } = config;
        let response_read_key = read_key.clone();
        let response_read_nonce = read_nonce.clone();
        if read_length_key_source.is_empty() {
            read_length_key_source = read_key.clone();
        }
        if read_length_nonce_source.is_empty() {
            read_length_nonce_source = read_nonce.clone();
        }
        if write_length_key_source.is_empty() {
            write_length_key_source = write_key.clone();
        }
        if write_length_nonce_source.is_empty() {
            write_length_nonce_source = write_nonce.clone();
        }
        let reader = BodyCodec::new(BodyCodecConfig {
            key: read_key,
            nonce: read_nonce,
            length_key_source: read_length_key_source,
            length_nonce_source: read_length_nonce_source,
            cipher,
            authenticated_length,
            chunk_masking,
            global_padding,
        })?;
        let writer = BodyCodec::new(BodyCodecConfig {
            key: write_key,
            nonce: write_nonce,
            length_key_source: write_length_key_source,
            length_nonce_source: write_length_nonce_source,
            cipher,
            authenticated_length,
            chunk_masking,
            global_padding,
        })?;
        let (read_state, response_header_key, response_header_nonce) =
            if let Some(expected_header) = response_header {
                (
                    ReadState::ResponseHeaderLength {
                        expected_header,
                        buf: vec![0_u8; 18],
                        pos: 0,
                    },
                    Some(response_read_key),
                    Some(response_read_nonce),
                )
            } else {
                (
                    if chunk_stream {
                        ReadState::Length {
                            buf: vec![0_u8; reader.length_frame_size()],
                            pos: 0,
                        }
                    } else {
                        ReadState::Raw
                    },
                    None,
                    None,
                )
            };
        Ok(Self {
            inner,
            reader,
            writer,
            chunk_stream,
            read_plain: Vec::new(),
            read_plain_pos: 0,
            read_state,
            response_header_key,
            response_header_nonce,
            write_buf: Vec::new(),
            write_pos: 0,
            pending_write_len: None,
            write_shutdown_sent: false,
        })
    }
}

pub(crate) fn wrap_tcp_inbound_stream<S>(
    stream: S,
    accept: VmessAccept,
) -> Result<VmessAeadStream<S>, zero_core::Error> {
    VmessAeadStream::inbound(stream, accept)
}

impl<S> VmessAeadStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn serve_read_plain(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        if self.read_plain_pos >= self.read_plain.len() {
            self.read_plain.clear();
            self.read_plain_pos = 0;
            return false;
        }

        let available = &self.read_plain[self.read_plain_pos..];
        let n = available.len().min(buf.remaining());
        buf.put_slice(&available[..n]);
        self.read_plain_pos += n;
        true
    }

    fn poll_read_decrypted(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 || self.serve_read_plain(buf) {
            return Poll::Ready(Ok(()));
        }

        loop {
            match &mut self.read_state {
                ReadState::ResponseHeaderLength {
                    expected_header,
                    buf: encrypted_len,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_len, pos, false)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        let encrypted_len_array: [u8; 18] =
                            encrypted_len.as_slice().try_into().map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "vmess response header length frame invalid",
                                )
                            })?;
                        let response_key =
                            self.response_header_key.as_deref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "vmess response header key missing",
                                )
                            })?;
                        let response_nonce =
                            self.response_header_nonce.as_deref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "vmess response header nonce missing",
                                )
                            })?;
                        let expected_len = open_xray_response_header_length(
                            response_key,
                            response_nonce,
                            &encrypted_len_array,
                        )
                        .map_err(protocol_error)?;
                        if expected_len > 256 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "vmess response header too large",
                            )));
                        }
                        self.read_state = ReadState::ResponseHeaderPayload {
                            expected_header: *expected_header,
                            buf: vec![0_u8; expected_len + GCM_TAG_LEN],
                            pos: 0,
                        };
                    }
                },
                ReadState::ResponseHeaderPayload {
                    expected_header,
                    buf: encrypted_payload,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_payload, pos, false)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        let response_key =
                            self.response_header_key.as_deref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "vmess response header key missing",
                                )
                            })?;
                        let response_nonce =
                            self.response_header_nonce.as_deref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "vmess response header nonce missing",
                                )
                            })?;
                        let plaintext = open_xray_response_header_payload(
                            response_key,
                            response_nonce,
                            encrypted_payload,
                        )
                        .map_err(protocol_error)?;
                        if plaintext.len() < 4 || plaintext[0] != *expected_header {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "vmess server rejected connection",
                            )));
                        }
                        self.read_state = if self.chunk_stream {
                            ReadState::Length {
                                buf: vec![0_u8; self.reader.length_frame_size()],
                                pos: 0,
                            }
                        } else {
                            ReadState::Raw
                        };
                        self.response_header_key = None;
                        self.response_header_nonce = None;
                    }
                },
                ReadState::Length {
                    buf: encrypted_len,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_len, pos, true)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        if encrypted_len.is_empty() {
                            self.read_state = ReadState::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        let expected_len = self
                            .reader
                            .open_length(encrypted_len)
                            .map_err(protocol_error)?;
                        self.read_state = ReadState::Payload {
                            expected_len,
                            buf: vec![0_u8; expected_len],
                            pos: 0,
                        };
                    }
                },
                ReadState::Payload {
                    expected_len,
                    buf: encrypted_payload,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_payload, pos, false)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.read_plain = self
                            .reader
                            .open_payload(*expected_len, encrypted_payload)
                            .map_err(protocol_error)?;
                        self.read_plain_pos = 0;
                        if self.read_plain.is_empty() {
                            self.read_state = ReadState::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        self.read_state = ReadState::Length {
                            buf: vec![0_u8; self.reader.length_frame_size()],
                            pos: 0,
                        };
                        if self.serve_read_plain(buf) {
                            return Poll::Ready(Ok(()));
                        }
                    }
                },
                ReadState::Raw => return Pin::new(&mut self.inner).poll_read(cx, buf),
                ReadState::Eof => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.write_pos < self.write_buf.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.write_buf[self.write_pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "vmess write zero",
                    )));
                }
                Poll::Ready(Ok(n)) => self.write_pos += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.write_buf.clear();
        self.write_pos = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_write_encrypted(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.chunk_stream {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        }
        let had_pending = self.write_pos < self.write_buf.len();
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => {
                if had_pending {
                    if let Some(n) = self.pending_write_len.take() {
                        return Poll::Ready(Ok(n));
                    }
                }
            }
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let n = buf.len().min(self.writer.max_plain_payload());
        self.write_buf = self.writer.seal_chunk(&buf[..n]).map_err(protocol_error)?;
        self.write_pos = 0;
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) | Poll::Pending => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        }
        if self.write_pos == 0 && self.write_pos < self.write_buf.len() {
            self.pending_write_len = Some(n);
            return Poll::Pending;
        }
        Poll::Ready(Ok(n))
    }

    fn poll_shutdown_encrypted(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.chunk_stream {
            return Pin::new(&mut self.inner).poll_shutdown(cx);
        }
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }

        if !self.write_shutdown_sent {
            self.write_buf = self.writer.seal_chunk(&[]).map_err(protocol_error)?;
            self.write_pos = 0;
            self.write_shutdown_sent = true;
            match self.poll_flush_pending(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
        }

        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_shutdown(cx),
            other => other,
        }
    }
}

impl<S> AsyncRead for VmessAeadStream<S>
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

impl<S> AsyncWrite for VmessAeadStream<S>
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
        Pin::into_inner(self).poll_shutdown_encrypted(cx)
    }
}

fn poll_fill<S>(
    inner: &mut S,
    cx: &mut Context<'_>,
    buf: &mut Vec<u8>,
    pos: &mut usize,
    allow_clean_eof: bool,
) -> io::Result<Poll<()>>
where
    S: AsyncRead + Unpin,
{
    while *pos < buf.len() {
        let mut read_buf = ReadBuf::new(&mut buf[*pos..]);
        match Pin::new(&mut *inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                if allow_clean_eof && *pos == 0 {
                    buf.clear();
                    return Ok(Poll::Ready(()));
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "vmess unexpected EOF",
                ));
            }
            Poll::Ready(Ok(())) => *pos += read_buf.filled().len(),
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Pending => return Ok(Poll::Pending),
        }
    }
    Ok(Poll::Ready(()))
}

fn protocol_error(error: zero_core::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
