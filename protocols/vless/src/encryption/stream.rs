// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::crypto::{ctr, header, invalid, mask, record_length, AeadState, Ctr};
use std::{
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Bounded, cancellation-safe record state. Handshake and application bytes are
/// written once; dropping a pending read never resets a nonce or loses a prefix.
pub struct EncryptionStream<S> {
    pub(super) inner: S,
    control: zero_traits::TransportBypassControl,
    out_raw: super::masking::HeaderMask,
    in_raw: super::masking::HeaderMask,
    pub(super) key: Vec<u8>,
    pub(super) send: AeadState,
    pub(super) receive: Option<AeadState>,
    pub(super) out_mask: Option<Ctr>,
    pub(super) in_mask: Option<Ctr>,
    pub(super) random: bool,
    pub(super) resume_guard: Option<super::state::ResumeGuard>,
    pending: Vec<u8>,
    written: usize,
    prefix_len: Option<usize>,
    input: Vec<u8>,
    needed: usize,
    read_header: Option<[u8; 5]>,
    plain: Vec<u8>,
    offset: usize,
    failed: bool,
}
impl<S> EncryptionStream<S> {
    pub(super) fn new(inner: S, key: Vec<u8>, send: AeadState, receive: Option<AeadState>) -> Self {
        let prefix_len = receive.is_none().then_some(16);
        Self {
            inner,
            control: Default::default(),
            out_raw: Default::default(),
            in_raw: Default::default(),
            key,
            send,
            receive,
            out_mask: None,
            in_mask: None,
            random: false,
            resume_guard: None,
            pending: Vec::new(),
            written: 0,
            prefix_len,
            input: Vec::new(),
            needed: prefix_len.unwrap_or(5),
            read_header: None,
            plain: Vec::new(),
            offset: 0,
            failed: false,
        }
    }
    pub(super) fn prewrite(&mut self, bytes: Vec<u8>) {
        self.pending = bytes;
    }
    pub(super) fn peer_padding(&mut self, length: usize) -> io::Result<()> {
        if !(17..=65535).contains(&length) {
            return Err(invalid("invalid encrypted padding length"));
        }
        self.prefix_len = Some(length);
        self.needed = length;
        Ok(())
    }
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
    pub fn map_inner<T>(self, map: impl FnOnce(S) -> T) -> EncryptionStream<T> {
        EncryptionStream {
            inner: map(self.inner),
            control: self.control,
            out_raw: self.out_raw,
            in_raw: self.in_raw,
            key: self.key,
            send: self.send,
            receive: self.receive,
            out_mask: self.out_mask,
            in_mask: self.in_mask,
            random: self.random,
            resume_guard: self.resume_guard,
            pending: self.pending,
            written: self.written,
            prefix_len: self.prefix_len,
            input: self.input,
            needed: self.needed,
            read_header: self.read_header,
            plain: self.plain,
            offset: self.offset,
            failed: self.failed,
        }
    }
}
impl<S: AsyncWrite + Unpin> EncryptionStream<S> {
    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.written < self.pending.len() {
            let n =
                ready!(Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.written..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.written += n;
        }
        self.pending.clear();
        self.written = 0;
        Poll::Ready(Ok(()))
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for EncryptionStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(invalid("encrypted stream is closed after failure")));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Err(err) = ready!(this.drain(cx)) {
            this.failed = true;
            return Poll::Ready(Err(err));
        }
        if this.control.write_bypass_requested() {
            let n = data.len().min(8192);
            this.pending.extend_from_slice(&data[..n]);
            if let Err(error) = this
                .out_raw
                .apply(&mut this.out_mask, &mut this.pending, false)
            {
                this.failed = true;
                return Poll::Ready(Err(error));
            }
            return Poll::Ready(Ok(n));
        }
        let n = data.len().min(8192);
        let mut header = header(n + 16);
        let rotate = this.send.exhausted();
        let encrypted = this.send.seal(&data[..n], &header)?;
        if rotate {
            let mut context = header.to_vec();
            context.extend(&encrypted);
            this.send = AeadState::new(&context, &this.key, this.send.aes);
        }
        mask(&mut this.out_mask, &mut header);
        this.pending.extend(header);
        this.pending.extend(encrypted);
        // Report consumption immediately after accepting into the bounded queue.
        // A subsequent poll never reports an earlier caller's byte count.
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}
impl<S: AsyncRead + Unpin> EncryptionStream<S> {
    fn read_record(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        loop {
            while self.input.len() < self.needed {
                let mut bytes = [0; 4096];
                let take = bytes.len().min(self.needed - self.input.len());
                let mut buf = ReadBuf::new(&mut bytes[..take]);
                ready!(Pin::new(&mut self.inner).poll_read(cx, &mut buf))?;
                if buf.filled().is_empty() {
                    if self.input.is_empty()
                        && self.read_header.is_none()
                        && self.prefix_len.is_none()
                    {
                        return Poll::Ready(Ok(false));
                    }
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }
                self.input.extend_from_slice(buf.filled());
            }
            if self.prefix_len.take().is_some() {
                if let Some(aead) = &mut self.receive {
                    aead.open(&self.input, &[])?;
                } else {
                    self.receive = Some(AeadState::new(&self.input, &self.key, self.send.aes));
                    if self.random {
                        self.in_mask =
                            Some(ctr(&self.key, self.input.as_slice().try_into().unwrap()));
                    }
                }
                self.input.clear();
                self.needed = 5;
                continue;
            }
            if let Some(header) = self.read_header.take() {
                let receive = self
                    .receive
                    .as_mut()
                    .ok_or_else(|| invalid("missing receive cipher"))?;
                let rotate = receive.exhausted();
                let next = if rotate {
                    let mut context = header.to_vec();
                    context.extend(&self.input);
                    Some(AeadState::new(&context, &self.key, receive.aes))
                } else {
                    None
                };
                self.plain = receive.open(&self.input, &header)?;
                if let Some(next) = next {
                    self.receive = Some(next);
                }
                self.resume_guard = None;
                self.offset = 0;
                self.input.clear();
                self.needed = 5;
                return Poll::Ready(Ok(true));
            }
            let mut header: [u8; 5] = self.input.as_slice().try_into().unwrap();
            mask(&mut self.in_mask, &mut header);
            self.needed = match record_length(&header) {
                Ok(length) => length,
                Err(err) => {
                    if let Some(guard) = self.resume_guard.take() {
                        guard.expire();
                    }
                    return Poll::Ready(Err(err));
                }
            };
            self.read_header = Some(header);
            self.input.clear();
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for EncryptionStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if dst.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.failed {
            return Poll::Ready(Err(invalid("encrypted stream is closed after failure")));
        }
        if this.offset == this.plain.len() && this.control.read_bypass_requested() {
            if !this.input.is_empty() || this.read_header.is_some() || this.prefix_len.is_some() {
                this.failed = true;
                return Poll::Ready(Err(invalid("encrypted bypass requested within a record")));
            }
            let mut bytes = [0; 4096];
            let n = bytes.len().min(dst.remaining());
            let mut buf = ReadBuf::new(&mut bytes[..n]);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut buf))?;
            let n = buf.filled().len();
            if let Err(error) = this.in_raw.apply(&mut this.in_mask, &mut bytes[..n], true) {
                this.failed = true;
                return Poll::Ready(Err(error));
            }
            dst.put_slice(&bytes[..n]);
            return Poll::Ready(Ok(()));
        }
        if this.offset == this.plain.len() {
            match ready!(this.read_record(cx)) {
                Ok(false) => return Poll::Ready(Ok(())),
                Ok(true) => {}
                Err(err) => {
                    this.failed = true;
                    return Poll::Ready(Err(err));
                }
            }
        }
        let n = dst.remaining().min(this.plain.len() - this.offset);
        dst.put_slice(&this.plain[this.offset..this.offset + n]);
        this.offset += n;
        Poll::Ready(Ok(()))
    }
}

mod traits;
