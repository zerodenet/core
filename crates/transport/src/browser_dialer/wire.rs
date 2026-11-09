//! Admission in front of the existing private tungstenite codec. It buffers
//! only a fixed RFC6455 header, never a second message, parser or socket pool.
use super::memory::{Memory, Wait};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::PrefixedSocket;

pub(super) struct Socket {
    io: PrefixedSocket,
    http: usize,
    header: [u8; 14],
    collected: usize,
    delivered: usize,
    required: usize,
    payload: u64,
    fragmented: usize,
    largest_frame: usize,
    largest_message: usize,
    wait: Mutex<Option<Wait>>,
    // The actual underlying prefix/socket is freed before accounting. The
    // caller also retains this exact credit after its outer message buffer.
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    _cancel_header: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    memory: Memory,
}
impl Socket {
    pub(super) fn new(
        io: PrefixedSocket,
        http: usize,
        memory: Memory,
        cancel: tokio_util::sync::CancellationToken,
    ) -> io::Result<Self> {
        let (cancelled, header) = memory.cancellation(cancel)?;
        Ok(Self {
            io,
            http,
            header: [0; 14],
            collected: 0,
            delivered: 0,
            required: 2,
            payload: 0,
            fragmented: 0,
            largest_frame: 0,
            largest_message: 0,
            wait: Mutex::new(None),
            cancelled,
            _cancel_header: header,
            memory,
        })
    }
    fn poll_cancelled(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Browser Dialer retired",
            ))
        } else {
            Ok(())
        }
    }
    fn poll_credit(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Unique &mut Socket means no mutex guard/callback is held while the
        // actual root admission future is polled or freed.
        let Some(mut wait) = self
            .wait
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return Poll::Ready(Ok(()));
        };
        match wait.future.as_mut().poll(cx) {
            Poll::Pending => {
                *self
                    .wait
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(wait);
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(result),
        }
    }
    fn admit_header(&mut self) -> io::Result<()> {
        let code = self.header[0] & 0x0f;
        let marker = self.header[1] & 0x7f;
        let length = match marker {
            126 => u16::from_be_bytes([self.header[2], self.header[3]]) as u64,
            127 => u64::from_be_bytes(self.header[2..10].try_into().unwrap()),
            _ => marker as u64,
        };
        self.payload = length;
        // Match the unchanged pinned tungstenite defaults. Invalid over-limit
        // frames are forwarded to its original validator, which rejects the
        // header before reading/allocating its payload.
        if length > 16 * 1024 * 1024 || (code & 8 != 0 && length > 125) {
            return Ok(());
        }
        let length = length as usize;
        let message = match code {
            1 | 2 => length,
            0 => self
                .fragmented
                .checked_add(length)
                .ok_or(io::ErrorKind::InvalidData)?,
            _ => 0,
        };
        if message > 64 * 1024 * 1024 {
            return Ok(());
        }
        if code <= 2 {
            self.fragmented = if self.header[0] & 0x80 == 0 {
                message
            } else {
                0
            };
        }
        self.largest_frame = self.largest_frame.max(length);
        self.largest_message = self.largest_message.max(message);
        // Pinned codec: geometric input-buffer old+new, extracted frame,
        // incomplete text/binary collector old+new, previous outer read buffer
        // and final output can coexist. This is an on-demand backing peak;
        // protocol maxima are never eagerly reserved per idle browser.
        let peak = self
            .largest_frame
            .checked_add(self.largest_message)
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or(io::ErrorKind::InvalidData)?;
        *self
            .wait
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            self.memory.receive_growth(peak)?;
        Ok(())
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_cancelled(cx)?;
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = &mut *self;
        loop {
            std::task::ready!(this.poll_credit(cx))?;
            if this.http != 0 {
                let max = output.remaining().min(this.http);
                output.initialize_unfilled_to(max);
                let mut bounded = output.take(max);
                std::task::ready!(Pin::new(&mut this.io).poll_read(cx, &mut bounded))?;
                let size = bounded.filled().len();
                output.advance(size);
                this.http -= size;
                return Poll::Ready(Ok(()));
            }
            if this.delivered < this.collected && this.collected == this.required {
                let length = (this.collected - this.delivered).min(output.remaining());
                output.put_slice(&this.header[this.delivered..this.delivered + length]);
                this.delivered += length;
                return Poll::Ready(Ok(()));
            }
            if this.delivered == this.required {
                if this.payload != 0 {
                    let max = usize::try_from(this.payload)
                        .unwrap_or(usize::MAX)
                        .min(output.remaining());
                    output.initialize_unfilled_to(max);
                    let mut bounded = output.take(max);
                    std::task::ready!(Pin::new(&mut this.io).poll_read(cx, &mut bounded))?;
                    let size = bounded.filled().len();
                    output.advance(size);
                    this.payload -= size as u64;
                    return Poll::Ready(Ok(()));
                }
                this.collected = 0;
                this.delivered = 0;
                this.required = 2;
            }
            let mut header = ReadBuf::new(&mut this.header[this.collected..this.required]);
            std::task::ready!(Pin::new(&mut this.io).poll_read(cx, &mut header))?;
            let size = header.filled().len();
            if size == 0 {
                return Poll::Ready(Ok(()));
            }
            this.collected += size;
            if this.collected < this.required {
                continue;
            }
            if this.required == 2 {
                let extra = match this.header[1] & 0x7f {
                    126 => 2,
                    127 => 8,
                    _ => 0,
                };
                this.required = 2 + extra + if this.header[1] & 0x80 != 0 { 4 } else { 0 };
                if this.collected < this.required {
                    continue;
                }
            }
            this.admit_header()?;
        }
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_cancelled(cx)?;
        std::task::ready!(self.poll_credit(cx))?;
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_cancelled(cx)?;
        std::task::ready!(self.poll_credit(cx))?;
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
