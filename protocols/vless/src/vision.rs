use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncWrite, ReadBuf};
use zero_traits::TransportBypassControl;

mod padding;
mod stream_io;
mod tls;

use padding::vision_padding_len;
use tls::{contains_tls13_server_hello, starts_with_tls_application_data};

const COMMAND_CONTINUE: u8 = 0;
const COMMAND_END: u8 = 1;
const COMMAND_DIRECT: u8 = 2;
const FRAME_HEADER_LEN: usize = 5;
const UUID_LEN: usize = 16;
const MAX_CONTENT_LEN: usize = 16 * 1024 - UUID_LEN - FRAME_HEADER_LEN;
const MAX_FILTER_PACKETS: u8 = 8;

pub struct VisionStream<S> {
    inner: S,
    uuid: [u8; UUID_LEN],
    control: Option<TransportBypassControl>,
    read_wire: Vec<u8>,
    read_output: VecDeque<u8>,
    read_first_frame: bool,
    read_framing: bool,
    write_first_frame: bool,
    write_framing: bool,
    write_packets: u8,
    pending_write: Vec<u8>,
    pending_offset: usize,
    end_after_drain: bool,
    direct_after_drain: bool,
    tls_probe: Vec<u8>,
    tls13: bool,
}

impl<S> VisionStream<S> {
    pub fn new(inner: S, uuid: [u8; UUID_LEN], control: Option<TransportBypassControl>) -> Self {
        Self {
            inner,
            uuid,
            control,
            read_wire: Vec::new(),
            read_output: VecDeque::new(),
            read_first_frame: true,
            read_framing: true,
            write_first_frame: true,
            write_framing: true,
            write_packets: 0,
            pending_write: Vec::new(),
            pending_offset: 0,
            end_after_drain: false,
            direct_after_drain: false,
            tls_probe: Vec::new(),
            tls13: false,
        }
    }

    fn copy_read_output(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        if self.read_output.is_empty() || buf.remaining() == 0 {
            return false;
        }
        let len = buf.remaining().min(self.read_output.len());
        for _ in 0..len {
            if let Some(byte) = self.read_output.pop_front() {
                buf.put_slice(&[byte]);
            }
        }
        true
    }

    fn process_read_wire(&mut self) -> io::Result<()> {
        loop {
            if !self.read_framing {
                self.read_output.extend(self.read_wire.drain(..));
                return Ok(());
            }

            let prefix_len = if self.read_first_frame { UUID_LEN } else { 0 };
            if self.read_wire.len() < prefix_len + FRAME_HEADER_LEN {
                return Ok(());
            }

            if self.read_first_frame && self.read_wire[..UUID_LEN] != self.uuid {
                // Xray treats a non-Vision first body as an unframed stream.
                self.read_framing = false;
                continue;
            }

            let header = prefix_len;
            let command = self.read_wire[header];
            let content_len =
                u16::from_be_bytes([self.read_wire[header + 1], self.read_wire[header + 2]])
                    as usize;
            let padding_len =
                u16::from_be_bytes([self.read_wire[header + 3], self.read_wire[header + 4]])
                    as usize;
            if content_len > MAX_CONTENT_LEN || padding_len > 16 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid VLESS Vision frame length",
                ));
            }
            let total = prefix_len + FRAME_HEADER_LEN + content_len + padding_len;
            if self.read_wire.len() < total {
                return Ok(());
            }

            self.read_wire.drain(..prefix_len + FRAME_HEADER_LEN);
            let content: Vec<u8> = self.read_wire.drain(..content_len).collect();
            self.read_wire.drain(..padding_len);
            self.observe_tls(&content);
            self.read_output.extend(content);
            self.read_first_frame = false;

            match command {
                COMMAND_CONTINUE => {}
                COMMAND_END => self.read_framing = false,
                COMMAND_DIRECT => {
                    let Some(control) = &self.control else {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "VLESS Vision direct mode requires a switchable transport",
                        ));
                    };
                    control.request_read_bypass();
                    self.read_framing = false;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid VLESS Vision command",
                    ));
                }
            }
        }
    }

    fn observe_tls(&mut self, content: &[u8]) {
        if self.tls_probe.len() >= 64 * 1024 {
            return;
        }
        let remaining = 64 * 1024 - self.tls_probe.len();
        self.tls_probe
            .extend_from_slice(&content[..content.len().min(remaining)]);
        self.tls13 |= contains_tls13_server_hello(&self.tls_probe);
    }

    fn choose_command(&mut self, content: &[u8]) -> u8 {
        self.write_packets = self.write_packets.saturating_add(1);
        self.observe_tls(content);
        if starts_with_tls_application_data(content) {
            if self.tls13 && self.control.is_some() {
                return COMMAND_DIRECT;
            }
            return COMMAND_END;
        }
        if self.write_packets >= MAX_FILTER_PACKETS {
            COMMAND_END
        } else {
            COMMAND_CONTINUE
        }
    }

    fn encode_write_frame(&mut self, content: &[u8]) -> usize {
        let consumed = content.len().min(MAX_CONTENT_LEN);
        let content = &content[..consumed];
        let command = self.choose_command(content);
        let padding_len =
            vision_padding_len(content.len(), self.write_packets <= 1, MAX_CONTENT_LEN);
        let prefix_len = if self.write_first_frame { UUID_LEN } else { 0 };
        self.pending_write
            .reserve(prefix_len + FRAME_HEADER_LEN + consumed + padding_len);
        if self.write_first_frame {
            self.pending_write.extend_from_slice(&self.uuid);
            self.write_first_frame = false;
        }
        self.pending_write.push(command);
        self.pending_write
            .extend_from_slice(&(consumed as u16).to_be_bytes());
        self.pending_write
            .extend_from_slice(&(padding_len as u16).to_be_bytes());
        self.pending_write.extend_from_slice(content);
        self.pending_write
            .resize(self.pending_write.len() + padding_len, 0);
        self.end_after_drain = command == COMMAND_END;
        self.direct_after_drain = command == COMMAND_DIRECT;
        consumed
    }
}

impl<S> VisionStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_drain_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pending_offset < self.pending_write.len() {
            match Pin::new(&mut self.inner)
                .poll_write(cx, &self.pending_write[self.pending_offset..])
            {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(written)) => self.pending_offset += written,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.pending_write.clear();
        self.pending_offset = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_finish_transition(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.end_after_drain && !self.direct_after_drain {
            return Poll::Ready(Ok(()));
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                if self.direct_after_drain {
                    let Some(control) = &self.control else {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "VLESS Vision direct mode requires a switchable transport",
                        )));
                    };
                    control.request_write_bypass();
                }
                self.end_after_drain = false;
                self.direct_after_drain = false;
                self.write_framing = false;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}
