use std::io::{self, BufRead, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use x25519_dalek::{PublicKey, StaticSecret};

use super::reality_client_connection::{RealityClientConfig, RealityClientConnection};
use super::reality_server_connection::{RealityServerConfig, RealityServerConnection};
use super::reality_util::{decode_private_key, decode_public_key, decode_short_id, encode_key};
use zero_traits::TransportBypassControl;
use ztls::cipher::{CipherSuite, DEFAULT_CIPHER_SUITES};
use ztls::reader_writer::{RealityReader, RealityWriter};

mod handshake;
mod profile;
#[path = "stream/io.rs"]
mod stream_io;
use handshake::perform_reality_handshake;
pub(super) use handshake::{feed_reality_server_connection, perform_reality_server_handshake};
pub use profile::{
    generate_reality_key_pair, upgrade_reality_client, upgrade_reality_server,
    RealityClientOptions, RealityServerOptions, VlessRealityServerProfile,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsState {
    Stream,
    ReadShutdown,
    WriteShutdown,
    FullyShutdown,
}

impl TlsState {
    fn shutdown_read(&mut self) {
        *self = match *self {
            Self::WriteShutdown | Self::FullyShutdown => Self::FullyShutdown,
            _ => Self::ReadShutdown,
        };
    }

    fn shutdown_write(&mut self) {
        *self = match *self {
            Self::ReadShutdown | Self::FullyShutdown => Self::FullyShutdown,
            _ => Self::WriteShutdown,
        };
    }

    fn readable(self) -> bool {
        !matches!(self, Self::ReadShutdown | Self::FullyShutdown)
    }

    fn writeable(self) -> bool {
        !matches!(self, Self::WriteShutdown | Self::FullyShutdown)
    }
}

pub struct RealityTlsStream<IO> {
    io: IO,
    session: RealitySession,
    state: TlsState,
    need_flush: bool,
    transport_bypass_control: TransportBypassControl,
}

impl<IO> RealityTlsStream<IO> {
    pub fn server_name(&self) -> Option<&str> {
        match &self.session {
            RealitySession::Server(session) => session.server_name(),
            _ => None,
        }
    }
    pub fn negotiated_alpn(&self) -> Option<&str> {
        match &self.session {
            RealitySession::Server(session) => session.negotiated_alpn(),
            _ => None,
        }
    }
}

enum RealitySession {
    Client(RealityClientConnection),
    Server(RealityServerConnection),
}

impl RealitySession {
    fn wants_read(&self) -> bool {
        match self {
            Self::Client(session) => session.wants_read(),
            Self::Server(session) => session.wants_read(),
        }
    }

    fn wants_write(&self) -> bool {
        match self {
            Self::Client(session) => session.wants_write(),
            Self::Server(session) => session.wants_write(),
        }
    }

    fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize> {
        match self {
            Self::Client(session) => session.read_tls(rd),
            Self::Server(session) => session.read_tls(rd),
        }
    }

    fn next_tls_read_limit(&self) -> usize {
        match self {
            Self::Client(session) => session.next_tls_read_limit(),
            Self::Server(session) => session.next_tls_read_limit(),
        }
    }

    fn process_new_packets(&mut self) -> io::Result<()> {
        match self {
            Self::Client(session) => session.process_new_packets().map(|_| ()),
            Self::Server(session) => session.process_new_packets().map(|_| ()),
        }
    }

    fn reader(&mut self) -> RealityReader<'_> {
        match self {
            Self::Client(session) => session.reader(),
            Self::Server(session) => session.reader(),
        }
    }

    fn writer(&mut self) -> RealityWriter<'_> {
        match self {
            Self::Client(session) => session.writer(),
            Self::Server(session) => session.writer(),
        }
    }

    fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize> {
        match self {
            Self::Client(session) => session.write_tls(wr),
            Self::Server(session) => session.write_tls(wr),
        }
    }

    fn send_close_notify(&mut self) {
        match self {
            Self::Client(session) => session.send_close_notify(),
            Self::Server(session) => session.send_close_notify(),
        }
    }
}

impl<IO> RealityTlsStream<IO> {
    fn new(io: IO, session: RealityClientConnection) -> Self {
        debug_assert!(!session.is_handshaking());
        Self {
            io,
            session: RealitySession::Client(session),
            state: TlsState::Stream,
            need_flush: false,
            transport_bypass_control: TransportBypassControl::default(),
        }
    }

    pub(super) fn new_server(io: IO, session: RealityServerConnection) -> Self {
        debug_assert!(!session.is_handshaking());
        Self {
            io,
            session: RealitySession::Server(session),
            state: TlsState::Stream,
            need_flush: false,
            transport_bypass_control: TransportBypassControl::default(),
        }
    }

    pub fn transport_bypass_control(&self) -> TransportBypassControl {
        self.transport_bypass_control.clone()
    }
}

impl<IO> RealityTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn write_tls_direct(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let mut adapter = SyncWriteAdapter {
            io: &mut self.io,
            cx,
        };
        match self.session.write_tls(&mut adapter) {
            Ok(read) => Poll::Ready(Ok(read)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn drain_all_writes(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.session.wants_write() {
            match self.write_tls_direct(cx) {
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(_)) => self.need_flush = true,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }
}

struct SyncReadAdapter<'a, 'b, T> {
    io: &'a mut T,
    cx: &'a mut Context<'b>,
}

impl<T> io::Read for SyncReadAdapter<'_, '_, T>
where
    T: AsyncRead + Unpin,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_buf = ReadBuf::new(buf);
        match Pin::new(&mut self.io).poll_read(self.cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Ok(read_buf.filled().len()),
            Poll::Ready(Err(error)) => Err(error),
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}

struct SyncWriteAdapter<'a, 'b, T> {
    io: &'a mut T,
    cx: &'a mut Context<'b>,
}

impl<T> io::Write for SyncWriteAdapter<'_, '_, T>
where
    T: AsyncWrite + Unpin,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match Pin::new(&mut self.io).poll_write(self.cx, buf) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match Pin::new(&mut self.io).poll_flush(self.cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}
