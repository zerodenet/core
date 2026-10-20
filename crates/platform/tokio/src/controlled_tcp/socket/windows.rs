//! Persistent ownership across Winsock connect and Tokio registration.
use crate::TokioSocket;
#[cfg(test)]
use std::os::windows::io::RawSocket;
use std::{
    io,
    net::{SocketAddr, TcpStream},
    os::windows::io::AsRawSocket,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use zero_traits::IoObserver;
mod unknown;
use unknown::ConsumedUnknown;

enum Status {
    Raw(TcpStream),
    Registered(TokioSocket),
    ConsumedUnknown(ConsumedUnknown),
}
pub(in crate::controlled_tcp) struct NativeSocket {
    status: Status,
    pub(in crate::controlled_tcp) interface: Option<crate::EgressInterface>,
    observer: Option<Arc<dyn IoObserver>>,
    last_local: Option<SocketAddr>,
}
impl NativeSocket {
    pub(in crate::controlled_tcp) fn new(
        socket: TcpStream,
        interface: Option<crate::EgressInterface>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Self {
        if let Some(observer) = &observer {
            observer.stream_boundary();
        }
        Self {
            last_local: socket.local_addr().ok(),
            status: Status::Raw(socket),
            interface,
            observer,
        }
    }
    pub(in crate::controlled_tcp) fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.status {
            Status::Raw(socket) => socket.local_addr(),
            Status::Registered(socket) => socket.local_addr(),
            Status::ConsumedUnknown(source) => Err(source.error()),
        }
    }
    pub(in crate::controlled_tcp) fn observed_local_addr(&self) -> Option<SocketAddr> {
        self.local_addr().ok().or(self.last_local)
    }
    pub(in crate::controlled_tcp) fn peer_addr(&self) -> io::Result<SocketAddr> {
        match &self.status {
            Status::Raw(socket) => socket.peer_addr(),
            Status::Registered(socket) => socket.peer_addr(),
            Status::ConsumedUnknown(source) => Err(source.error()),
        }
    }
    pub(in crate::controlled_tcp) fn begin_connect(
        &mut self,
        peer: SocketAddr,
    ) -> Result<(), (&'static str, io::Error)> {
        let Status::Raw(socket) = &self.status else {
            return Err((
                "connect_socket",
                io::Error::new(io::ErrorKind::AlreadyExists, "TCP connect already started"),
            ));
        };
        // Same pinned socket2/WinSock connect and WouldBlock rule as Tokio's
        // TcpSocket::connect and Mio Windows. No ConnectEx or second connect.
        if let Err(source) = socket2::SockRef::from(socket).connect(&peer.into()) {
            if source.kind() != io::ErrorKind::WouldBlock {
                return Err(("connect_socket", source));
            }
        }
        self.last_local = socket.local_addr().ok().or(self.last_local);
        let identity = socket.as_raw_socket();
        // from_std consumes the original even when registration fails. Install
        // the unknown marker BEFORE conversion; it claims no live ownership.
        let marker = ConsumedUnknown::new(
            identity,
            io::Error::other("TCP registration did not return an observed result"),
        );
        let Status::Raw(socket) =
            std::mem::replace(&mut self.status, Status::ConsumedUnknown(marker))
        else {
            unreachable!("raw socket checked")
        };
        match tokio::net::TcpStream::from_std(socket) {
            Ok(stream) => {
                let mut socket = TokioSocket::new(stream);
                socket.egress_interface = self.interface.clone();
                // The actual socket boundary was observed once at preparation.
                socket.observer = self.observer.take();
                self.status = Status::Registered(socket);
                Ok(())
            }
            Err(source) => {
                let unknown = ConsumedUnknown::new(identity, source);
                let result = unknown.error();
                self.status = Status::ConsumedUnknown(unknown);
                Err(("register_socket", result))
            }
        }
    }
    pub(in crate::controlled_tcp) fn poll_connect(
        &self,
        cx: &mut Context<'_>,
        peer: SocketAddr,
    ) -> Poll<io::Result<()>> {
        let socket = match self.registered() {
            Ok(socket) => socket,
            Err(source) => return Poll::Ready(Err(source)),
        };
        loop {
            match socket.inner.poll_write_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                Poll::Ready(Ok(())) => {}
            }
            match socket.inner.try_io(Interest::WRITABLE, || {
                let native = socket2::SockRef::from(&socket.inner);
                if let Some(source) = native.take_error()? {
                    return Err(source);
                }
                let actual = match native.peer_addr() {
                    Ok(actual) => actual
                        .as_socket()
                        .ok_or_else(|| io::Error::other("TCP peer address family is invalid"))?,
                    Err(source)
                        if source.kind() == io::ErrorKind::NotConnected
                            || source.kind() == io::ErrorKind::WouldBlock =>
                    {
                        return Err(io::Error::new(io::ErrorKind::WouldBlock, source))
                    }
                    Err(source) => return Err(source),
                };
                let same_scope = match (actual, peer) {
                    (SocketAddr::V6(actual), SocketAddr::V6(peer)) => {
                        peer.scope_id() == 0 || actual.scope_id() == peer.scope_id()
                    }
                    _ => true,
                };
                if actual.ip() != peer.ip() || actual.port() != peer.port() || !same_scope {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TCP connected to an unexpected peer",
                    ));
                }
                Ok(())
            }) {
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
        }
    }
    fn registered(&self) -> io::Result<&TokioSocket> {
        match &self.status {
            Status::Registered(socket) => Ok(socket),
            Status::ConsumedUnknown(source) => Err(source.error()),
            Status::Raw(_) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TCP connect has not started",
            )),
        }
    }
    fn registered_mut(&mut self) -> io::Result<&mut TokioSocket> {
        match &mut self.status {
            Status::Registered(socket) => Ok(socket),
            Status::ConsumedUnknown(source) => Err(source.error()),
            Status::Raw(_) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TCP connect has not started",
            )),
        }
    }
    pub(in crate::controlled_tcp) fn set_nodelay(&self) -> io::Result<()> {
        self.registered()?.inner.set_nodelay(true)
    }
    pub(in crate::controlled_tcp) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.registered_mut() {
            Ok(socket) => Pin::new(socket).poll_read(cx, buffer),
            Err(source) => Poll::Ready(Err(source)),
        }
    }
    pub(in crate::controlled_tcp) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.registered_mut() {
            Ok(socket) => Pin::new(socket).poll_write(cx, bytes),
            Err(source) => Poll::Ready(Err(source)),
        }
    }
    pub(in crate::controlled_tcp) fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.registered_mut() {
            Ok(socket) => Pin::new(socket).poll_shutdown(cx),
            Err(source) => Poll::Ready(Err(source)),
        }
    }
    pub(in crate::controlled_tcp) fn close(self) -> io::Result<()> {
        match self.status {
            Status::Raw(socket) => super::super::native::native_close(socket),
            Status::Registered(socket) => socket.close(),
            Status::ConsumedUnknown(source) => Err(source.error()),
        }
    }
    #[cfg(test)]
    pub(in crate::controlled_tcp) fn original_socket(&self) -> io::Result<RawSocket> {
        match &self.status {
            Status::Raw(socket) => Ok(socket.as_raw_socket()),
            Status::Registered(socket) => Ok(socket.inner.as_raw_socket()),
            Status::ConsumedUnknown(source) => Err(source.error()),
        }
    }
}
#[cfg(test)]
#[path = "../../../tests/controlled_tcp/connect_windows/unknown.rs"]
mod tests;
