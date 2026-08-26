use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{lookup_host, TcpListener as TokioTcpListener, TcpSocket, TcpStream, UdpSocket};
use zero_traits::{
    AsyncSocket, DatagramSocket as DatagramSocketTrait, DnsResolver, IpAddress, SocketAddress,
    TcpListener as TcpListenerTrait, TransportBypassControl,
};

mod egress;
mod process;
use egress::{bind_tcp_to_interface, bind_udp_to_interface, datagram_bind_address};
pub use egress::{
    EgressBindingReason, EgressInterface, EgressInterfaceControl, EgressRouteLookupStatus,
    EgressSelection,
};
pub use process::{lookup_local_tcp_process, lookup_local_udp_process, LocalProcessInfo};

#[derive(Debug)]
pub struct TokioSocket {
    inner: TcpStream,
    egress_interface: Option<EgressInterface>,
}

#[derive(Debug)]
pub struct TcpConnectError {
    stage: &'static str,
    interface_bound: bool,
    local_addr: Option<SocketAddr>,
    error: io::Error,
}

impl TcpConnectError {
    pub fn stage(&self) -> &'static str {
        self.stage
    }

    pub fn interface_bound(&self) -> bool {
        self.interface_bound
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    pub fn error(&self) -> &io::Error {
        &self.error
    }

    pub fn into_inner(self) -> io::Error {
        self.error
    }
}

impl TokioSocket {
    pub fn new(inner: TcpStream) -> Self {
        Self {
            inner,
            egress_interface: None,
        }
    }

    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self::new(stream))
    }

    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        Self::connect_addr_on(addr, None).await
    }

    /// Connect while forcing the socket onto a physical egress interface.
    ///
    /// TUN runtimes use this before installing a default route through the
    /// tunnel so Zero's own upstream and direct connections cannot re-enter
    /// the TUN device.
    pub async fn connect_addr_on(
        addr: SocketAddr,
        interface: Option<&EgressInterface>,
    ) -> io::Result<Self> {
        Self::connect_addr_on_observed(addr, interface)
            .await
            .map_err(TcpConnectError::into_inner)
    }

    pub async fn connect_addr_on_observed(
        addr: SocketAddr,
        interface: Option<&EgressInterface>,
    ) -> Result<Self, TcpConnectError> {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|error| TcpConnectError {
            stage: "create_socket",
            interface_bound: false,
            local_addr: None,
            error,
        })?;
        let interface = interface.filter(|_| !addr.ip().is_loopback());
        if let Some(interface) = interface {
            bind_tcp_to_interface(&socket, addr, interface).map_err(|error| TcpConnectError {
                stage: "bind_interface",
                interface_bound: false,
                local_addr: socket.local_addr().ok(),
                error,
            })?;
        }
        let interface_bound = interface.is_some();
        let local_addr = socket.local_addr().ok();
        let stream = socket
            .connect(addr)
            .await
            .map_err(|error| TcpConnectError {
                stage: "connect_socket",
                interface_bound,
                local_addr,
                error,
            })?;
        let local_addr = stream.local_addr().ok();
        stream.set_nodelay(true).map_err(|error| TcpConnectError {
            stage: "configure_socket",
            interface_bound,
            local_addr,
            error,
        })?;
        Ok(Self {
            inner: stream,
            egress_interface: interface.cloned(),
        })
    }

    pub fn into_inner(self) -> TcpStream {
        self.inner
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Physical interface selected when this outbound socket was created.
    /// Transport layers that open companion sockets must preserve it.
    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.egress_interface.as_ref()
    }
}

impl AsyncSocket for TokioSocket {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.inner.read(buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.inner.shutdown().await
    }
}

impl AsyncRead for TokioSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TokioSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct PrefixedSocket {
    prefix: Vec<u8>,
    offset: usize,
    inner: TokioSocket,
}

impl PrefixedSocket {
    pub fn from_prefix(inner: TokioSocket, prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            offset: 0,
            inner,
        }
    }

    pub fn from_byte(inner: TokioSocket, first: u8) -> Self {
        Self::from_prefix(inner, vec![first])
    }
}

impl ClientStream for PrefixedSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
}

impl AsyncSocket for PrefixedSocket {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if self.offset < self.prefix.len() {
            let available = self.prefix.len() - self.offset;
            let to_copy = available.min(buf.len());
            buf[..to_copy].copy_from_slice(&self.prefix[self.offset..self.offset + to_copy]);
            self.offset += to_copy;
            return Ok(to_copy);
        }

        AsyncSocket::read(&mut self.inner, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncSocket::write_all(&mut self.inner, buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncSocket::shutdown(&mut self.inner).await
    }
}

impl AsyncRead for PrefixedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset < self.prefix.len() {
            let available = self.prefix.len() - self.offset;
            let to_copy = available.min(buf.remaining());
            if to_copy > 0 {
                let start = self.offset;
                let end = start + to_copy;
                buf.put_slice(&self.prefix[start..end]);
                self.offset = end;
            }
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct TokioListener {
    inner: TokioTcpListener,
}

impl TokioListener {
    pub async fn bind(addr: &str) -> io::Result<Self> {
        TokioTcpListener::bind(addr)
            .await
            .map(|inner| Self { inner })
    }

    pub async fn accept(&self) -> io::Result<(TokioSocket, Option<SocketAddress>)> {
        <Self as TcpListenerTrait>::accept(self).await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl TcpListenerTrait for TokioListener {
    type Stream = TokioSocket;
    type Error = io::Error;

    async fn accept(&self) -> Result<(Self::Stream, Option<SocketAddress>), Self::Error> {
        let (stream, remote_addr) = self.inner.accept().await?;

        Ok((
            TokioSocket::new(stream),
            Some(socket_addr_to_socket_address(remote_addr)),
        ))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TokioResolver;

impl DnsResolver for TokioResolver {
    type Error = io::Error;

    async fn resolve(&self, domain: &str) -> Result<Vec<IpAddress>, Self::Error> {
        let mut resolved = Vec::new();

        for addr in lookup_host((domain, 0)).await? {
            resolved.push(ip_addr_to_ip(addr.ip()));
        }

        Ok(resolved)
    }
}

pub fn socket_addr_to_ip(addr: SocketAddr) -> IpAddress {
    ip_addr_to_ip(addr.ip())
}

pub fn socket_addr_to_socket_address(addr: SocketAddr) -> SocketAddress {
    SocketAddress::new(socket_addr_to_ip(addr), addr.port())
}

pub fn socket_address_to_socket_addr(addr: SocketAddress) -> SocketAddr {
    socket_addr_from_ip(addr.ip, addr.port)
}

fn ip_addr_to_ip(addr: IpAddr) -> IpAddress {
    match addr {
        IpAddr::V4(addr) => IpAddress::V4(addr.octets()),
        IpAddr::V6(addr) => IpAddress::V6(addr.octets()),
    }
}

pub async fn relay_bidirectional(left: TokioSocket, right: TokioSocket) -> io::Result<(u64, u64)> {
    let mut left = left.into_inner();
    let mut right = right.into_inner();

    copy_bidirectional(&mut left, &mut right).await
}

#[derive(Debug)]
pub struct TokioDatagramSocket {
    inner: UdpSocket,
    egress_interface: Option<EgressInterface>,
}

impl TokioDatagramSocket {
    pub async fn bind(addr: &str) -> io::Result<Self> {
        UdpSocket::bind(addr).await.map(|inner| Self {
            inner,
            egress_interface: None,
        })
    }

    pub async fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        UdpSocket::bind(addr).await.map(|inner| Self {
            inner,
            egress_interface: None,
        })
    }

    /// Bind a datagram socket and force its packets onto a physical egress
    /// interface when one is selected by the active TUN route transaction.
    pub async fn bind_addr_on(
        addr: SocketAddr,
        interface: Option<&EgressInterface>,
    ) -> io::Result<Self> {
        if interface.is_none() {
            return Self::bind_addr(addr).await;
        }
        let interface = interface.expect("checked above");
        let socket = std::net::UdpSocket::bind(addr)?;
        bind_udp_to_interface(&socket, addr, interface)?;
        socket.set_nonblocking(true)?;
        UdpSocket::from_std(socket).map(|inner| Self {
            inner,
            egress_interface: Some(interface.clone()),
        })
    }

    /// Bind a datagram socket for a specific peer, applying the physical
    /// egress only for non-loopback traffic.
    pub async fn bind_for_peer_on(
        peer: SocketAddr,
        interface: Option<&EgressInterface>,
    ) -> io::Result<Self> {
        Self::bind_for_peer_on_with_port(peer, interface, None).await
    }

    /// Bind for a peer while attempting to retain a caller-owned UDP source
    /// port. Address conflicts fall back to an ephemeral port; interface
    /// binding failures remain fatal.
    pub async fn bind_for_peer_on_with_port(
        peer: SocketAddr,
        interface: Option<&EgressInterface>,
        preferred_port: Option<u16>,
    ) -> io::Result<Self> {
        let egress_interface = interface.filter(|_| !peer.ip().is_loopback()).cloned();
        let socket = bind_std_datagram_socket_for_peer_with_port(peer, interface, preferred_port)?;
        UdpSocket::from_std(socket).map(|inner| Self {
            inner,
            egress_interface,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.egress_interface.as_ref()
    }

    pub async fn recv_from_addr(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    pub async fn send_to_addr(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.inner.send_to(buf, addr).await
    }
}

/// Create a non-blocking standard UDP socket suitable for Tokio or QUIC and
/// bind it to the selected physical egress for `peer`.
pub fn bind_std_datagram_socket_for_peer(
    peer: SocketAddr,
    interface: Option<&EgressInterface>,
) -> io::Result<std::net::UdpSocket> {
    bind_std_datagram_socket_for_peer_with_port(peer, interface, None)
}

pub fn bind_std_datagram_socket_for_peer_with_port(
    peer: SocketAddr,
    interface: Option<&EgressInterface>,
    preferred_port: Option<u16>,
) -> io::Result<std::net::UdpSocket> {
    let mut local = datagram_bind_address(peer, interface)?;
    local.set_port(preferred_port.unwrap_or(0));
    let socket = match std::net::UdpSocket::bind(local) {
        Ok(socket) => socket,
        Err(_) if preferred_port.is_some() => {
            local.set_port(0);
            std::net::UdpSocket::bind(local)?
        }
        Err(error) => return Err(error),
    };
    if let Some(interface) = interface.filter(|_| !peer.ip().is_loopback()) {
        bind_udp_to_interface(&socket, local, interface)?;
    }
    socket.set_nonblocking(true)?;
    Ok(socket)
}

impl DatagramSocketTrait for TokioDatagramSocket {
    type Error = io::Error;

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, IpAddress, u16), Self::Error> {
        let (read, addr) = self.inner.recv_from(buf).await?;
        Ok((read, ip_addr_to_ip(addr.ip()), addr.port()))
    }

    async fn send_to(&self, buf: &[u8], addr: IpAddress, port: u16) -> Result<(), Self::Error> {
        self.inner
            .send_to(buf, socket_addr_from_ip(addr, port))
            .await
            .map(|_| ())
    }
}

fn socket_addr_from_ip(ip: IpAddress, port: u16) -> SocketAddr {
    match ip {
        IpAddress::V4(bytes) => SocketAddr::new(IpAddr::V4(bytes.into()), port),
        IpAddress::V6(bytes) => SocketAddr::new(IpAddr::V6(bytes.into()), port),
    }
}

// ── ClientStream & TcpRelayStream ──

/// A bidirectional client stream that can report its local address.
pub trait ClientStream:
    AsyncSocket<Error = io::Error> + AsyncRead + AsyncWrite + Send + Sync + Unpin
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ClientStream: local_addr not available",
        ))
    }

    /// The remote (peer) socket address, if available.
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ClientStream: peer_addr not available",
        ))
    }
}

impl ClientStream for TokioSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}

/// Type-erased bidirectional relay stream.
///
/// Wraps any `AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static` stream
/// behind a `Box<dyn>` so callers can return different concrete stream types
/// from the same function.
pub struct TcpRelayStream {
    inner: Box<dyn RelayIo>,
    local_addr: Option<SocketAddr>,
    transport_bypass_control: Option<TransportBypassControl>,
}

trait RelayIo: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

impl<T> RelayIo for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

impl TcpRelayStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        Self {
            inner: Box::new(stream),
            local_addr: None,
            transport_bypass_control: None,
        }
    }

    pub fn with_local_addr<S>(stream: S, addr: SocketAddr) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        Self {
            inner: Box::new(stream),
            local_addr: Some(addr),
            transport_bypass_control: None,
        }
    }

    pub fn with_transport_bypass_control<S>(stream: S, control: TransportBypassControl) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        Self {
            inner: Box::new(stream),
            local_addr: None,
            transport_bypass_control: Some(control),
        }
    }
}

impl From<TokioSocket> for TcpRelayStream {
    fn from(socket: TokioSocket) -> Self {
        match socket.local_addr() {
            Ok(addr) => Self::with_local_addr(socket, addr),
            Err(_) => Self::new(socket),
        }
    }
}

impl ClientStream for TcpRelayStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "local_addr not available"))
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ClientStream: peer_addr not available",
        ))
    }
}

impl AsyncSocket for TcpRelayStream {
    type Error = io::Error;

    fn transport_bypass_control(&self) -> Option<TransportBypassControl> {
        self.transport_bypass_control.clone()
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(&mut self.inner, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(&mut self.inner, buf).await?;
        AsyncWriteExt::flush(&mut self.inner).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(&mut self.inner).await
    }
}

impl AsyncRead for TcpRelayStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpRelayStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Carrier established by relay prefix hops, consumed by the final-hop transport builder.
pub struct RelayCarrier {
    pub stream: TcpRelayStream,
    pub server: String,
    pub port: u16,
}

// ── TransportConnector trait ──

/// Establishes transport-layer connections over raw TCP sockets.
///
/// Protocol crates implement this to wrap a connected [`TokioSocket`] with
/// their transport (TLS, WebSocket, gRPC, H2, etc.).  Callers inject the
/// transport configuration at construction time and call
/// [`TransportConnector::connect`] for each socket.
#[allow(async_fn_in_trait)]
pub trait TransportConnector: Send + Sync {
    /// The concrete bidirectional stream type, e.g. [`TcpRelayStream`].
    type Stream;

    /// Wrap `socket` with the transport layer and return a stream
    /// connected to `server:port`.
    async fn connect(
        &self,
        socket: TokioSocket,
        server: &str,
        port: u16,
    ) -> io::Result<Self::Stream>;
}

/// Resolves a host and establishes a raw TCP connection.
///
/// Used by connection pools and protocol handlers to obtain connected
/// [`TokioSocket`]s for further transport wrapping.
#[allow(async_fn_in_trait)]
pub trait TcpConnector: Send + Sync {
    /// Resolve `host` and connect to `host:port`, returning a connected socket.
    async fn connect(&self, host: &str, port: u16) -> io::Result<TokioSocket>;
}

// ── Cross-crate trait impls ──
