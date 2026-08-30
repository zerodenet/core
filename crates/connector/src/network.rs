use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

pub trait EventSinkTcpStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T> EventSinkTcpStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub type EventSinkTcpConnectFuture =
    Pin<Box<dyn Future<Output = io::Result<Box<dyn EventSinkTcpStream>>> + Send + 'static>>;

/// Narrow network boundary used by event delivery transports.
///
/// The standalone connector crate can use [`EventDispatcherNetwork::system`].
/// The Zero application injects its shared TUN underlay-aware dialer instead so
/// webhook sockets cannot re-enter the data plane.
pub trait EventSinkTcpDialer: Send + Sync + 'static {
    fn connect(&self, host: String, port: u16) -> EventSinkTcpConnectFuture;
}

#[derive(Clone)]
pub struct EventDispatcherNetwork {
    #[cfg_attr(not(feature = "webhook"), allow(dead_code))]
    dialer: Arc<dyn EventSinkTcpDialer>,
}

impl EventDispatcherNetwork {
    pub fn new(dialer: Arc<dyn EventSinkTcpDialer>) -> Self {
        Self { dialer }
    }

    /// Explicit system-route network for standalone embedding and tests.
    ///
    /// The Zero binary does not use this path: it injects the proxy's shared
    /// egress authority with `EventDispatcherNetwork::new`.
    pub fn system() -> Self {
        Self::new(Arc::new(SystemEventSinkTcpDialer))
    }

    #[cfg(feature = "webhook")]
    pub(crate) fn dialer(&self) -> Arc<dyn EventSinkTcpDialer> {
        self.dialer.clone()
    }
}

impl std::fmt::Debug for EventDispatcherNetwork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventDispatcherNetwork")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct SystemEventSinkTcpDialer;

impl EventSinkTcpDialer for SystemEventSinkTcpDialer {
    fn connect(&self, host: String, port: u16) -> EventSinkTcpConnectFuture {
        Box::pin(async move {
            let stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
            Ok(Box::new(stream) as Box<dyn EventSinkTcpStream>)
        })
    }
}
