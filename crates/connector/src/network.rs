use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
pub use zero_platform_tokio::contract::network::EventNetworkLimits;

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

    /// Concrete application owners install only cold metadata after their
    /// previous actual Epoch retired. Bounds grant no native IO or task rights.
    fn configure_limits(&self, _limits: EventNetworkLimits) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "standalone network has no original budget owner",
        ))
    }
    /// Positive original native/user/Scope retirement; an error/EOF/Drop is not
    /// proof. A dialer without retained originals cannot supply this fact.
    fn retirement_complete(&self) -> bool {
        false
    }

    /// Application owners reject a new generation until the original one closed.
    fn begin_generation(&self) -> io::Result<()> {
        Ok(())
    }
    fn retire_generation(
        &self,
        _deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "standalone event network has no original close authority",
            ))
        })
    }
    fn prepare_request(
        &self,
        _deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
    /// The application overrides this with its retained original TaskScope.
    fn spawn_event_task(
        &self,
        task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> io::Result<()> {
        tokio::spawn(task);
        Ok(())
    }
    /// Driven by the dispatcher caller, outside the tasks being supervised.
    fn drive_event_tasks(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending())
    }
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

    pub fn configure_limits(&self, limits: EventNetworkLimits) -> io::Result<()> {
        self.dialer.configure_limits(limits)
    }
    pub fn retirement_complete(&self) -> bool {
        self.dialer.retirement_complete()
    }
    pub fn begin_generation(&self) -> io::Result<()> {
        self.dialer.begin_generation()
    }

    pub async fn retire_generation(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.dialer.retire_generation(deadline).await
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
