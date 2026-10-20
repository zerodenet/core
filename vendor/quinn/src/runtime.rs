use std::{
    fmt::Debug,
    future::Future,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::Instrument;
use udp::{RecvMeta, Transmit};

use crate::{ConnectionError, Instant};

/// The actual Quinn driver creation site, before its result is erased.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverOrigin {
    /// The endpoint owns socket reception and connection-event dispatch.
    Endpoint,
    /// One connection owns its protocol driver.
    Connection,
}

/// The original result of one Quinn driver.
///
/// This is a body result, not evidence that an owning runtime has joined its task
/// or that an application has received a connection failure.
#[derive(Debug)]
pub enum DriverExit {
    /// The endpoint driver's original I/O result.
    Endpoint(io::Result<()>),
    /// The connection driver's original I/O result and protocol close reason.
    Connection {
        /// An original I/O failure, if the driver returned one.
        result: io::Result<()>,
        /// The typed reason retained by this same connection when its driver exits.
        close_reason: Option<ConnectionError>,
    },
}

/// A driver future with its actual result preserved for an owning runtime.
pub type DriverFuture = Pin<Box<dyn Future<Output = DriverExit> + Send>>;

/// Abstracts I/O and timer operations for runtime independence
pub trait Runtime: Send + Sync + Debug + 'static {
    /// Construct a timer that will expire at `i`
    fn new_timer(&self, i: Instant) -> Pin<Box<dyn AsyncTimer>>;
    /// Drive `future` to completion in the background
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>);
    /// Drive one actual Quinn driver, preserving its origin and result.
    ///
    /// The default keeps the legacy background-spawn and I/O-error logging
    /// behavior. Owning runtimes may override this to register the actual driver
    /// with their existing task owner. Completion alone is not a task join or a
    /// receipt that a caller has accepted the failure.
    fn spawn_driver(&self, _origin: DriverOrigin, future: DriverFuture) {
        self.spawn(Box::pin(
            async move {
                match future.await {
                    DriverExit::Endpoint(result) => {
                        if let Err(e) = result {
                            tracing::error!("I/O error: {}", e);
                        }
                    }
                    DriverExit::Connection { result, .. } => {
                        if let Err(e) = result {
                            tracing::error!("I/O error: {e}");
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        ));
    }
    /// Convert `t` into the socket type used by this runtime
    #[cfg(not(wasm_browser))]
    fn wrap_udp_socket(&self, t: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>>;
    /// Look up the current time
    ///
    /// Allows simulating the flow of time for testing.
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Abstract implementation of an async timer for runtime independence
pub trait AsyncTimer: Send + Debug + 'static {
    /// Update the timer to expire at `i`
    fn reset(self: Pin<&mut Self>, i: Instant);
    /// Check whether the timer has expired, and register to be woken if not
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()>;
}

/// Abstract implementation of a UDP socket for runtime independence
pub trait AsyncUdpSocket: Send + Sync + Debug + 'static {
    /// Create a [`UdpPoller`] that can register a single task for write-readiness notifications
    ///
    /// A `poll_send` method on a single object can usually store only one [`Waker`] at a time,
    /// i.e. allow at most one caller to wait for an event. This method allows any number of
    /// interested tasks to construct their own [`UdpPoller`] object. They can all then wait for the
    /// same event and be notified concurrently, because each [`UdpPoller`] can store a separate
    /// [`Waker`].
    ///
    /// [`Waker`]: std::task::Waker
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>>;

    /// Send UDP datagrams from `transmits`, or return `WouldBlock` and clear the underlying
    /// socket's readiness, or return an I/O error
    ///
    /// If this returns [`io::ErrorKind::WouldBlock`], [`UdpPoller::poll_writable`] must be called
    /// to register the calling task to be woken when a send should be attempted again.
    fn try_send(&self, transmit: &Transmit) -> io::Result<()>;

    /// Receive UDP datagrams, or register to be woken if receiving may succeed in the future
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>>;

    /// Look up the local IP address and port used by this socket
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Maximum number of datagrams that a [`Transmit`] may encode
    fn max_transmit_segments(&self) -> usize {
        1
    }

    /// Maximum number of datagrams that might be described by a single [`RecvMeta`]
    fn max_receive_segments(&self) -> usize {
        1
    }

    /// Whether datagrams might get fragmented into multiple parts
    ///
    /// Sockets should prevent this for best performance. See e.g. the `IPV6_DONTFRAG` socket
    /// option.
    fn may_fragment(&self) -> bool {
        true
    }
}

/// An object polled to detect when an associated [`AsyncUdpSocket`] is writable
///
/// Any number of `UdpPoller`s may exist for a single [`AsyncUdpSocket`]. Each `UdpPoller` is
/// responsible for notifying at most one task when that socket becomes writable.
pub trait UdpPoller: Send + Sync + Debug + 'static {
    /// Check whether the associated socket is likely to be writable
    ///
    /// Must be called after [`AsyncUdpSocket::try_send`] returns [`io::ErrorKind::WouldBlock`] to
    /// register the task associated with `cx` to be woken when a send should be attempted
    /// again. Unlike in [`Future::poll`], a [`UdpPoller`] may be reused indefinitely no matter how
    /// many times `poll_writable` returns [`Poll::Ready`].
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>>;
}

pin_project_lite::pin_project! {
    /// Helper adapting a function `MakeFut` that constructs a single-use future `Fut` into a
    /// [`UdpPoller`] that may be reused indefinitely
    struct UdpPollHelper<MakeFut, Fut> {
        make_fut: MakeFut,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<MakeFut, Fut> UdpPollHelper<MakeFut, Fut> {
    /// Construct a [`UdpPoller`] that calls `make_fut` to get the future to poll, storing it until
    /// it yields [`Poll::Ready`], then creating a new one on the next
    /// [`poll_writable`](UdpPoller::poll_writable)
    #[cfg(any(
        feature = "runtime-async-std",
        feature = "runtime-smol",
        feature = "runtime-tokio",
    ))]
    fn new(make_fut: MakeFut) -> Self {
        Self {
            make_fut,
            fut: None,
        }
    }
}

impl<MakeFut, Fut> UdpPoller for UdpPollHelper<MakeFut, Fut>
where
    MakeFut: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + Sync + 'static,
{
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let mut this = self.project();
        if this.fut.is_none() {
            this.fut.set(Some((this.make_fut)()));
        }
        // We're forced to `unwrap` here because `Fut` may be `!Unpin`, which means we can't safely
        // obtain an `&mut Fut` after storing it in `self.fut` when `self` is already behind `Pin`,
        // and if we didn't store it then we wouldn't be able to keep it alive between
        // `poll_writable` calls.
        let result = this.fut.as_mut().as_pin_mut().unwrap().poll(cx);
        if result.is_ready() {
            // Polling an arbitrary `Future` after it becomes ready is a logic error, so arrange for
            // a new `Future` to be created on the next call.
            this.fut.set(None);
        }
        result
    }
}

impl<MakeFut, Fut> Debug for UdpPollHelper<MakeFut, Fut> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpPollHelper").finish_non_exhaustive()
    }
}

/// Automatically select an appropriate runtime from those enabled at compile time
///
/// If `runtime-tokio` is enabled and this function is called from within a Tokio runtime context,
/// then `TokioRuntime` is returned. Otherwise, if `runtime-async-std` is enabled, `AsyncStdRuntime`
/// is returned. Otherwise, if `runtime-smol` is enabled, `SmolRuntime` is returned.
/// Otherwise, `None` is returned.
#[allow(clippy::needless_return)] // Be sure we return the right thing
pub fn default_runtime() -> Option<Arc<dyn Runtime>> {
    #[cfg(feature = "runtime-tokio")]
    {
        if ::tokio::runtime::Handle::try_current().is_ok() {
            return Some(Arc::new(TokioRuntime));
        }
    }

    #[cfg(feature = "runtime-async-std")]
    {
        return Some(Arc::new(AsyncStdRuntime));
    }

    #[cfg(all(feature = "runtime-smol", not(feature = "runtime-async-std")))]
    {
        return Some(Arc::new(SmolRuntime));
    }

    #[cfg(not(any(feature = "runtime-async-std", feature = "runtime-smol")))]
    None
}

#[cfg(feature = "runtime-tokio")]
mod tokio;
// Due to MSRV, we must specify `self::` where there's crate/module ambiguity
#[cfg(feature = "runtime-tokio")]
pub use self::tokio::TokioRuntime;

#[cfg(feature = "async-io")]
mod async_io;
// Due to MSRV, we must specify `self::` where there's crate/module ambiguity
#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
pub use self::async_io::*;
