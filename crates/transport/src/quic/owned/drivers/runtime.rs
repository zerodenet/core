use super::*;
use quinn::{DriverExit, DriverFuture, DriverOrigin};
use std::{fmt, future::Future, pin::Pin};
use tracing::Instrument;
use zero_platform_tokio::contract::{scope::TaskClass, Impact, Reason, Stage};

pub(super) struct OwnedRuntime {
    pub(super) owner: tracked::WeakDrivers<OwnedDriverOrigin, DriverBody>,
    pub(super) callbacks: Arc<super::super::callbacks::Broker>,
}
impl fmt::Debug for OwnedRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedQuinnRuntime").finish_non_exhaustive()
    }
}
impl quinn::Runtime for OwnedRuntime {
    fn new_timer(&self, instant: std::time::Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        quinn::Runtime::new_timer(&quinn::TokioRuntime, instant)
    }
    fn now(&self) -> std::time::Instant {
        quinn::Runtime::now(&quinn::TokioRuntime)
    }
    fn wrap_udp_socket(
        &self,
        _socket: std::net::UdpSocket,
    ) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "owned Quinn requires a preowned abstract metadata grant",
        ))
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        if let Some(owner) = self.owner.upgrade() {
            owner.reject(None, DriverRegistrationReason::UnsupportedGenericSpawn);
        }
        // An unaccepted future has no invented TaskId or successful TaskExit.
        drop(future);
    }
    fn spawn_driver(&self, origin: DriverOrigin, future: DriverFuture) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        if self.callbacks.failed() {
            owner.reject(Some(origin.into()), DriverRegistrationReason::ObserverFault);
            return;
        }
        let future = super::super::callbacks::Boundary::new(future, self.callbacks.clone());
        let class = match origin {
            DriverOrigin::Endpoint => TaskClass::Critical,
            DriverOrigin::Connection => TaskClass::Auxiliary,
        };
        // Owner admission is reserved first, but no owner lock crosses actual
        // scope registration, its external wakeups or destruction of a refusal.
        let _registration = owner.spawn(origin.into(), class, |slot| {
            async move {
                let exit = future.await;
                let body = match exit {
                    DriverExit::Endpoint(result) => DriverBody {
                        origin: OwnedDriverOrigin::Quinn(DriverOrigin::Endpoint),
                        io_error: result.err().map(Arc::new),
                        close_reason: None,
                        #[cfg(any(feature = "split_http", feature = "hysteria"))]
                        http3: None,
                    },
                    DriverExit::Connection {
                        result,
                        close_reason,
                    } => DriverBody {
                        origin: OwnedDriverOrigin::Quinn(DriverOrigin::Connection),
                        io_error: result.err().map(Arc::new),
                        close_reason,
                        #[cfg(any(feature = "split_http", feature = "hysteria"))]
                        http3: None,
                    },
                };
                let body = slot.record(body);
                if body.origin != slot.origin() {
                    return Err(ChannelError::new(
                        Stage::Close,
                        Reason::Internal,
                        Impact::Component,
                    ));
                }
                if let Some(source) = &body.io_error {
                    return Err(ChannelError::new(
                        Stage::Close,
                        Reason::Io,
                        match origin {
                            DriverOrigin::Endpoint => Impact::Component,
                            DriverOrigin::Connection => Impact::Session,
                        },
                    )
                    .with_source(OriginalIo(source.clone())));
                }
                Ok(())
            }
            .instrument(tracing::Span::current())
        });
    }
}
#[derive(Debug)]
struct OriginalIo(Arc<io::Error>);
impl fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
