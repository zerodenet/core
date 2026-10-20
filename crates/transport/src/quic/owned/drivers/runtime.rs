use super::*;
use std::{future::Future, pin::Pin};
use tracing::Instrument;

pub(super) struct OwnedRuntime {
    pub(super) owner: Weak<Shared>,
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
        let rejection = {
            let mut state = owner.state.lock().unwrap();
            if self.callbacks.failed() {
                Some(DriverRegistrationReason::ObserverFault)
            } else if state.retired {
                Some(DriverRegistrationReason::Retired)
            } else if state.records.len() + state.pending >= owner.limit.get() {
                Some(DriverRegistrationReason::LocalCapacity)
            } else {
                state.pending += 1;
                None
            }
        };
        if let Some(reason) = rejection {
            owner.reject(Some(origin), reason);
            drop(future);
            return;
        }
        let record = Arc::new(Record {
            origin,
            registration: Mutex::new(None),
            body: Mutex::new(None),
            received: Mutex::new(None),
            delivered: AtomicBool::new(false),
        });
        let task_record = record.clone();
        let future = super::super::callbacks::Boundary::new(future, self.callbacks.clone());
        let class = match origin {
            DriverOrigin::Endpoint => TaskClass::Critical,
            DriverOrigin::Connection => TaskClass::Auxiliary,
        };
        // Owner admission is reserved first, but no owner lock crosses actual
        // scope registration, its external wakeups or destruction of a refusal.
        let registration = owner.registrar.spawn_tracked(
            class,
            async move {
                let exit = future.await;
                let body = match exit {
                    DriverExit::Endpoint(result) => DriverBody {
                        origin: DriverOrigin::Endpoint,
                        io_error: result.err().map(Arc::new),
                        close_reason: None,
                    },
                    DriverExit::Connection {
                        result,
                        close_reason,
                    } => DriverBody {
                        origin: DriverOrigin::Connection,
                        io_error: result.err().map(Arc::new),
                        close_reason,
                    },
                };
                let body = Arc::new(body);
                *task_record.body.lock().unwrap() = Some(body.clone());
                if body.origin != task_record.origin {
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
            .instrument(tracing::Span::current()),
        );
        match registration {
            Ok((id, witness)) => {
                *record.registration.lock().unwrap() = Some((id, witness));
                let mut state = owner.state.lock().unwrap();
                state.pending -= 1;
                state.records.insert(id, record);
            }
            Err(source) => {
                owner.state.lock().unwrap().pending -= 1;
                owner.reject(Some(origin), DriverRegistrationReason::Scope(source));
            }
        }
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
