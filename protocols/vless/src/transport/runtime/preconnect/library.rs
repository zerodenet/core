//! Explicit stand-alone protocol-library warming; Kernel uses the Host factory.
use super::*;
use std::{future::Future, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};
use zero_platform_tokio::TcpRelayStream;
pub(super) const CONNECTION_LIFETIME: Duration = Duration::from_secs(120);
pub(super) const REPLENISH_DELAY: Duration = Duration::from_millis(200);
struct ReadyConnection {
    stream: TcpRelayStream,
    expires_at: tokio::time::Instant,
    _taken: oneshot::Sender<()>,
}
pub(super) struct Library {
    capacity: usize,
    started: AtomicBool,
    ready_tx: mpsc::Sender<ReadyConnection>,
    ready_rx: tokio::sync::Mutex<mpsc::Receiver<ReadyConnection>>,
    cancelled: watch::Sender<bool>,
}
impl Library {
    pub(super) fn new(capacity: usize) -> Self {
        let (ready_tx, ready_rx) = mpsc::channel(capacity);
        let (cancelled, _) = watch::channel(false);
        Self {
            capacity,
            started: false.into(),
            ready_tx,
            ready_rx: tokio::sync::Mutex::new(ready_rx),
            cancelled,
        }
    }
    pub(super) fn start<Open, OpenFuture>(&self, open: Open)
    where
        Open: Clone + Fn() -> OpenFuture + Send + Sync + 'static,
        OpenFuture: Future<Output = Result<TcpRelayStream, RuntimeError>> + Send + 'static,
    {
        if *self.cancelled.borrow() {
            return;
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        for _ in 0..self.capacity {
            if *self.cancelled.borrow() {
                return;
            }
            let ready = self.ready_tx.clone();
            let mut cancelled = self.cancelled.subscribe();
            let open = open.clone();
            tokio::spawn(async move {
                if *cancelled.borrow() {
                    return;
                }
                loop {
                    if *cancelled.borrow() {
                        break;
                    }
                    let permit = tokio::select! {
                        permit = ready.reserve() => match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        },
                        _ = cancelled.changed() => break,
                    };
                    if *cancelled.borrow() {
                        drop(permit);
                        break;
                    }
                    let stream = tokio::select! {
                        result = open() => result,
                        _ = cancelled.changed() => break,
                    };
                    match stream {
                        Ok(stream) => {
                            if *cancelled.borrow() {
                                drop(permit);
                                break;
                            }
                            let (taken, receipt) = oneshot::channel();
                            permit.send(ReadyConnection {
                                stream,
                                expires_at: tokio::time::Instant::now() + CONNECTION_LIFETIME,
                                _taken: taken,
                            });
                            // Match the reference's unbuffered handoff: the
                            // replenish delay begins when a caller takes or
                            // discards this connection, not when it is queued.
                            tokio::select! {
                                _ = receipt => {}
                                _ = cancelled.changed() => break,
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(REPLENISH_DELAY) => {}
                                _ = cancelled.changed() => break,
                            }
                        }
                        Err(error) => {
                            drop(permit);
                            tracing::warn!(error = %error, "VLESS pre-connect failed");
                            tokio::task::yield_now().await;
                        }
                    }
                }
            });
        }
    }

    pub(super) fn retire(&self) {
        self.cancelled.send_replace(true);
        if let Ok(mut ready) = self.ready_rx.try_lock() {
            ready.close();
            while ready.try_recv().is_ok() {}
        }
    }
}
impl Drop for Library {
    fn drop(&mut self) {
        self.cancelled.send_replace(true);
        let ready = self.ready_rx.get_mut();
        ready.close();
        while ready.try_recv().is_ok() {}
    }
}
impl Access {
    pub(in crate::transport) async fn take<Open, OpenFuture>(
        &self,
        open: Open,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        Open: Clone + Fn() -> OpenFuture + Send + Sync + 'static,
        OpenFuture: Future<Output = Result<TcpRelayStream, RuntimeError>> + Send + 'static,
    {
        let library = self.0.library()?;
        if *library.cancelled.borrow() {
            return Err(retired_error());
        }
        library.start(open);
        let mut cancelled = library.cancelled.subscribe();
        if *cancelled.borrow() {
            return Err(retired_error());
        }
        let mut ready = tokio::select! {
            guard = library.ready_rx.lock() => guard,
            _ = cancelled.changed() => return Err(retired_error()),
        };
        loop {
            if *cancelled.borrow() {
                ready.close();
                while ready.try_recv().is_ok() {}
                return Err(retired_error());
            }
            let connection = tokio::select! {
                connection = ready.recv() => connection,
                _ = cancelled.changed() => {
                    ready.close();
                    while ready.try_recv().is_ok() {}
                    return Err(retired_error());
                }
            };
            let Some(connection) = connection else {
                return Err(retired_error());
            };
            if tokio::time::Instant::now() < connection.expires_at {
                return Ok(connection.stream);
            }
        }
    }
}
fn retired_error() -> RuntimeError {
    RuntimeError::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "VLESS preconnect pool retired",
    ))
}
