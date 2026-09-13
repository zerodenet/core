use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use zero_platform_tokio::TcpRelayStream;
use zero_transport::RuntimeError;

const CONNECTION_LIFETIME: Duration = Duration::from_secs(2 * 60);
const REPLENISH_DELAY: Duration = Duration::from_millis(200);

struct ReadyConnection {
    stream: TcpRelayStream,
    expires_at: tokio::time::Instant,
    _taken: oneshot::Sender<()>,
}

struct Pool {
    capacity: usize,
    started: AtomicBool,
    ready_tx: mpsc::Sender<ReadyConnection>,
    ready_rx: tokio::sync::Mutex<mpsc::Receiver<ReadyConnection>>,
    cancelled: watch::Sender<bool>,
}

#[derive(Clone)]
pub(in crate::transport) struct Access(Arc<Pool>);

impl std::fmt::Debug for Access {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VlessPreconnectPool")
            .field("capacity", &self.0.capacity)
            .finish_non_exhaustive()
    }
}

type PoolKey = (String, [u8; 32], u32);

#[derive(Default)]
struct State {
    pools: Mutex<HashMap<PoolKey, Arc<Pool>>>,
}

#[derive(Clone, Default)]
pub(super) struct Registry(Arc<State>);

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VlessPreconnectRegistry")
            .finish_non_exhaustive()
    }
}

impl Registry {
    pub(super) fn create(
        &self,
        tag: &str,
        carrier_identity: [u8; 32],
        capacity: u32,
    ) -> Option<Access> {
        let configured_capacity = capacity;
        let capacity = usize::try_from(capacity).ok().filter(|value| *value > 0)?;
        let key = (tag.to_owned(), carrier_identity, configured_capacity);
        let mut pools = self
            .0
            .pools
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(pool) = pools.get(&key) {
            return Some(Access(pool.clone()));
        }
        let (ready_tx, ready_rx) = mpsc::channel(capacity);
        let (cancelled, _) = watch::channel(false);
        let pool = Arc::new(Pool {
            capacity,
            started: AtomicBool::new(false),
            ready_tx,
            ready_rx: tokio::sync::Mutex::new(ready_rx),
            cancelled,
        });
        pools.insert(key, pool.clone());
        Some(Access(pool))
    }

    pub(super) fn retire(&self) {
        self.0.retire();
    }
}

impl State {
    fn retire(&self) {
        let mut pools = self.pools.lock().unwrap_or_else(|error| error.into_inner());
        for (_, pool) in pools.drain() {
            pool.retire();
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let pools = self
            .pools
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        for (_, pool) in pools.drain() {
            pool.retire();
        }
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
        if *self.0.cancelled.borrow() {
            return Err(retired_error());
        }
        self.0.start(open);
        let mut cancelled = self.0.cancelled.subscribe();
        if *cancelled.borrow() {
            return Err(retired_error());
        }
        let mut ready = tokio::select! {
            guard = self.0.ready_rx.lock() => guard,
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

impl Pool {
    fn start<Open, OpenFuture>(&self, open: Open)
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

    fn retire(&self) {
        self.cancelled.send_replace(true);
        if let Ok(mut ready) = self.ready_rx.try_lock() {
            ready.close();
            while ready.try_recv().is_ok() {}
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.cancelled.send_replace(true);
        let ready = self.ready_rx.get_mut();
        ready.close();
        while ready.try_recv().is_ok() {}
    }
}

fn retired_error() -> RuntimeError {
    RuntimeError::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "VLESS preconnect pool retired",
    ))
}

#[cfg(test)]
#[path = "../../../tests/transport/preconnect.rs"]
mod tests;
