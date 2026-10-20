//! Persistent ownership of one actual DoQ endpoint and its one connection.
//! Quinn 0.11.9 spawns exactly an endpoint driver and a connection driver for
//! this shape. Both real tasks are registered, bounded, and naturally joined.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::{task::JoinSet, time::Instant};
use zero_platform_tokio::contract::CancellationToken;
mod runtime;

#[derive(Default)]
pub(super) struct Status {
    failure: Mutex<Option<Arc<io::Error>>>,
    pub(super) cancel: CancellationToken,
}
#[derive(Debug)]
struct Source(Arc<io::Error>);
impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for Source {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl Status {
    pub(super) fn record(&self, source: io::Error) {
        self.failure
            .lock()
            .expect("DoQ owner source poisoned")
            .get_or_insert_with(|| Arc::new(source));
        self.cancel.cancel();
    }
    pub(super) fn check(&self) -> io::Result<()> {
        match self
            .failure
            .lock()
            .expect("DoQ owner source poisoned")
            .as_ref()
        {
            Some(source) => Err(io::Error::new(source.kind(), Source(source.clone()))),
            None => Ok(()),
        }
    }
    pub(super) fn io_error(&self, source: io::Error) -> io::Error {
        if source.kind() == io::ErrorKind::WouldBlock {
            return source;
        }
        let source = Arc::new(source);
        self.record(io::Error::new(source.kind(), Source(source.clone())));
        io::Error::new(source.kind(), Source(source))
    }
}
struct State {
    closing: bool,
    tasks: JoinSet<()>,
}
pub(crate) struct EndpointOwner {
    setup: Mutex<()>,
    endpoint: Mutex<Option<quinn::Endpoint>>,
    state: Mutex<State>,
    cleanup: tokio::sync::Mutex<()>,
    pub(super) status: Arc<Status>,
}
impl EndpointOwner {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            setup: Mutex::new(()),
            endpoint: Mutex::new(None),
            state: Mutex::new(State {
                closing: false,
                tasks: JoinSet::new(),
            }),
            cleanup: tokio::sync::Mutex::new(()),
            status: Arc::default(),
        })
    }
    fn reap(state: &mut State, status: &Status) {
        while let Some(result) = state.tasks.try_join_next() {
            if let Err(source) = result {
                status.record(io::Error::other(source));
            }
        }
    }
    pub(super) fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) -> io::Result<()> {
        let mut state = self.state.lock().expect("DoQ tasks poisoned");
        Self::reap(&mut state, &self.status);
        self.status.check()?;
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoQ endpoint is closing",
            ));
        }
        // This is the actual one-endpoint/one-connection shape, not a default
        // pool multiplier or a claimed global bound on all DNS queries.
        if state.tasks.len() >= 2 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "DoQ endpoint and connection driver slots exhausted",
            ));
        }
        state.tasks.spawn(future);
        Ok(())
    }
    pub(super) fn setup(
        self: &Arc<Self>,
        peer: std::net::SocketAddr,
        client: quinn::ClientConfig,
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<()> {
        // Endpoint::new synchronously calls Runtime::spawn. The separate setup
        // gate lets that registration take state without reentrant locking,
        // while shutdown cannot confirm an empty owner and permit late setup.
        let _setup = self.setup.lock().expect("DoQ setup gate poisoned");
        self.ensure_open()?;
        let interface = egress.try_current_for_peer(peer)?;
        let socket =
            zero_platform_tokio::bind_std_datagram_socket_for_peer(peer, interface.as_ref())?;
        let runtime = Arc::new(runtime::OwnedRuntime {
            owner: Arc::downgrade(self),
            status: self.status.clone(),
        });
        let mut endpoint =
            quinn::Endpoint::new(quinn::EndpointConfig::default(), None, socket, runtime)?;
        endpoint.set_default_client_config(client);
        *self.endpoint.lock().expect("DoQ endpoint poisoned") = Some(endpoint);
        self.status.check()
    }
    pub(super) fn ensure_open(&self) -> io::Result<()> {
        self.status.check()?;
        if self.state.lock().expect("DoQ tasks poisoned").closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoQ endpoint is closing",
            ));
        }
        Ok(())
    }
    pub(super) fn connect(
        &self,
        peer: std::net::SocketAddr,
        name: &str,
    ) -> io::Result<quinn::Connecting> {
        let _setup = self.setup.lock().expect("DoQ setup gate poisoned");
        self.ensure_open()?;
        let connecting = self
            .endpoint
            .lock()
            .expect("DoQ endpoint poisoned")
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "DoQ endpoint is not installed")
            })?
            .connect(peer, name)
            .map_err(io::Error::other)?;
        self.status.check()?;
        Ok(connecting)
    }
    pub(crate) fn request_close(&self) {
        let _setup = self.setup.lock().expect("DoQ setup gate poisoned");
        self.state.lock().expect("DoQ tasks poisoned").closing = true;
        self.status.cancel.cancel();
        if let Some(endpoint) = self
            .endpoint
            .lock()
            .expect("DoQ endpoint poisoned")
            .as_ref()
        {
            endpoint.close(0u32.into(), b"DNS query finished");
        }
    }
    pub(crate) fn closing(&self) -> bool {
        self.state.lock().expect("DoQ tasks poisoned").closing
    }
    pub(crate) fn drained(&self) -> bool {
        let _setup = self.setup.lock().expect("DoQ setup gate poisoned");
        let mut state = self.state.lock().expect("DoQ tasks poisoned");
        Self::reap(&mut state, &self.status);
        self.endpoint
            .lock()
            .expect("DoQ endpoint poisoned")
            .is_none()
            && state.tasks.is_empty()
            && self.status.check().is_ok()
    }
    pub(crate) async fn shutdown(&self, deadline: Instant) -> io::Result<()> {
        self.request_close();
        let _cleanup = tokio::time::timeout_at(deadline, self.cleanup.lock())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        let endpoint = self.endpoint.lock().expect("DoQ endpoint poisoned").clone();
        if let Some(endpoint) = endpoint {
            tokio::time::timeout_at(deadline, endpoint.wait_idle())
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
            // wait_idle needs a live handle; the endpoint driver needs all
            // handles gone. Never keep this cleanup clone across the join.
            drop(endpoint);
        }
        drop(self.endpoint.lock().expect("DoQ endpoint poisoned").take());
        loop {
            let joined = std::future::poll_fn(|cx| {
                let mut state = self.state.lock().expect("DoQ tasks poisoned");
                match state.tasks.poll_join_next(cx) {
                    Poll::Ready(Some(result)) => {
                        if let Err(source) = result {
                            self.status.record(io::Error::other(source));
                        }
                        Poll::Ready(true)
                    }
                    Poll::Ready(None) => Poll::Ready(false),
                    Poll::Pending => Poll::Pending,
                }
            });
            if !tokio::time::timeout_at(deadline, joined)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
            {
                break;
            }
        }
        self.status.check()
    }
    #[cfg(test)]
    pub(super) fn task_count(&self) -> usize {
        self.state.lock().unwrap().tasks.len()
    }
    #[cfg(test)]
    pub(super) fn local_addr(&self) -> std::net::SocketAddr {
        self.endpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .local_addr()
            .unwrap()
    }
}

pub(super) struct CloseOnDrop(pub(super) Arc<EndpointOwner>);
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.request_close();
    }
}

#[cfg(test)]
mod tests;
