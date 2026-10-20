//! HTTP/2 connection admission and retirement for one DoH backend.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use zero_platform_tokio::contract::CancellationToken;

use super::*;

#[derive(Clone, PartialEq, Eq)]
struct ConnectionKey {
    addr: SocketAddr,
    generation: u64,
    detour: Option<String>,
}

impl ConnectionKey {
    fn ensure_current(
        &self,
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<()> {
        if self.generation != egress.generation() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "DoH egress changed while preparing a connection",
            ));
        }
        Ok(())
    }
}

pub(super) struct ClientState {
    usable: AtomicBool,
    pub(super) leases: AtomicUsize,
    pub(super) cancel: CancellationToken,
    changed: Arc<tokio::sync::Notify>,
}
impl ClientState {
    pub(super) fn new(changed: Arc<tokio::sync::Notify>) -> Self {
        Self {
            usable: AtomicBool::new(true),
            leases: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            changed,
        }
    }
    pub(super) fn retire(&self) {
        self.usable.store(false, Ordering::Release);
        self.cancel.cancel();
        self.changed.notify_waiters();
    }
}
pub(super) struct DohClient {
    key: ConnectionKey,
    sender: h2::client::SendRequest<Bytes>,
    state: Arc<ClientState>,
}
pub(super) struct ReadyClient {
    pub(super) sender: h2::client::SendRequest<Bytes>,
    state: Arc<ClientState>,
    complete: bool,
}
impl ReadyClient {
    async fn ready(mut self) -> io::Result<Self> {
        self.sender = self
            .sender
            .clone()
            .ready()
            .await
            .map_err(io::Error::other)?;
        if !self.state.usable.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "DoH connection retired while waiting for readiness",
            ));
        }
        Ok(self)
    }
    pub(super) fn complete(&mut self) {
        self.complete = true;
    }
}
impl Drop for ReadyClient {
    fn drop(&mut self) {
        if !self.complete {
            self.state.retire();
        }
        self.state.leases.fetch_sub(1, Ordering::AcqRel);
        self.state.changed.notify_waiters();
    }
}
impl Drop for DohClient {
    fn drop(&mut self) {
        self.state.retire();
    }
}
impl DohClient {
    fn lease(&self) -> ReadyClient {
        self.state.leases.fetch_add(1, Ordering::AcqRel);
        ReadyClient {
            sender: self.sender.clone(),
            state: self.state.clone(),
            complete: false,
        }
    }
}

impl DohDnsResolver {
    pub(super) async fn ready_client(
        &self,
        addr: SocketAddr,
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<ReadyClient> {
        self.owner.ensure_open()?;
        let key = ConnectionKey {
            addr,
            generation: self.egress.generation(),
            detour: detour.map(ToOwned::to_owned),
        };
        if let Some(client) = self.cached_client(&key).await {
            match client.ready().await {
                Ok(client) => {
                    key.ensure_current(&self.egress)?;
                    return Ok(client);
                }
                Err(error) => {
                    tracing::debug!(server = %addr, %error, "discarding stale DoH HTTP/2 connection");
                }
            }
        }
        let _connect = self.connect_lock.lock().await;
        key.ensure_current(&self.egress)?;
        if let Some(client) = self.cached_client(&key).await {
            match client.ready().await {
                Ok(client) => {
                    key.ensure_current(&self.egress)?;
                    return Ok(client);
                }
                Err(error) => {
                    tracing::debug!(server = %addr, %error, "replacing stale DoH HTTP/2 connection");
                }
            }
        }
        // This endpoint slot is being replaced. Stop its old users and join
        // the actual driver before allocating the replacement physical socket.
        self.clients
            .lock()
            .await
            .retain(|entry| entry.key.addr != key.addr);
        let deadline = super::super::ATTEMPT_DEADLINE
            .try_with(|deadline| *deadline)
            .unwrap_or_else(|_| tokio::time::Instant::now() + DNS_TIMEOUT);
        self.owner.drain_retired(deadline).await?;
        self.owner.ensure_open()?;
        let cancel = self.owner.cancellation();
        let client=tokio::select! {biased;
            _=cancel.cancelled()=>return Err(io::Error::new(io::ErrorKind::NotConnected,"DoH pool closed during inline setup")),
            result=self.connect_client(&key,connector)=>result?,
        }.ready().await?;
        key.ensure_current(&self.egress)?;
        Ok(client)
    }

    async fn cached_client(&self, key: &ConnectionKey) -> Option<ReadyClient> {
        let mut clients = self.clients.lock().await;
        let generation = self.egress.generation();
        clients.retain(|entry| {
            entry.key.generation == generation && entry.state.usable.load(Ordering::Acquire)
        });
        clients
            .iter()
            .find(|entry| entry.key == *key)
            .map(DohClient::lease)
    }

    async fn connect_client(
        &self,
        key: &ConnectionKey,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<ReadyClient> {
        let stream =
            super::super::connect_tcp(key.addr, &self.egress, key.detour.as_deref(), connector)
                .await
                .map_err(|error| {
                    io::Error::new(error.kind(), DohError::new("DoH connect failed", error))
                })?;
        key.ensure_current(&self.egress)?;
        // The common DNS dial operation logs the actual pre-connect selection;
        // do not re-probe the route afterwards and misreport a new selection
        // as the binding of this already-open socket.
        tracing::debug!(server = %key.addr, outbound = ?key.detour,
            egress_generation = key.generation, "DoH TCP socket connected");

        let stream = if let Some(tls) = &self.tls {
            let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let connector = tokio_rustls::TlsConnector::from(Arc::clone(tls));
            let tls = connector
                .connect(server_name, stream)
                .await
                .map_err(|error| context_error("DoH TLS failed", error))?;
            zero_platform_tokio::TcpRelayStream::new(tls)
        } else {
            stream
        };
        key.ensure_current(&self.egress)?;
        let (sender, connection): (h2::client::SendRequest<Bytes>, _) =
            h2::client::handshake(stream)
                .await
                .map_err(|error| context_error("DoH HTTP/2 handshake failed", error))?;
        let mut clients = self.clients.lock().await;
        key.ensure_current(&self.egress)?;
        let entry = DohClient {
            key: key.clone(),
            sender,
            state: self.owner.client(),
        };
        let client = entry.lease();
        clients.retain(|entry| {
            entry.key.addr != key.addr
                && entry.key.generation == key.generation
                && entry.state.usable.load(Ordering::Acquire)
        });
        // Spawn admission and closing share the actual owner lock. A failed
        // registration drops this unstarted connection without publishing it.
        self.owner.spawn(entry.state.clone(), connection)?;
        clients.push(entry);
        Ok(client)
    }
}
