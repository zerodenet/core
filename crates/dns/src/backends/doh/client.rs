//! HTTP/2 connection admission and retirement for one DoH backend.

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

mod state;
pub(super) use state::ClientState;

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
        self.state.ensure_current()?;
        Ok(self)
    }
    pub(super) fn complete(&mut self) -> io::Result<()> {
        self.state.ensure_current()?;
        self.complete = true;
        Ok(())
    }
}
impl Drop for ReadyClient {
    fn drop(&mut self) {
        if !self.complete || !self.state.reusable {
            self.state.retire();
        }
        self.state
            .leases
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
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
        self.state
            .leases
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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
        clients.retain(|entry| entry.key.generation == generation && entry.state.is_reusable());
        clients
            .iter()
            .find(|entry| entry.key == *key)
            .map(DohClient::lease)
    }
}

mod connect;
