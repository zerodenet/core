//! HTTP/2 connection admission and retirement for one DoH backend.
use std::sync::atomic::{AtomicBool, Ordering};

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

pub(super) struct DohClient {
    key: ConnectionKey,
    sender: h2::client::SendRequest<Bytes>,
    usable: Arc<AtomicBool>,
}

/// A failed or cancelled exchange retires only its own connection. In
/// particular, cancellation by the enclosing configurable backend deadline
/// runs this guard even when the inner DoH timeout has not fired yet.
pub(super) struct ReadyClient {
    pub(super) sender: h2::client::SendRequest<Bytes>,
    usable: Arc<AtomicBool>,
    complete: bool,
}

impl ReadyClient {
    async fn ready(mut self) -> io::Result<Self> {
        self.sender = self.sender.clone().ready().await.map_err(|error| {
            io::Error::other(format!("DoH HTTP/2 connection not ready: {error}"))
        })?;
        if !self.usable.load(Ordering::Acquire) {
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
            self.usable.store(false, Ordering::Release);
        }
    }
}

impl DohClient {
    fn lease(&self) -> ReadyClient {
        ReadyClient {
            sender: self.sender.clone(),
            usable: self.usable.clone(),
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
        let client = self.connect_client(&key, connector).await?.ready().await?;
        key.ensure_current(&self.egress)?;
        Ok(client)
    }

    async fn cached_client(&self, key: &ConnectionKey) -> Option<ReadyClient> {
        let mut clients = self.clients.lock().await;
        let generation = self.egress.generation();
        clients.retain(|entry| {
            entry.key.generation == generation && entry.usable.load(Ordering::Acquire)
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
                    io::Error::new(error.kind(), format!("DoH connect failed: {error}"))
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
                .map_err(|error| io::Error::other(format!("DoH TLS failed: {error}")))?;
            zero_platform_tokio::TcpRelayStream::new(tls)
        } else {
            stream
        };
        key.ensure_current(&self.egress)?;
        let (sender, connection): (h2::client::SendRequest<Bytes>, _) =
            h2::client::handshake(stream).await.map_err(|error| {
                io::Error::other(format!("DoH HTTP/2 handshake failed: {error}"))
            })?;
        let mut clients = self.clients.lock().await;
        key.ensure_current(&self.egress)?;
        let entry = DohClient {
            key: key.clone(),
            sender,
            usable: Arc::new(AtomicBool::new(true)),
        };
        let client = entry.lease();
        clients.retain(|entry| {
            entry.key.addr != key.addr
                && entry.key.generation == key.generation
                && entry.usable.load(Ordering::Acquire)
        });
        clients.push(entry);
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "DoH HTTP/2 connection closed");
            }
        });
        Ok(client)
    }
}
