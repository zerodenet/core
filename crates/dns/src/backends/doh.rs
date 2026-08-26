use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request};

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DohDnsResolver {
    port: u16,
    path: String,
    addrs: Vec<SocketAddr>,
    server_name: String,
    tls: Arc<rustls::ClientConfig>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    clients: tokio::sync::Mutex<Vec<DohClient>>,
    connect_lock: tokio::sync::Mutex<()>,
}

struct DohClient {
    addr: SocketAddr,
    underlay: Option<zero_platform_tokio::EgressInterface>,
    detour: Option<String>,
    sender: h2::client::SendRequest<Bytes>,
}

impl DohDnsResolver {
    pub(crate) fn new(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let ips = if bootstrap.is_empty() {
            host.parse().map(|ip| vec![ip]).unwrap_or_default()
        } else {
            bootstrap
        };
        if ips.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("DoH host `{host}` requires a bootstrap address"),
            ));
        }
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| io::Error::other(format!("DoH TLS protocol: {error}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Self {
            addrs: ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect(),
            server_name: server_name.unwrap_or_else(|| host.clone()),
            port,
            path,
            tls: Arc::new(tls),
            egress,
            clients: tokio::sync::Mutex::new(Vec::new()),
            connect_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) async fn exchange(
        &self,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        let addrs = self.endpoint_addresses();
        let mut last_error = None;
        for addr in addrs {
            match self.exchange_with(addr, query, detour, connector).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "DoH backend has no endpoint")
        }))
    }

    fn endpoint_addresses(&self) -> Vec<SocketAddr> {
        self.addrs.clone()
    }

    pub(crate) fn endpoint_labels(&self) -> Vec<String> {
        self.addrs.iter().map(ToString::to_string).collect()
    }

    async fn exchange_with(
        &self,
        addr: SocketAddr,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        tokio::time::timeout(
            DNS_TIMEOUT,
            self.exchange_with_timeout(addr, query, detour, connector),
        )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoH request timeout"))?
    }

    async fn exchange_with_timeout(
        &self,
        addr: SocketAddr,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        let mut client = self.ready_client(addr, detour, connector).await?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(self.uri())
            .header("content-type", "application/dns-message")
            .header("accept", "application/dns-message")
            .header("content-length", query.len())
            .body(())
            .map_err(|error| io::Error::other(format!("DoH request build failed: {error}")))?;
        let (response, mut body) = client
            .send_request(request, false)
            .map_err(|error| io::Error::other(format!("DoH send request failed: {error}")))?;
        body.send_data(Bytes::copy_from_slice(query), true)
            .map_err(|error| io::Error::other(format!("DoH send body failed: {error}")))?;

        let response = response
            .await
            .map_err(|error| io::Error::other(format!("DoH response failed: {error}")))?;
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "DoH server returned HTTP {}",
                response.status()
            )));
        }
        read_body(response.into_body()).await
    }

    async fn ready_client(
        &self,
        addr: SocketAddr,
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<h2::client::SendRequest<Bytes>> {
        let underlay = if detour.is_some() {
            None
        } else {
            self.egress.current_for(addr.is_ipv6())
        };
        if let Some(sender) = self.cached_client(addr, underlay.as_ref(), detour).await {
            match sender.ready().await {
                Ok(sender) => return Ok(sender),
                Err(error) => {
                    tracing::debug!(server = %addr, error = %error, "discarding stale DoH HTTP/2 connection");
                }
            }
        }
        let _connect = self.connect_lock.lock().await;
        if let Some(sender) = self.cached_client(addr, underlay.as_ref(), detour).await {
            match sender.ready().await {
                Ok(sender) => return Ok(sender),
                Err(error) => {
                    tracing::debug!(server = %addr, error = %error, "replacing stale DoH HTTP/2 connection");
                    self.evict_client(addr, underlay.as_ref(), detour).await;
                }
            }
        }
        self.connect_client(addr, underlay, detour, connector)
            .await?
            .ready()
            .await
            .map_err(|error| io::Error::other(format!("DoH HTTP/2 connection not ready: {error}")))
    }

    async fn cached_client(
        &self,
        addr: SocketAddr,
        underlay: Option<&zero_platform_tokio::EgressInterface>,
        detour: Option<&str>,
    ) -> Option<h2::client::SendRequest<Bytes>> {
        self.clients
            .lock()
            .await
            .iter()
            .find(|client| {
                client.addr == addr
                    && client.underlay.as_ref() == underlay
                    && client.detour.as_deref() == detour
            })
            .map(|client| client.sender.clone())
    }

    async fn evict_client(
        &self,
        addr: SocketAddr,
        underlay: Option<&zero_platform_tokio::EgressInterface>,
        detour: Option<&str>,
    ) {
        self.clients
            .lock()
            .await
            .retain(|client| {
                !(client.addr == addr
                    && client.underlay.as_ref() == underlay
                    && client.detour.as_deref() == detour)
            });
    }

    async fn connect_client(
        &self,
        addr: SocketAddr,
        underlay: Option<zero_platform_tokio::EgressInterface>,
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<h2::client::SendRequest<Bytes>> {
        let stream = super::connect_tcp(addr, &self.egress, detour, connector)
            .await
            .map_err(|error| {
                io::Error::new(error.kind(), format!("DoH connect failed: {error}"))
            })?;
        if let Some(detour) = detour {
            tracing::debug!(server = %addr, outbound = %detour, "DoH TCP detour connected");
        } else {
            let selection = self.egress.select_for_peer(addr);
            tracing::debug!(
                server = %addr,
                egress_name = selection.interface().map(zero_platform_tokio::EgressInterface::name),
                egress_index = selection.interface().map(zero_platform_tokio::EgressInterface::index),
                binding_reason = selection.binding_reason().as_str(),
                "DoH TCP socket connected"
            );
        }

        let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls));
        let tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|error| io::Error::other(format!("DoH TLS failed: {error}")))?;
        let (client, connection): (h2::client::SendRequest<Bytes>, _) = h2::client::handshake(tls)
            .await
            .map_err(|error| io::Error::other(format!("DoH HTTP/2 handshake failed: {error}")))?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "DoH HTTP/2 connection closed");
            }
        });
        let mut clients = self.clients.lock().await;
        clients.retain(|entry| entry.addr != addr);
        clients.push(DohClient {
            addr,
            underlay,
            detour: detour.map(ToOwned::to_owned),
            sender: client.clone(),
        });
        Ok(client)
    }

    fn uri(&self) -> String {
        let authority = if self.server_name.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]", self.server_name)
        } else {
            self.server_name.clone()
        };
        format!("https://{authority}:{}{}", self.port, self.path)
    }
}

async fn read_body(mut body: h2::RecvStream) -> io::Result<Vec<u8>> {
    let mut response = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|error| io::Error::other(format!("DoH read failed: {error}")))?;
        if response.len().saturating_add(chunk.len()) > crate::message::MAX_DNS_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DoH response exceeds DNS message limit",
            ));
        }
        response.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|error| io::Error::other(format!("DoH flow control failed: {error}")))?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests;
