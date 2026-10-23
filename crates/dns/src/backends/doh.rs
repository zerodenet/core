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
    tls: Option<Arc<rustls::ClientConfig>>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    clients: Arc<tokio::sync::Mutex<Vec<DohClient>>>,
    pub(crate) owner: Arc<owner::DriverOwner>,
    connect_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(feature = "test-support")]
    test_io_started: std::sync::atomic::AtomicBool,
}

mod client;
pub(super) mod owner;
use client::DohClient;

impl DohDnsResolver {
    pub(crate) fn new(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        Self::new_with_security(host, port, path, bootstrap, server_name, egress, true)
    }

    pub(crate) fn new_cleartext(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        Self::new_with_security(host, port, path, bootstrap, None, egress, false)
    }

    fn new_with_security(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress: zero_platform_tokio::EgressInterfaceControl,
        secure: bool,
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
        let clients = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let connect_lock = Arc::new(tokio::sync::Mutex::new(()));
        let owner = Arc::new(owner::DriverOwner::new(
            clients.clone(),
            connect_lock.clone(),
            ips.len(),
        ));
        Ok(Self {
            addrs: ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect(),
            server_name: server_name.unwrap_or_else(|| host.clone()),
            port,
            path,
            tls: secure.then(|| Arc::new(tls)),
            egress,
            clients,
            owner,
            connect_lock,
            #[cfg(feature = "test-support")]
            test_io_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn install_test_ca(
        &mut self,
        certificate: rustls::pki_types::CertificateDer<'static>,
    ) -> io::Result<()> {
        if self
            .test_io_started
            .load(std::sync::atomic::Ordering::Acquire)
            || !self.owner.resource_admission_open()
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "fixture CA installation requires an unused cold DoH backend",
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).map_err(io::Error::other)?;
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        self.tls = Some(Arc::new(tls));
        Ok(())
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
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "DoH request to {addr} timed out after {}ms",
                    DNS_TIMEOUT.as_millis()
                ),
            )
        })?
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
            .map_err(|error| context_error("DoH request build failed", error))?;
        let (response, mut body) = client
            .sender
            .send_request(request, false)
            .map_err(|error| context_error("DoH send request failed", error))?;
        body.send_data(Bytes::copy_from_slice(query), true)
            .map_err(|error| context_error("DoH send body failed", error))?;

        let response = response
            .await
            .map_err(|error| context_error("DoH response failed", error))?;
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "DoH server returned HTTP {}",
                response.status()
            )));
        }
        let response = read_body(response.into_body()).await?;
        client.complete()?;
        Ok(response)
    }

    fn uri(&self) -> String {
        let authority = if self.server_name.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]", self.server_name)
        } else {
            self.server_name.clone()
        };
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        format!("{scheme}://{authority}:{}{}", self.port, self.path)
    }
}

async fn read_body(mut body: h2::RecvStream) -> io::Result<Vec<u8>> {
    let mut response = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|error| context_error("DoH read failed", error))?;
        if response.len().saturating_add(chunk.len()) > crate::message::MAX_DNS_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DoH response exceeds DNS message limit",
            ));
        }
        response.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|error| context_error("DoH flow control failed", error))?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "../../tests/doh_pool/mod.rs"]
mod pool_tests;

impl Drop for DohDnsResolver {
    fn drop(&mut self) {
        self.owner.retire();
    }
}

#[derive(Debug)]
struct DohError {
    context: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
}
impl DohError {
    fn new(context: &'static str, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            context,
            source: Box::new(source),
        }
    }
}
impl std::fmt::Display for DohError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context, self.source)
    }
}
impl std::error::Error for DohError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
fn context_error(
    context: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> io::Error {
    io::Error::other(DohError::new(context, source))
}
