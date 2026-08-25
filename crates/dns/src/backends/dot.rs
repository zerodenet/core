use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DotDnsResolver {
    addrs: Vec<SocketAddr>,
    server_name: String,
    tls: Arc<rustls::ClientConfig>,
    egress: zero_platform_tokio::EgressInterfaceControl,
}

impl DotDnsResolver {
    pub(crate) fn new(
        host: String,
        port: u16,
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
                format!("DoT host `{host}` requires a bootstrap address"),
            ));
        }
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|error| io::Error::other(format!("DoT TLS protocol: {error}")))?
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        Ok(Self {
            addrs: ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect(),
            server_name: server_name.unwrap_or(host),
            tls,
            egress,
        })
    }

    pub(crate) async fn exchange(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        let mut last_error = None;
        for addr in &self.addrs {
            match self.exchange_with(*addr, query).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "DoT backend has no endpoint")
        }))
    }

    async fn exchange_with(&self, addr: SocketAddr, query: &[u8]) -> io::Result<Vec<u8>> {
        let interface = self.egress.try_current_for_peer(addr)?;
        let stream = tokio::time::timeout(
            DNS_TIMEOUT,
            zero_platform_tokio::TokioSocket::connect_addr_on(addr, interface.as_ref()),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoT connect timeout"))??;
        let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls));
        let mut tls = tokio::time::timeout(DNS_TIMEOUT, connector.connect(server_name, stream))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoT TLS timeout"))?
            .map_err(|error| io::Error::other(format!("DoT TLS failed: {error}")))?;
        super::write_framed(&mut tls, query).await?;
        super::read_framed(&mut tls).await
    }
}
