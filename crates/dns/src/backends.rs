//! Internal enum over all DNS resolver backends.

use std::io;
#[cfg(any(feature = "doh", feature = "dot"))]
use std::time::Duration;

use zero_config::DnsServerConfig;
use zero_traits::{DnsResolver, IpAddress};

use crate::system::TokioSystemResolver;
#[cfg(feature = "udp")]
use crate::udp::UdpDnsResolver;

#[cfg(any(feature = "doh", feature = "dot"))]
const DNS_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "dot")]
use std::net::SocketAddr;
#[cfg(feature = "dot")]
use std::sync::Arc;

/// All DNS resolver backend variants.
pub(crate) enum ResolverBackend {
    System(TokioSystemResolver),
    #[cfg(feature = "udp")]
    Udp(UdpDnsResolver),
    #[cfg(feature = "doh")]
    Doh(DohDnsResolver),
    #[cfg(feature = "dot")]
    Dot(DotDnsResolver),
}

impl ResolverBackend {
    /// Build a backend from its config.
    pub(crate) fn build(server: &DnsServerConfig) -> io::Result<Self> {
        match server {
            DnsServerConfig::System => Ok(Self::System(TokioSystemResolver)),
            #[cfg(feature = "udp")]
            DnsServerConfig::Udp { address, port } => {
                let address = address.parse::<std::net::IpAddr>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid UDP DNS address `{address}`: {error}"),
                    )
                })?;
                Ok(Self::Udp(UdpDnsResolver::new(std::net::SocketAddr::new(
                    address, *port,
                ))))
            }
            #[cfg(not(feature = "udp"))]
            DnsServerConfig::Udp { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UDP DNS backend is not compiled (enable feature `udp`)",
            )),
            #[cfg(feature = "doh")]
            DnsServerConfig::Doh { url, server_name } => Ok(Self::Doh(DohDnsResolver::new(
                url.clone(),
                server_name.clone(),
            )?)),
            #[cfg(not(feature = "doh"))]
            DnsServerConfig::Doh { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS-over-HTTPS is not compiled (enable feature `doh`)",
            )),
            #[cfg(feature = "dot")]
            DnsServerConfig::Dot {
                address,
                port,
                server_name,
            } => Ok(Self::Dot(DotDnsResolver::new(
                address.clone(),
                *port,
                server_name.clone(),
            )?)),
            #[cfg(not(feature = "dot"))]
            DnsServerConfig::Dot { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS-over-TLS is not compiled (enable feature `dot`)",
            )),
        }
    }

    pub(crate) async fn resolve(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        match self {
            Self::System(r) => r.resolve(domain).await,
            #[cfg(feature = "udp")]
            Self::Udp(r) => r.resolve(domain).await,
            #[cfg(feature = "doh")]
            Self::Doh(r) => r.resolve(domain).await,
            #[cfg(feature = "dot")]
            Self::Dot(r) => r.resolve(domain).await,
        }
    }
}

// ── DoH resolver ──────────────────────────────────────────────────────

#[cfg(feature = "doh")]
pub(crate) struct DohDnsResolver {
    client: reqwest::Client,
    url: String,
}

#[cfg(feature = "doh")]
impl DohDnsResolver {
    fn new(url: String, _server_name: Option<String>) -> io::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(DNS_TIMEOUT)
            .build()
            .map_err(|e| io::Error::other(format!("failed to build doh client: {e}")))?;

        Ok(Self { client, url })
    }

    async fn resolve(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        // Try A record first, then AAAA.
        let mut ips = self.query(domain, 0x0001).await?;
        if ips.is_empty() {
            ips = self.query(domain, 0x001c).await?;
        }
        Ok(ips)
    }

    async fn query(&self, domain: &str, qtype: u16) -> io::Result<Vec<IpAddress>> {
        let msg = crate::udp::build_query(domain, qtype);

        let response = self
            .client
            .post(&self.url)
            .header("Content-Type", "application/dns-message")
            .header("Accept", "application/dns-message")
            .body(msg)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("doh request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(io::Error::other(format!(
                "doh server returned HTTP {status}"
            )));
        }

        let body = response
            .bytes()
            .await
            .map_err(|e| io::Error::other(format!("doh read failed: {e}")))?;

        crate::udp::parse_response(&body, qtype)
    }
}

// ── DoT resolver ──────────────────────────────────────────────────────

#[cfg(feature = "dot")]
pub(crate) struct DotDnsResolver {
    addr: SocketAddr,
    server_name: String,
    tls_config: Arc<rustls::ClientConfig>,
}

#[cfg(feature = "dot")]
impl DotDnsResolver {
    fn new(address: String, port: u16, server_name: Option<String>) -> io::Result<Self> {
        let ip = address.parse::<std::net::IpAddr>().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid dot address `{address}`: {e}"),
            )
        })?;
        let addr = SocketAddr::new(ip, port);

        let server_name = server_name.unwrap_or(address);
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );

        Ok(Self {
            addr,
            server_name,
            tls_config,
        })
    }

    async fn resolve(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        let mut ips = self.query(domain, 0x0001).await?;
        if ips.is_empty() {
            ips = self.query(domain, 0x001c).await?;
        }
        Ok(ips)
    }

    async fn query(&self, domain: &str, qtype: u16) -> io::Result<Vec<IpAddress>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let tcp_stream: TcpStream =
            tokio::time::timeout(DNS_TIMEOUT, TcpStream::connect(self.addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dot connect timeout"))??;

        let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid dot server_name: {e}"),
                )
            })?;

        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls_config));
        let mut tls_stream: tokio_rustls::client::TlsStream<TcpStream> =
            tokio::time::timeout(DNS_TIMEOUT, connector.connect(server_name, tcp_stream))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dot tls handshake timeout"))?
                .map_err(|e| io::Error::other(format!("dot tls failed: {e}")))?;

        // Build DNS query
        let msg = crate::udp::build_query(domain, qtype);

        // DoT framing: 2-byte big-endian length prefix + DNS message
        let len = msg.len() as u16;
        let mut frame = Vec::with_capacity(2 + msg.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&msg);

        tls_stream.write_all(&frame).await?;
        tls_stream.flush().await?;

        // Read response: 2-byte length prefix
        let mut len_buf = [0u8; 2];
        tls_stream.read_exact(&mut len_buf).await?;
        let resp_len = u16::from_be_bytes(len_buf) as usize;

        let mut resp = vec![0u8; resp_len];
        tls_stream.read_exact(&mut resp).await?;

        crate::udp::parse_response(&resp, qtype)
    }
}
