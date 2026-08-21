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
    pub(crate) fn build(
        server: &DnsServerConfig,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let _ = &egress_interface;
        match server {
            DnsServerConfig::System => Ok(Self::System(TokioSystemResolver)),
            #[cfg(feature = "udp")]
            DnsServerConfig::Udp { port, .. } => {
                let address = server_endpoint(server)?;
                Ok(Self::Udp(UdpDnsResolver::new(
                    std::net::SocketAddr::new(address, *port),
                    egress_interface,
                )))
            }
            #[cfg(not(feature = "udp"))]
            DnsServerConfig::Udp { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UDP DNS backend is not compiled (enable feature `udp`)",
            )),
            #[cfg(feature = "doh")]
            DnsServerConfig::Doh {
                host,
                port,
                path,
                bootstrap,
                server_name,
            } => Ok(Self::Doh(DohDnsResolver::new(
                host.clone(),
                *port,
                path.clone(),
                bootstrap.clone(),
                server_name.clone(),
                egress_interface,
            )?)),
            #[cfg(not(feature = "doh"))]
            DnsServerConfig::Doh { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS-over-HTTPS is not compiled (enable feature `doh`)",
            )),
            #[cfg(feature = "dot")]
            DnsServerConfig::Dot {
                host,
                port,
                bootstrap,
                server_name,
            } => Ok(Self::Dot(DotDnsResolver::new(
                host.clone(),
                *port,
                bootstrap.clone(),
                server_name.clone(),
                egress_interface,
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

fn server_endpoint(server: &DnsServerConfig) -> io::Result<std::net::IpAddr> {
    server
        .endpoint_addresses()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "DNS endpoint is empty"))
}

// ── DoH resolver ──────────────────────────────────────────────────────

#[cfg(feature = "doh")]
pub(crate) struct DohDnsResolver {
    url: String,
    authority: String,
    port: u16,
    bootstrap: Vec<std::net::IpAddr>,
    egress_interface: zero_platform_tokio::EgressInterfaceControl,
    client: std::sync::Mutex<DohClientState>,
}

#[cfg(feature = "doh")]
struct DohClientState {
    egress_interface: Option<zero_platform_tokio::EgressInterface>,
    client: reqwest::Client,
}

#[cfg(feature = "doh")]
impl DohDnsResolver {
    fn new(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let bootstrap = if bootstrap.is_empty() {
            host.parse::<std::net::IpAddr>()
                .map(|address| vec![address])
                .unwrap_or_default()
        } else {
            bootstrap
        };
        let authority = server_name.as_deref().unwrap_or(&host).to_owned();
        let url_authority = if authority.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{authority}]")
        } else {
            authority.clone()
        };
        let url = format!("https://{url_authority}:{port}{path}");
        let selected = doh_egress_interface(&egress_interface);
        let client = build_doh_client(selected.as_ref(), &authority, port, &bootstrap)?;
        Ok(Self {
            url,
            authority,
            port,
            bootstrap,
            egress_interface,
            client: std::sync::Mutex::new(DohClientState {
                egress_interface: selected,
                client,
            }),
        })
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
        let client = self.client()?;

        let response = client
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

    fn client(&self) -> io::Result<reqwest::Client> {
        let selected = doh_egress_interface(&self.egress_interface);
        let mut state = self.client.lock().expect("DoH client lock poisoned");
        if state.egress_interface != selected {
            state.client = build_doh_client(
                selected.as_ref(),
                &self.authority,
                self.port,
                &self.bootstrap,
            )?;
            state.egress_interface = selected;
        }
        Ok(state.client.clone())
    }
}

#[cfg(feature = "doh")]
fn build_doh_client(
    interface: Option<&zero_platform_tokio::EgressInterface>,
    authority: &str,
    port: u16,
    bootstrap: &[std::net::IpAddr],
) -> io::Result<reqwest::Client> {
    let client = reqwest::Client::builder().timeout(DNS_TIMEOUT);
    let bootstrap_addresses = bootstrap
        .iter()
        .map(|address| std::net::SocketAddr::new(*address, port))
        .collect::<Vec<_>>();
    let client = if bootstrap_addresses.is_empty() || authority.parse::<std::net::IpAddr>().is_ok()
    {
        client
    } else {
        client.resolve_to_addrs(authority, &bootstrap_addresses)
    };
    #[cfg(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    ))]
    let client = match interface {
        Some(interface) => client.interface(interface.name()),
        None => client,
    };
    #[cfg(not(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    )))]
    let _ = interface;
    client
        .build()
        .map_err(|error| io::Error::other(format!("failed to build doh client: {error}")))
}

#[cfg(all(
    feature = "doh",
    any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    )
))]
fn doh_egress_interface(
    control: &zero_platform_tokio::EgressInterfaceControl,
) -> Option<zero_platform_tokio::EgressInterface> {
    control.current()
}

#[cfg(all(
    feature = "doh",
    not(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    ))
))]
fn doh_egress_interface(
    _control: &zero_platform_tokio::EgressInterfaceControl,
) -> Option<zero_platform_tokio::EgressInterface> {
    None
}

// ── DoT resolver ──────────────────────────────────────────────────────

#[cfg(feature = "dot")]
pub(crate) struct DotDnsResolver {
    addr: SocketAddr,
    server_name: String,
    tls_config: Arc<rustls::ClientConfig>,
    egress_interface: zero_platform_tokio::EgressInterfaceControl,
}

#[cfg(feature = "dot")]
impl DotDnsResolver {
    fn new(
        host: String,
        port: u16,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let ip = bootstrap
            .first()
            .copied()
            .or_else(|| host.parse::<std::net::IpAddr>().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("DoT host `{host}` requires a bootstrap address"),
                )
            })?;
        let addr = SocketAddr::new(ip, port);

        let server_name = server_name.unwrap_or(host);
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
            egress_interface,
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
        let interface = self.egress_interface.current_for_peer(self.addr);
        let tcp_stream = tokio::time::timeout(
            DNS_TIMEOUT,
            zero_platform_tokio::TokioSocket::connect_addr_on(self.addr, interface.as_ref()),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dot connect timeout"))??;
        let selected = tcp_stream.egress_interface();
        tracing::debug!(
            server = %self.addr,
            local = ?tcp_stream.local_addr().ok(),
            egress_name = selected.map(zero_platform_tokio::EgressInterface::name),
            egress_index = selected.map(zero_platform_tokio::EgressInterface::index),
            "DNS-over-TLS socket connected"
        );

        let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid dot server_name: {e}"),
                )
            })?;

        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls_config));
        let mut tls_stream =
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
