use std::io;
use std::net::{IpAddr, SocketAddr};

use crate::DnsSystem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchDnsRecord {
    pub config_list: Option<Vec<u8>>,
    pub ttl_seconds: u32,
}

impl DnsSystem {
    /// Query the exact DNS endpoint encoded in an Xray-compatible ECH source.
    /// The endpoint bootstrap follows the node resolver and physical egress,
    /// while the HTTPS RR itself bypasses DNS dispatch to avoid recursion.
    pub async fn query_ech_config(
        &self,
        query_name: &str,
        server: &str,
    ) -> io::Result<EchDnsRecord> {
        let query_name = crate::message::normalize_domain(query_name)?;
        let endpoint = url::Url::parse(server).map_err(invalid_endpoint)?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| invalid_endpoint("ECH DNS endpoint has no host"))?;
        let bootstrap = self.bootstrap(host).await?;
        let query = crate::message::build_query(&query_name, crate::message::TYPE_HTTPS)?;
        let response = match endpoint.scheme() {
            #[cfg(feature = "udp")]
            "udp" => {
                if endpoint.path() != "" && endpoint.path() != "/" {
                    return Err(invalid_endpoint("UDP ECH DNS endpoint cannot have a path"));
                }
                let port = endpoint.port().unwrap_or(53);
                let addresses = bootstrap
                    .iter()
                    .copied()
                    .map(|ip| SocketAddr::new(ip, port))
                    .collect();
                crate::udp::UdpDnsResolver::new(addresses, self.egress_interface.clone())
                    .exchange(&query)
                    .await?
            }
            #[cfg(not(feature = "udp"))]
            "udp" => return Err(unsupported("UDP")),
            #[cfg(feature = "doh")]
            "https" | "h2c" => {
                let secure = endpoint.scheme() == "https";
                let port = endpoint.port().unwrap_or(if secure { 443 } else { 80 });
                let mut path = endpoint.path().to_owned();
                if path.is_empty() {
                    path.push('/');
                }
                if let Some(query) = endpoint.query() {
                    path.push('?');
                    path.push_str(query);
                }
                let resolver = if secure {
                    crate::backends::DohDnsResolver::new(
                        host.to_owned(),
                        port,
                        path,
                        bootstrap,
                        Some(host.to_owned()),
                        self.egress_interface.clone(),
                    )?
                } else {
                    crate::backends::DohDnsResolver::new_cleartext(
                        host.to_owned(),
                        port,
                        path,
                        bootstrap,
                        self.egress_interface.clone(),
                    )?
                };
                resolver.exchange(&query, None, None).await?
            }
            #[cfg(not(feature = "doh"))]
            "https" | "h2c" => return Err(unsupported("DNS-over-HTTPS")),
            _ => return Err(invalid_endpoint("unsupported ECH DNS endpoint scheme")),
        };
        let parsed = crate::message::parse_ech_config_response(&query, &response)?;
        Ok(EchDnsRecord {
            config_list: parsed.config_list,
            ttl_seconds: parsed.ttl_seconds,
        })
    }

    async fn bootstrap(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        if let Ok(ip) = host.parse() {
            return Ok(vec![ip]);
        }
        let addresses = self.resolve_node(host).await?;
        let addresses = addresses
            .into_iter()
            .map(|address| match address {
                zero_traits::IpAddress::V4(value) => IpAddr::from(value),
                zero_traits::IpAddress::V6(value) => IpAddr::from(value),
            })
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "ECH DNS endpoint bootstrap returned no addresses",
            ));
        }
        Ok(addresses)
    }
}

fn invalid_endpoint(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

#[allow(dead_code)]
fn unsupported(transport: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{transport} ECH DNS support is not compiled"),
    )
}
