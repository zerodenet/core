use std::io;
use std::net::IpAddr;
#[cfg(feature = "udp")]
use std::net::SocketAddr;

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
        if self.owned_root.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "dynamic ECH endpoints require a separately authorized owned DNS root",
            ));
        }
        let query_name = crate::message::normalize_domain(query_name)?;
        let endpoint = url::Url::parse(server).map_err(invalid_endpoint)?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| invalid_endpoint("ECH DNS endpoint has no host"))?;
        let bootstrap = self.bootstrap(host).await?;
        let query = crate::message::build_query(&query_name, crate::message::TYPE_HTTPS)?;
        let response = self
            .exchange_ech_query(&endpoint, host, bootstrap, &query)
            .await?;
        let parsed = crate::message::parse_ech_config_response(&query, &response)?;
        Ok(EchDnsRecord {
            config_list: parsed.config_list,
            ttl_seconds: parsed.ttl_seconds,
        })
    }

    async fn exchange_ech_query(
        &self,
        endpoint: &url::Url,
        _host: &str,
        _bootstrap: Vec<IpAddr>,
        _query: &[u8],
    ) -> io::Result<Vec<u8>> {
        match endpoint.scheme() {
            #[cfg(feature = "udp")]
            "udp" => {
                if endpoint.path() != "" && endpoint.path() != "/" {
                    return Err(invalid_endpoint("UDP ECH DNS endpoint cannot have a path"));
                }
                let port = endpoint.port().unwrap_or(53);
                let addresses = _bootstrap
                    .iter()
                    .copied()
                    .map(|ip| SocketAddr::new(ip, port))
                    .collect();
                crate::udp::UdpDnsResolver::new(addresses, self.egress_interface.clone())
                    .exchange(_query)
                    .await
            }
            #[cfg(not(feature = "udp"))]
            "udp" => Err(unsupported("UDP")),
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
                        _host.to_owned(),
                        port,
                        path,
                        _bootstrap,
                        Some(_host.to_owned()),
                        self.egress_interface.clone(),
                    )?
                } else {
                    crate::backends::DohDnsResolver::new_cleartext(
                        _host.to_owned(),
                        port,
                        path,
                        _bootstrap,
                        self.egress_interface.clone(),
                    )?
                };
                // This temporary pool is a real resource too. Register before
                // any socket/driver I/O so caller cancellation cannot discard
                // the only JoinSet owner when the resolver is dropped.
                self.transport_owners.register_doh(resolver.owner.clone())?;
                let result = resolver.exchange(_query, None, None).await;
                let closed = resolver
                    .owner
                    .shutdown(self.ech_pool_close_deadline())
                    .await;
                match (result, closed) {
                    (Ok(response), Ok(())) => Ok(response),
                    (Err(source), _) => Err(source),
                    (Ok(_), Err(source)) => Err(source),
                }
            }
            #[cfg(not(feature = "doh"))]
            "https" | "h2c" => Err(unsupported("DNS-over-HTTPS")),
            _ => Err(invalid_endpoint("unsupported ECH DNS endpoint scheme")),
        }
    }

    #[cfg(feature = "doh")]
    fn ech_pool_close_deadline(&self) -> tokio::time::Instant {
        crate::backends::ATTEMPT_DEADLINE
            .try_with(|deadline| *deadline)
            .unwrap_or_else(|_| {
                // No server tag exists for an explicit ECH endpoint; use the
                // actual shared DNS policy budget, including its existing default.
                let timeout_ms = match &*self.inner.read().expect("DNS system lock poisoned") {
                    crate::DnsSystemInner::Configured { policy, .. } => policy.timeout_ms,
                    _ => zero_config::DnsPolicyConfig::default().timeout_ms,
                };
                tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms)
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

#[cfg(all(test, feature = "doh"))]
mod tests;
