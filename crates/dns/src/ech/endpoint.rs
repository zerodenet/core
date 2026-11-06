//! Exact configured ECH DNS destinations; parsing grants no native authority.
use std::{io, net::IpAddr};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EchDnsEndpoint {
    pub(crate) url: url::Url,
    host: String,
    port: u16,
    datagram: bool,
}
impl EchDnsEndpoint {
    pub fn parse(server: &str) -> io::Result<Self> {
        let url = url::Url::parse(server).map_err(super::invalid_endpoint)?;
        let host = url
            .host_str()
            .ok_or_else(|| super::invalid_endpoint("ECH DNS endpoint has no host"))?;
        let (datagram, default_port) = match url.scheme() {
            "udp" => {
                if !matches!(url.path(), "" | "/") {
                    return Err(super::invalid_endpoint(
                        "UDP ECH DNS endpoint cannot have a path",
                    ));
                }
                (true, 53)
            }
            "https" => (false, 443),
            "h2c" => (false, 80),
            _ => {
                return Err(super::invalid_endpoint(
                    "unsupported ECH DNS endpoint scheme",
                ))
            }
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let port = url.port().unwrap_or(default_port);
        if port == 0 {
            return Err(super::invalid_endpoint("ECH DNS endpoint has zero port"));
        }
        Ok(Self {
            host: host.to_owned(),
            port,
            datagram,
            url,
        })
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn literal_peer(&self) -> Option<std::net::SocketAddr> {
        self.host
            .parse::<IpAddr>()
            .ok()
            .map(|ip| std::net::SocketAddr::new(ip, self.port))
    }
    pub fn is_datagram(&self) -> bool {
        self.datagram
    }
    pub fn as_str(&self) -> &str {
        self.url.as_str()
    }
}
