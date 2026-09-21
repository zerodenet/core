use core::{net::IpAddr, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint<'a> {
    pub host: &'a str,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointError {
    InvalidFormat,
    InvalidHost,
    InvalidPort,
}

pub fn parse_endpoint(value: &str) -> Result<Endpoint<'_>, EndpointError> {
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or(EndpointError::InvalidFormat)?;
        if IpAddr::from_str(host).is_err() || !host.contains(':') {
            return Err(EndpointError::InvalidHost);
        }
        (host, port)
    } else {
        let (host, port) = value.rsplit_once(':').ok_or(EndpointError::InvalidFormat)?;
        if host.contains(':') {
            return Err(EndpointError::InvalidFormat);
        }
        (host, port)
    };
    if !valid_host(host) {
        return Err(EndpointError::InvalidHost);
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| EndpointError::InvalidPort)?;
    if port == 0 {
        return Err(EndpointError::InvalidPort);
    }
    Ok(Endpoint { host, port })
}

fn valid_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    if IpAddr::from_str(host).is_ok() {
        return true;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}
