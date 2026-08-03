use alloc::format;
use alloc::string::{String, ToString};
use core::net::{Ipv4Addr, Ipv6Addr};
use core::str::FromStr;

use zero_core::{Address, Error};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedHttpRequestLine {
    Connect {
        target: Address,
        port: u16,
    },
    Forward {
        target: Address,
        port: u16,
        origin_form_line: String,
    },
}

pub(crate) fn first_line(request: &[u8]) -> Result<&str, Error> {
    let request = core::str::from_utf8(request)
        .map_err(|_| Error::Protocol("HTTP request is not valid UTF-8"))?;

    request
        .split("\r\n")
        .next()
        .filter(|line| !line.is_empty())
        .ok_or(Error::Protocol("HTTP request line is missing"))
}

pub(crate) fn parse_request_line(line: &str) -> Result<ParsedHttpRequestLine, Error> {
    let mut parts = line.split_ascii_whitespace();
    let method = parts
        .next()
        .ok_or(Error::Protocol("HTTP method is missing"))?;
    let request_target = parts
        .next()
        .ok_or(Error::Protocol("HTTP request target is missing"))?;
    let version = parts
        .next()
        .ok_or(Error::Protocol("HTTP version is missing"))?;

    if parts.next().is_some() {
        return Err(Error::Protocol(
            "HTTP request line contains unexpected fields",
        ));
    }

    if !version.starts_with("HTTP/") {
        return Err(Error::Protocol("HTTP version is invalid"));
    }

    if method == "CONNECT" {
        let (target, port) = parse_authority(request_target, None)?;
        return Ok(ParsedHttpRequestLine::Connect { target, port });
    }

    let (target, port, origin_form) = parse_absolute_http_uri(request_target)?;
    Ok(ParsedHttpRequestLine::Forward {
        target,
        port,
        origin_form_line: format!("{method} {origin_form} {version}\r\n"),
    })
}

fn parse_absolute_http_uri(uri: &str) -> Result<(Address, u16, String), Error> {
    let Some(scheme) = uri.get(..7) else {
        return Err(Error::Protocol(
            "HTTP forward-proxy request target must use absolute-form",
        ));
    };
    if !scheme.eq_ignore_ascii_case("http://") {
        return Err(Error::Unsupported(
            "HTTP forward-proxy scheme is not supported",
        ));
    }

    let remainder = &uri[7..];
    if remainder.is_empty() {
        return Err(Error::Protocol(
            "HTTP forward-proxy authority must not be empty",
        ));
    }
    if remainder.contains('#') {
        return Err(Error::Protocol(
            "HTTP forward-proxy request target must not contain a fragment",
        ));
    }

    let target_start = remainder
        .find(|character| character == '/' || character == '?')
        .unwrap_or(remainder.len());
    let authority = &remainder[..target_start];
    let suffix = &remainder[target_start..];

    if authority.contains('@') {
        return Err(Error::Protocol(
            "HTTP forward-proxy authority must not contain user information",
        ));
    }

    let (target, port) = parse_authority(authority, Some(80))?;
    let origin_form = if suffix.is_empty() {
        "/".to_string()
    } else if suffix.starts_with('?') {
        format!("/{suffix}")
    } else {
        suffix.to_string()
    };

    Ok((target, port, origin_form))
}

fn parse_authority(authority: &str, default_port: Option<u16>) -> Result<(Address, u16), Error> {
    if authority.is_empty() {
        return Err(Error::Protocol("HTTP authority must not be empty"));
    }

    if let Some(rest) = authority.strip_prefix('[') {
        let end = rest
            .find(']')
            .ok_or(Error::Protocol("HTTP IPv6 authority is malformed"))?;
        let host = &rest[..end];
        let suffix = &rest[end + 1..];
        let port = if suffix.is_empty() {
            default_port.ok_or(Error::Protocol("HTTP authority port is missing"))?
        } else {
            let port = suffix
                .strip_prefix(':')
                .ok_or(Error::Protocol("HTTP IPv6 authority is malformed"))?;
            parse_port(port)?
        };

        let addr = Ipv6Addr::from_str(host)
            .map_err(|_| Error::Protocol("HTTP IPv6 address is invalid"))?;

        return Ok((Address::Ipv6(addr.octets()), port));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            if host.contains(':') {
                return Err(Error::Protocol("HTTP IPv6 authority must use brackets"));
            }
            (host, parse_port(port)?)
        }
        None => (
            authority,
            default_port.ok_or(Error::Protocol("HTTP authority port is missing"))?,
        ),
    };

    if let Ok(addr) = Ipv4Addr::from_str(host) {
        return Ok((Address::Ipv4(addr.octets()), port));
    }

    if host.is_empty() {
        return Err(Error::Protocol("HTTP host must not be empty"));
    }

    Ok((Address::Domain(host.to_string()), port))
}

fn parse_port(port: &str) -> Result<u16, Error> {
    port.parse::<u16>()
        .map_err(|_| Error::Protocol("HTTP port is invalid"))
}
