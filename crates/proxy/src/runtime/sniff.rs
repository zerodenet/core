use std::net::IpAddr;

use regex::Regex;
use zero_core::{Address, InboundMuxTcpRelay, Session, TargetHostSource};

#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod udp;

pub(crate) const SNIFF_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);
pub(crate) const MAX_SNIFF_BYTES: usize = 32_767;
const MAX_TLS_RECORD_LENGTH: usize = 18_432;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SniffedProtocol {
    Http,
    Tls,
    Quic,
    FakeDns,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SniffedDomain {
    pub(crate) domain: String,
    pub(crate) protocol: SniffedProtocol,
    pub(crate) source: TargetHostSource,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SniffProgress {
    Pending,
    Domain(SniffedDomain),
    NoMatch,
}

#[derive(Clone)]
enum DomainExclusion {
    Exact(String),
    Regex(Regex),
}

#[derive(Clone, Default)]
pub(crate) struct SniffingPolicy {
    enabled: bool,
    http: bool,
    tls: bool,
    quic: bool,
    fake_dns: bool,
    excluded: Vec<DomainExclusion>,
    metadata_only: bool,
    route_only: bool,
}

impl SniffingPolicy {
    pub(crate) fn new(
        enabled: bool,
        destination_override: &[String],
        domains_excluded: &[String],
        metadata_only: bool,
        route_only: bool,
    ) -> Result<Self, regex::Error> {
        let mut policy = Self {
            enabled,
            metadata_only,
            route_only,
            ..Self::default()
        };
        for protocol in destination_override {
            match protocol.to_ascii_lowercase().as_str() {
                "http" => policy.http = true,
                "tls" | "https" | "ssl" => policy.tls = true,
                "quic" => policy.quic = true,
                "fakedns" | "fakedns+others" => policy.fake_dns = true,
                _ => {}
            }
        }
        for excluded in domains_excluded {
            let excluded = excluded.to_ascii_lowercase();
            if let Some(pattern) = excluded.strip_prefix("regexp:") {
                policy
                    .excluded
                    .push(DomainExclusion::Regex(Regex::new(pattern)?));
            } else {
                policy.excluded.push(DomainExclusion::Exact(excluded));
            }
        }
        Ok(policy)
    }

    pub(crate) fn reads_payload(&self, fake_dns_fallback: bool) -> bool {
        self.enabled
            && !self.metadata_only
            && (self.http || self.tls || self.quic || (self.fake_dns && fake_dns_fallback))
    }

    pub(crate) fn sniffs_tcp(&self, fake_dns_fallback: bool) -> bool {
        self.reads_payload(fake_dns_fallback)
            && (self.http || self.tls || (self.fake_dns && fake_dns_fallback))
    }

    pub(crate) fn sniffs_quic(&self, fake_dns_fallback: bool) -> bool {
        self.reads_payload(fake_dns_fallback) && (self.quic || (self.fake_dns && fake_dns_fallback))
    }

    pub(crate) fn route_only(&self) -> bool {
        self.route_only
    }

    pub(crate) fn accepts(&self, sniffed: &SniffedDomain) -> bool {
        self.accepts_with_fake_dns_fallback(sniffed, false)
    }

    pub(crate) fn accepts_with_fake_dns_fallback(
        &self,
        sniffed: &SniffedDomain,
        fake_dns_fallback: bool,
    ) -> bool {
        let protocol_enabled = match sniffed.protocol {
            SniffedProtocol::Http => self.http,
            SniffedProtocol::Tls => self.tls,
            SniffedProtocol::Quic => self.quic,
            SniffedProtocol::FakeDns => self.fake_dns,
        };
        (protocol_enabled || (self.fake_dns && fake_dns_fallback))
            && !self.excluded.iter().any(|excluded| match excluded {
                DomainExclusion::Exact(domain) => domain == &sniffed.domain,
                DomainExclusion::Regex(pattern) => pattern.is_match(&sniffed.domain),
            })
    }

    pub(crate) fn apply_to_session(&self, session: &mut Session, sniffed: SniffedDomain) {
        self.apply_to_session_with_fake_dns_fallback(session, sniffed, false);
    }

    fn apply_to_session_with_fake_dns_fallback(
        &self,
        session: &mut Session,
        sniffed: SniffedDomain,
        fake_dns_fallback: bool,
    ) {
        if !self.accepts_with_fake_dns_fallback(&sniffed, fake_dns_fallback) {
            return;
        }
        let domain = Address::Domain(sniffed.domain.clone());
        if self.route_only && sniffed.protocol != SniffedProtocol::FakeDns && !fake_dns_fallback {
            session.route_target = Some(domain);
        } else {
            if session.original_target.is_none() {
                session.original_target = Some(session.target.clone());
            }
            session.target = domain;
            session.target_host_source = Some(sniffed.source);
        }
        if matches!(
            sniffed.protocol,
            SniffedProtocol::Tls | SniffedProtocol::Quic
        ) {
            session.sni = Some(sniffed.domain);
        }
    }

    pub(crate) async fn apply_fake_dns_metadata(
        &self,
        resolver: &zero_dns::DnsSystem,
        session: &mut Session,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        session.skip_fake_ip_restore = true;
        match self.fake_dns_metadata(resolver, &session.target).await {
            FakeDnsMetadata::Domain(sniffed) => {
                self.apply_to_session(session, sniffed);
                false
            }
            FakeDnsMetadata::ContentFallback => !self.metadata_only,
            FakeDnsMetadata::None => false,
        }
    }

    pub(crate) async fn fake_dns_metadata(
        &self,
        resolver: &zero_dns::DnsSystem,
        target: &Address,
    ) -> FakeDnsMetadata {
        if !self.enabled || !self.fake_dns {
            return FakeDnsMetadata::None;
        }
        let Some((ip, standard_ip)) = address_ip(target) else {
            return FakeDnsMetadata::None;
        };
        if !resolver.fake_ip_contains(standard_ip) {
            return FakeDnsMetadata::None;
        }
        match resolver.lookup_fake_ip(&ip).await {
            Some(domain) => {
                normalize_sniffed_domain(domain).map_or(FakeDnsMetadata::None, |domain| {
                    FakeDnsMetadata::Domain(SniffedDomain {
                        domain,
                        protocol: SniffedProtocol::FakeDns,
                        source: TargetHostSource::FakeIp,
                    })
                })
            }
            None => FakeDnsMetadata::ContentFallback,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }
}

pub(crate) enum FakeDnsMetadata {
    None,
    Domain(SniffedDomain),
    ContentFallback,
}

pub(crate) fn sniff_tcp_prefix(
    prefix: &[u8],
    policy: &SniffingPolicy,
    fake_dns_fallback: bool,
) -> SniffProgress {
    if prefix.is_empty() {
        return SniffProgress::Pending;
    }
    if (policy.tls || (policy.fake_dns && fake_dns_fallback)) && prefix[0] == 0x16 {
        return sniff_tls_records(prefix);
    }
    if (policy.http || (policy.fake_dns && fake_dns_fallback)) && prefix[0].is_ascii_uppercase() {
        return sniff_http(prefix);
    }
    SniffProgress::NoMatch
}

pub(crate) async fn sniff_mux_tcp_session<R>(
    policy: &SniffingPolicy,
    session: &mut Session,
    relay: &mut R,
    fake_dns_fallback: bool,
) -> Vec<u8>
where
    R: InboundMuxTcpRelay,
{
    if !policy.sniffs_tcp(fake_dns_fallback) {
        return Vec::new();
    }
    let mut prefix = Vec::new();
    let sniff = async {
        loop {
            let remaining = MAX_SNIFF_BYTES.saturating_sub(prefix.len());
            if remaining == 0 {
                break;
            }
            match relay.read_inbound_chunk(remaining).await {
                Ok(Some(chunk)) if !chunk.is_empty() => prefix.extend_from_slice(&chunk),
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break,
            }
            match sniff_tcp_prefix(&prefix, policy, fake_dns_fallback) {
                SniffProgress::Domain(sniffed) => {
                    policy.apply_to_session_with_fake_dns_fallback(
                        session,
                        sniffed,
                        fake_dns_fallback,
                    );
                    break;
                }
                SniffProgress::NoMatch => break,
                SniffProgress::Pending => {}
            }
        }
    };
    let _ = tokio::time::timeout(SNIFF_TIMEOUT, sniff).await;
    prefix
}

fn address_ip(address: &Address) -> Option<(zero_traits::IpAddress, IpAddr)> {
    match address {
        Address::Ipv4(octets) => Some((
            zero_traits::IpAddress::V4(*octets),
            IpAddr::V4((*octets).into()),
        )),
        Address::Ipv6(octets) => Some((
            zero_traits::IpAddress::V6(*octets),
            IpAddr::V6((*octets).into()),
        )),
        Address::Domain(_) => None,
    }
}

pub(crate) fn sniff_tls_handshake(handshake: &[u8]) -> SniffProgress {
    if handshake.len() < 4 {
        return SniffProgress::Pending;
    }
    if handshake[0] != 0x01 {
        return SniffProgress::NoMatch;
    }
    let length =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if length > MAX_SNIFF_BYTES - 4 {
        return SniffProgress::NoMatch;
    }
    let Some(client_hello) = handshake.get(4..4 + length) else {
        return SniffProgress::Pending;
    };
    let Some((sni, encrypted_client_hello)) = parse_client_hello(client_hello) else {
        return SniffProgress::NoMatch;
    };
    if encrypted_client_hello {
        return SniffProgress::NoMatch;
    }
    sni.and_then(normalize_sniffed_domain)
        .map_or(SniffProgress::NoMatch, |domain| {
            SniffProgress::Domain(SniffedDomain {
                domain,
                protocol: SniffedProtocol::Tls,
                source: TargetHostSource::TlsSni,
            })
        })
}

fn sniff_tls_records(prefix: &[u8]) -> SniffProgress {
    let mut offset = 0;
    let mut handshake = Vec::new();
    loop {
        let Some(header) = prefix.get(offset..offset + 5) else {
            return SniffProgress::Pending;
        };
        if header[0] != 0x16 {
            return SniffProgress::NoMatch;
        }
        let length = u16::from_be_bytes([header[3], header[4]]) as usize;
        if length == 0 || length > MAX_TLS_RECORD_LENGTH {
            return SniffProgress::NoMatch;
        }
        let Some(record) = prefix.get(offset + 5..offset + 5 + length) else {
            return SniffProgress::Pending;
        };
        handshake.extend_from_slice(record);
        match sniff_tls_handshake(&handshake) {
            SniffProgress::Pending => {
                offset += 5 + length;
                if offset >= prefix.len() || handshake.len() >= MAX_SNIFF_BYTES {
                    return SniffProgress::Pending;
                }
            }
            result => return result,
        }
    }
}

fn sniff_http(prefix: &[u8]) -> SniffProgress {
    if prefix.len() > MAX_SNIFF_BYTES {
        return SniffProgress::NoMatch;
    }
    let Some(end) = prefix
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
    else {
        return if prefix.len() < 16 || looks_like_http_request(prefix) {
            SniffProgress::Pending
        } else {
            SniffProgress::NoMatch
        };
    };
    parse_http_host(&prefix[..end])
        .and_then(|domain| normalize_sniffed_domain(domain.to_owned()))
        .map_or(SniffProgress::NoMatch, |domain| {
            SniffProgress::Domain(SniffedDomain {
                domain,
                protocol: SniffedProtocol::Http,
                source: TargetHostSource::HttpHost,
            })
        })
}

fn looks_like_http_request(bytes: &[u8]) -> bool {
    let Some(space) = bytes.iter().position(|byte| *byte == b' ') else {
        return bytes
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || *byte == b'-');
    };
    space > 0
        && bytes[..space]
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || *byte == b'-')
}

fn parse_http_host(headers: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(headers).ok()?;
    let mut lines = text.split("\r\n");
    let mut request = lines.next()?.split_whitespace();
    let method = request.next()?;
    let target = request.next()?;
    let version = request.next()?;
    if request.next().is_some()
        || method.is_empty()
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'-')
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
    {
        return None;
    }
    lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("host").then(|| value.trim())
        })
        .or_else(|| absolute_form_authority(target))
        .and_then(strip_http_port)
}

fn absolute_form_authority(target: &str) -> Option<&str> {
    let remainder = target
        .strip_prefix("http://")
        .or_else(|| target.strip_prefix("https://"))?;
    let authority = remainder.split(['/', '?', '#']).next()?;
    (!authority.is_empty() && !authority.contains('@')).then_some(authority)
}

fn strip_http_port(authority: &str) -> Option<&str> {
    if authority.starts_with('[') {
        return None;
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => Some(host),
        Some(_) => None,
        None => Some(authority),
    }
}

fn parse_client_hello(client_hello: &[u8]) -> Option<(Option<String>, bool)> {
    let mut offset = 34;
    let session_id_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, session_id_length)?;
    let cipher_suites_length = take_u16(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, cipher_suites_length)?;
    let compression_methods_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, compression_methods_length)?;
    if offset == client_hello.len() {
        return Some((None, false));
    }
    let extensions_length = take_u16(client_hello, &mut offset)? as usize;
    let extensions = take(client_hello, &mut offset, extensions_length)?;
    let mut extension_offset = 0;
    let mut sni = None;
    let mut ech = false;
    while extension_offset + 4 <= extensions.len() {
        let kind = u16::from_be_bytes([
            extensions[extension_offset],
            extensions[extension_offset + 1],
        ]);
        let length = u16::from_be_bytes([
            extensions[extension_offset + 2],
            extensions[extension_offset + 3],
        ]) as usize;
        extension_offset += 4;
        let extension = extensions.get(extension_offset..extension_offset + length)?;
        if kind == 0 && extension.len() >= 5 && extension[2] == 0 {
            let name_length = u16::from_be_bytes([extension[3], extension[4]]) as usize;
            sni = std::str::from_utf8(extension.get(5..5 + name_length)?)
                .ok()
                .map(ToOwned::to_owned);
        } else if kind == 0xfe0d {
            ech = true;
        }
        extension_offset += length;
    }
    Some((sni, ech))
}

pub(crate) fn normalize_sniffed_domain(domain: String) -> Option<String> {
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();
    if domain.is_empty()
        || domain.len() > 253
        || domain.parse::<IpAddr>().is_ok()
        || domain.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return None;
    }
    Some(domain)
}

fn take_u8(bytes: &[u8], offset: &mut usize) -> Option<u8> {
    Some(take(bytes, offset, 1)?[0])
}

fn take_u16(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    let bytes = take(bytes, offset, 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = offset.checked_add(length)?;
    let value = bytes.get(*offset..end)?;
    *offset = end;
    Some(value)
}

#[cfg(test)]
#[path = "sniff/tests.rs"]
mod tests;
