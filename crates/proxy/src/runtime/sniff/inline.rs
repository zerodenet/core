//! Same borrowed ClientHello fields and finite normalized SNI, without a String cache.
use super::*;
#[cfg(feature = "managed-stream-runtime")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InlineName {
    bytes: [u8; 253],
    length: u8,
}
#[cfg(feature = "managed-stream-runtime")]
impl InlineName {
    pub(crate) fn new(domain: &str) -> Option<Self> {
        let domain = domain.trim_end_matches('.');
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
        let mut name = Self {
            bytes: [0; 253],
            length: domain.len() as u8,
        };
        for (target, byte) in name.bytes.iter_mut().zip(domain.bytes()) {
            *target = byte.to_ascii_lowercase();
        }
        Some(name)
    }
    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.length)])
            .expect("same normalized ASCII domain")
    }
}
#[cfg(feature = "managed-stream-runtime")]
pub(crate) enum NameProgress {
    Pending,
    Domain(InlineName),
    NoMatch,
}
#[cfg(feature = "managed-stream-runtime")]
pub(crate) fn sniff_tls_name(handshake: &[u8]) -> NameProgress {
    if handshake.len() < 4 {
        return NameProgress::Pending;
    }
    if handshake[0] != 1 {
        return NameProgress::NoMatch;
    }
    let length =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if length > MAX_SNIFF_BYTES - 4 {
        return NameProgress::NoMatch;
    }
    let Some(hello) = handshake.get(4..4 + length) else {
        return NameProgress::Pending;
    };
    let Some((name, ech)) = parse_client_hello_ref(hello) else {
        return NameProgress::NoMatch;
    };
    if ech {
        return NameProgress::NoMatch;
    }
    name.and_then(InlineName::new)
        .map_or(NameProgress::NoMatch, NameProgress::Domain)
}
#[cfg(feature = "managed-stream-runtime")]
impl SniffingPolicy {
    pub(crate) fn accepts_name(
        &self,
        protocol: SniffedProtocol,
        name: &str,
        fallback: bool,
    ) -> bool {
        let enabled = match protocol {
            SniffedProtocol::Http => self.http,
            SniffedProtocol::Tls => self.tls,
            SniffedProtocol::Quic => self.quic,
            SniffedProtocol::FakeDns => self.fake_dns,
        };
        (enabled || (self.fake_dns && fallback))
            && !self.excluded.iter().any(|excluded| match excluded {
                DomainExclusion::Exact(domain) => domain == name,
                DomainExclusion::Regex(pattern) => pattern.is_match(name),
            })
    }
}
pub(super) fn parse_client_hello_ref(client_hello: &[u8]) -> Option<(Option<&str>, bool)> {
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
            sni = std::str::from_utf8(extension.get(5..5 + name_length)?).ok();
        } else if kind == 0xfe0d {
            ech = true;
        }
        extension_offset += length;
    }
    Some((sni, ech))
}

#[cfg(all(test, feature = "managed-stream-runtime"))]
#[path = "inline/tests.rs"]
mod tests;
