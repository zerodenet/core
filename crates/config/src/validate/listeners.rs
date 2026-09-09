//! Auxiliary TCP claims share the authoritative inbound conflict check.
use crate::{ConfigError, InboundProtocolConfig, ListenConfig, RuntimeConfig};
pub(super) fn validate_auxiliary_listeners(config: &RuntimeConfig) -> Result<(), ConfigError> {
    let mut claims: Vec<(&ListenConfig, bool)> = Vec::new();
    for inbound in &config.inbounds {
        let quic = matches!(
            &inbound.protocol,
            InboundProtocolConfig::Hysteria2 { .. }
                | InboundProtocolConfig::Vless { quic: Some(_), .. }
        );
        if !quic {
            claims.push((&inbound.listen, false));
        }
        if let InboundProtocolConfig::Hysteria2 { masquerade, .. } = &inbound.protocol {
            claims.extend(
                masquerade
                    .http
                    .iter()
                    .chain(masquerade.https.iter())
                    .map(|listen| (listen, true)),
            );
        }
    }
    for (i, (listen, auxiliary)) in claims.iter().enumerate() {
        for (other, other_auxiliary) in &claims[..i] {
            if (*auxiliary || *other_auxiliary)
                && listen.port == other.port
                && overlaps(&listen.address, &other.address)
            {
                return Err(ConfigError::DuplicateInboundListen {
                    address: listen.address.clone(),
                    port: listen.port,
                });
            }
        }
    }
    Ok(())
}
fn overlaps(a: &str, b: &str) -> bool {
    let a = a.trim_matches(['[', ']']);
    let b = b.trim_matches(['[', ']']);
    if a.eq_ignore_ascii_case(b) {
        return true;
    }
    match (a.parse::<std::net::IpAddr>(), b.parse::<std::net::IpAddr>()) {
        (Ok(a), Ok(b)) => {
            let a = a.to_canonical();
            let b = b.to_canonical();
            // IPv6 wildcard sockets can also claim IPv4; reject the portable conflict.
            a == b
                || ((a.is_unspecified() || b.is_unspecified()) && a.is_ipv4() == b.is_ipv4())
                || (a.is_ipv6() && a.is_unspecified())
                || (b.is_ipv6() && b.is_unspecified())
        }
        _ => false,
    }
}
