//! Auxiliary TCP claims share the authoritative inbound conflict check.
use crate::{ConfigError, InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use std::collections::HashSet;
use zero_core::address::listen_hosts_overlap;

#[derive(Clone)]
struct Claim {
    address: String,
    port: u16,
    auxiliary: bool,
    browser: Option<zero_traits::BrowserDialerSettings>,
}

pub(super) fn validate_auxiliary_listeners(config: &RuntimeConfig) -> Result<(), ConfigError> {
    let mut claims = Vec::new();
    let mut browser_settings = HashSet::new();
    for inbound in &config.inbounds {
        let quic = matches!(
            &inbound.protocol,
            InboundProtocolConfig::Hysteria2 { .. }
                | InboundProtocolConfig::Vless { quic: Some(_), .. }
        );
        if !quic {
            claims.push(Claim {
                address: inbound.listen.address.clone(),
                port: inbound.listen.port,
                auxiliary: false,
                browser: None,
            });
        }
        if let InboundProtocolConfig::Hysteria2 { masquerade, .. } = &inbound.protocol {
            claims.extend(
                masquerade
                    .http
                    .iter()
                    .chain(masquerade.https.iter())
                    .map(|listen| Claim {
                        address: listen.address.clone(),
                        port: listen.port,
                        auxiliary: true,
                        browser: None,
                    }),
            );
        }
    }
    for outbound in &config.outbounds {
        let OutboundProtocolConfig::Vless { ws, split_http, .. } = &outbound.protocol else {
            continue;
        };
        for browser in [
            ws.as_deref()
                .and_then(|profile| profile.browser_dialer.as_ref()),
            split_http
                .as_deref()
                .and_then(|profile| profile.browser_dialer.as_ref()),
        ]
        .into_iter()
        .flatten()
        {
            let settings = browser.settings();
            if browser_settings.insert(settings.clone()) && browser_settings.len() > 64 {
                return Err(ConfigError::InvalidOutbound(
                    "at most 64 distinct Browser Dialer listeners are supported".to_owned(),
                ));
            }
            let address = settings
                .listen
                .parse::<std::net::SocketAddr>()
                .map_err(|_| {
                    ConfigError::InvalidOutbound(
                        "browser_dialer.listen must be an IP socket address".to_owned(),
                    )
                })?;
            claims.push(Claim {
                address: address.ip().to_string(),
                port: address.port(),
                auxiliary: true,
                browser: Some(settings),
            });
        }
    }
    for (i, claim) in claims.iter().enumerate() {
        for other in &claims[..i] {
            if (claim.auxiliary || other.auxiliary)
                && claim.port == other.port
                && listen_hosts_overlap(&claim.address, &other.address)
            {
                if claim.browser.is_some()
                    && claim.browser == other.browser
                    && claim.address.eq_ignore_ascii_case(&other.address)
                {
                    continue;
                }
                return Err(ConfigError::DuplicateInboundListen {
                    address: claim.address.clone(),
                    port: claim.port,
                });
            }
        }
    }
    Ok(())
}
/// The same canonical/wildcard policy applies to primary and auxiliary listeners.
pub(super) fn validate_inbound_listen(
    seen: &mut HashSet<(String, u16)>,
    address: &str,
    port: u16,
) -> Result<(), ConfigError> {
    if seen
        .iter()
        .any(|(other, other_port)| port == *other_port && listen_hosts_overlap(address, other))
    {
        return Err(ConfigError::DuplicateInboundListen {
            address: address.to_owned(),
            port,
        });
    }
    seen.insert((address.to_owned(), port));
    Ok(())
}
