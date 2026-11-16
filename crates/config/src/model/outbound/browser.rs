//! Structural Browser listener projection; no runtime/resource authority.
use super::*;
impl OutboundProtocolConfig {
    /// Actual ordinary VMess/Trojan Browser WS listener, when the configured
    /// native security/header facts are expressible by the browser channel.
    /// Other combinations retain their existing compatibility execution.
    pub fn ordinary_browser_ws_listener(&self) -> Option<&WebSocketConfig> {
        let ws = match self {
            Self::Trojan {
                server,
                ws,
                mux_concurrency: None,
                grpc: None,
                insecure: false,
                sni,
                client_fingerprint: None,
                ..
            } if sni
                .as_deref()
                .is_none_or(|name| name.is_empty() || name.eq_ignore_ascii_case(server)) =>
            {
                ws
            }
            Self::Vmess {
                server,
                ws,
                mux_concurrency: None,
                grpc: None,
                tls,
                ..
            } => {
                if let Some(tls) = tls {
                    let options = zero_traits::ClientTlsProfile::tls_options(tls.as_ref());
                    if options != Default::default()
                        || tls.disable_sni
                        || tls.ca_cert_path.is_some()
                        || tls.insecure
                        || !tls.alpn.is_empty()
                        || tls.client_fingerprint.is_some()
                        || tls.server_name.as_deref().is_some_and(|name| {
                            !name.is_empty() && !name.eq_ignore_ascii_case(server)
                        })
                    {
                        return None;
                    }
                }
                ws
            }
            _ => return None,
        };
        ws.as_deref().filter(|ws| {
            ws.browser_dialer.is_some()
                && ws.host.as_deref().is_none_or(str::is_empty)
                && ws.headers.is_empty()
        })
    }
}
