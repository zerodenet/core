use hysteria2::handshake::{AuthResponse, ReceiveBandwidth};
use hysteria2::udp::Hysteria2UdpFlowConfig;

#[test]
fn authentication_negotiates_udp_and_receive_bandwidth() {
    assert_eq!(
        AuthResponse::from_headers(Some("true"), Some("auto")),
        AuthResponse {
            udp_enabled: true,
            receive_bandwidth: ReceiveBandwidth::Auto,
        }
    );
    assert_eq!(
        AuthResponse::from_headers(Some("false"), Some("12500000")),
        AuthResponse {
            udp_enabled: false,
            receive_bandwidth: ReceiveBandwidth::Limit(12_500_000),
        }
    );
    for value in [
        None,
        Some(""),
        Some("invalid"),
        Some("-1"),
        Some("+1"),
        Some(" 1"),
    ] {
        assert_eq!(
            AuthResponse::from_headers(None, value),
            AuthResponse {
                udp_enabled: false,
                receive_bandwidth: ReceiveBandwidth::Limit(0),
            }
        );
    }
}

#[test]
fn udp_caches_separate_trust_policy_and_fingerprint() {
    let strict = Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None);
    let insecure =
        Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None).with_insecure(true);
    let fingerprint =
        Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", Some("chrome"));
    let sni = Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None)
        .with_server_name(Some("other.example"));
    assert_eq!(sni.connector_profile().server_name(), Some("other.example"));
    for other in [insecure, fingerprint, sni] {
        assert_ne!(strict.cache_key(), other.cache_key());
        assert_ne!(
            strict.flow_resume().flow_cache_key("localhost", 443),
            other.flow_resume().flow_cache_key("localhost", 443)
        );
        assert_ne!(strict.packet_path_spec(), other.packet_path_spec());
    }
    assert!(!strict.connector_profile().insecure());
}
