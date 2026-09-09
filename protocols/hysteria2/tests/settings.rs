use hysteria2::{
    handshake::ReceiveBandwidth,
    settings::{decode_site_path, parse_bandwidth, Settings},
};

#[test]
fn bandwidth_units_and_negotiation_follow_hysteria() {
    assert_eq!(parse_bandwidth(Some("100 Mbps")), Ok(12_500_000));
    assert_eq!(parse_bandwidth(Some("1 Gbps")), Ok(125_000_000));
    assert!(parse_bandwidth(Some("18446744073709551615 Mbps")).is_err());
    assert!(parse_bandwidth(Some("100 megabytes")).is_err());
    let settings = Settings {
        upload: 5_000_000,
        download: 8_000_000,
        ..Settings::default()
    };
    assert_eq!(settings.client_send_rate(ReceiveBandwidth::Auto), 0);
    assert_eq!(
        settings.client_send_rate(ReceiveBandwidth::Limit(0)),
        5_000_000
    );
    assert_eq!(
        settings.client_send_rate(ReceiveBandwidth::Limit(3_000_000)),
        3_000_000
    );
    assert_eq!(settings.server_send_rate(0), 0);
    assert_eq!(settings.server_send_rate(9_000_000), 5_000_000);
    assert_eq!(
        Settings {
            ignore_client_bandwidth: true,
            ..settings
        }
        .server_send_rate(9_000_000),
        0
    );
    assert_eq!(
        Settings::default().client_send_rate(ReceiveBandwidth::Limit(3_000_000)),
        3_000_000
    );
}

#[test]
fn receive_window_ceiling_changes_connection_cache_identity() {
    let make =
        || hysteria2::udp::Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None);
    let mut settings = Settings::default();
    let fixed = make().with_settings(settings).cache_key();
    settings.quic.max_stream_receive_window = Some(16_777_216);
    assert_ne!(fixed, make().with_settings(settings).cache_key());
}
#[test]
fn settings_enforce_window_and_liveness_bounds() {
    let mut settings = Settings::default();
    assert!(settings.validate().is_ok());
    settings.quic.keep_alive_interval_secs = 30;
    assert!(settings.validate().is_err());
    settings.quic.keep_alive_interval_secs = 0;
    assert!(settings.validate().is_ok());
    settings.quic.stream_receive_window = 16_383;
    assert!(settings.validate().is_err());
    settings.quic.stream_receive_window = 16_384;
    settings.upload = 1;
    assert!(settings.validate().is_ok());
    settings.upload = u64::MAX;
    assert!(settings.validate().is_err());
}
#[test]
fn changed_transport_policy_invalidates_udp_identity() {
    let make =
        || hysteria2::udp::Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None);
    let old = make().cache_key();
    let mut settings = Settings::default();
    settings.quic.disable_path_mtu_discovery = true;
    assert_ne!(old, make().with_settings(settings).cache_key());
    assert_ne!(
        make().flow_resume().flow_cache_key("localhost", 443),
        make()
            .with_settings(settings)
            .flow_resume()
            .flow_cache_key("localhost", 443)
    );
}
#[test]
fn website_paths_cannot_escape_the_root() {
    assert_eq!(
        decode_site_path("/docs/a%20b.html").unwrap(),
        "docs/a b.html"
    );
    for path in [
        "/../secret",
        "/%2e%2e/secret",
        "/a/%2f../secret",
        "/a%5c../secret",
        "/C:/secret",
        "/nul%00",
        "/bad%",
        "/bad%gg",
    ] {
        assert!(decode_site_path(path).is_err(), "{path}");
    }
}
#[cfg(feature = "validation")]
#[test]
fn reverse_proxy_only_accepts_fixed_http_origins() {
    for url in [
        "file:///etc/passwd",
        "http://user:pass@host/",
        "https://host/#secret",
        "/relative",
    ] {
        assert!(hysteria2::settings::validate_proxy_url(url).is_err());
    }
    assert!(hysteria2::settings::validate_proxy_url("https://example.com/base").is_ok());
}

#[test]
fn bbr_profile_is_part_of_connection_cache_identity() {
    let make =
        || hysteria2::udp::Hysteria2UdpFlowConfig::new("hy", "localhost", 443, "password", None);
    let mut settings = Settings::default();
    let standard = make().with_settings(settings).cache_key();
    settings.bbr_profile = hysteria2::settings::BbrProfile::Conservative;
    assert_ne!(standard, make().with_settings(settings).cache_key());
    settings.bbr_profile = hysteria2::settings::BbrProfile::Aggressive;
    assert_ne!(standard, make().with_settings(settings).cache_key());
}
