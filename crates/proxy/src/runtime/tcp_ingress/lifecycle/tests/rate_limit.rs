use super::apply_kernel_rate_limits_from_config;
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};

#[test]
fn hy2_transport_rates_do_not_become_tcp_or_udp_session_budgets() {
    let config = RuntimeConfig::parse(
        r#"{
        "inbounds":[{"tag":"hy", "listen":{"address":"127.0.0.1","port":443},
            "protocol":{"type":"hysteria2","password":"test","up_bps":1000000,"down_bps":2000000}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }"#,
    )
    .unwrap();
    for network in [Network::Tcp, Network::Udp] {
        for policy in [
            None,
            Some((Some(100_000), None)),
            Some((None, Some(200_000))),
        ] {
            let mut session = Session::new(
                1,
                Address::Domain("example.com".into()),
                443,
                network,
                ProtocolType::UNKNOWN,
            );
            let mut auth = SessionAuth::new("hysteria2");
            let (up, down) = policy.unwrap_or_default();
            auth.up_bps = up;
            auth.down_bps = down;
            session.apply_auth(auth);
            apply_kernel_rate_limits_from_config(&config, &mut session, "hy");
            assert_eq!((session.up_bps, session.down_bps), (up, down));
        }
    }
}

#[test]
fn other_protocol_session_defaults_and_explicit_policy_overrides_are_preserved() {
    for protocol in [
        serde_json::json!({"type":"trojan","password":"test"}),
        serde_json::json!({"type":"shadowsocks","password":"test","cipher":"aes-128-gcm"}),
    ] {
        let mut protocol = protocol;
        protocol["up_bps"] = serde_json::json!(100_000);
        protocol["down_bps"] = serde_json::json!(200_000);
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"in", "listen":{"address":"127.0.0.1","port":443}, "protocol":protocol}],
            "route":{"rules":[],"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let mut session = Session::new(
            1,
            Address::Domain("example.com".into()),
            443,
            Network::Tcp,
            ProtocolType::UNKNOWN,
        );
        session.up_bps = Some(50_000);
        apply_kernel_rate_limits_from_config(&config, &mut session, "in");
        assert_eq!(
            (session.up_bps, session.down_bps),
            (Some(50_000), Some(200_000))
        );
    }
}
