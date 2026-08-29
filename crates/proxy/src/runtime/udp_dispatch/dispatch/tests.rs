use std::time::Duration;

use zero_config::RuntimeConfig;
use zero_core::{Address, ProtocolType, SessionAuth};
use zero_engine::EngineError;

use crate::runtime::pipe::UdpPipeInput;
use crate::runtime::udp_ingress::UdpIngressRuntime;

fn input<'a>(
    target: Address,
    port: u16,
    payload: &'a [u8],
    auth: &'a SessionAuth,
) -> UdpPipeInput<'a> {
    UdpPipeInput {
        target,
        port,
        payload,
        protocol: ProtocolType::new("test"),
        auth: Some(auth),
        source_addr: None,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
    }
}

#[tokio::test]
async fn authenticated_udp_flow_applies_bidirectional_session_rate_limits() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("managed-in")
        .await
        .expect("create UDP dispatch");

    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP sink");
    let target_addr = sink.local_addr().expect("UDP sink address");
    let target = match target_addr.ip() {
        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    };

    let mut auth = SessionAuth::new("test");
    auth.principal_key = Some("account:1".to_owned());
    auth.up_bps = Some(1);
    auth.down_bps = Some(1);
    let payload = vec![0_u8; 16 * 1024];

    let session_id = dispatch
        .dispatch(input(target.clone(), target_addr.port(), &payload, &auth))
        .await
        .expect("start rate-limited UDP flow");

    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            dispatch.dispatch(input(target, target_addr.port(), &payload, &auth,)),
        )
        .await
        .is_err(),
        "subsequent UDP upload bypassed the authenticated session limiter"
    );

    let download = dispatch.rate_limiters_by_session_id(Some(session_id));
    download.throttle_download(payload.len()).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            download.throttle_download(payload.len()),
        )
        .await
        .is_err(),
        "UDP response lookup did not preserve download limiter debt"
    );
}

#[tokio::test]
async fn principal_cancellation_interrupts_a_throttled_first_packet_and_closes_carrier() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("managed-in")
        .await
        .expect("create UDP dispatch");

    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP sink");
    let target_addr = sink.local_addr().expect("UDP sink address");
    let target = match target_addr.ip() {
        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    };
    let mut auth = SessionAuth::new("test");
    auth.principal_key = Some("account:1".to_owned());
    auth.up_bps = Some(1);
    let payload = vec![0_u8; 32 * 1024];

    let task = tokio::spawn(async move {
        let result = dispatch
            .dispatch(input(target, target_addr.port(), &payload, &auth))
            .await;
        (dispatch, result)
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while engine.active_sessions().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("throttled first packet never registered its session");
    assert_eq!(
        engine
            .close_principal_flows("account:1", "principal_disabled")
            .len(),
        1
    );

    let (mut dispatch, result) = tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .expect("principal cancellation did not wake first-packet shaping")
        .expect("first-packet task panicked");
    assert!(result.is_err());
    assert!(
        dispatch.finish_pending_cancellations(),
        "authenticated carrier was not marked for closure after pre-flow cancellation"
    );
}

#[tokio::test]
async fn failed_tuple_is_not_recreated_for_every_packet() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("tun-in")
        .await
        .expect("create UDP dispatch");
    let auth = SessionAuth::new("test");
    let target = Address::Domain("invalid\0domain".to_owned());

    dispatch
        .dispatch(input(target.clone(), 53, b"first", &auth))
        .await
        .expect_err("invalid destination must fail");
    assert_eq!(engine.completed_sessions().len(), 1);

    let second = dispatch
        .dispatch(input(target, 53, b"second", &auth))
        .await
        .expect_err("failed tuple must remain in backoff");
    assert!(matches!(second, EngineError::AdmissionDenied { .. }));
    assert_eq!(
        engine.completed_sessions().len(),
        1,
        "backoff attempt created another failed session"
    );
}

#[tokio::test]
async fn raw_and_quic_sniffed_fake_ip_reverse_misses_share_failure_contract() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "system": { "type": "system" } },
                    "default_server": "system",
                    "answer": {
                        "type": "fake_ip",
                        "cidr": "198.18.0.0/24",
                        "ipv6_cidr": "fd00::/120",
                        "ttl_seconds": 60,
                        "max_entries": 16
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse Fake-IP config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("tun-in")
        .await
        .expect("create UDP dispatch");
    let auth = SessionAuth::new("test");

    let mut raw = input(
        Address::Ipv6([0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9]),
        443,
        b"raw",
        &auth,
    );
    raw.transparent_target = true;
    let raw_error = dispatch
        .dispatch(raw)
        .await
        .expect_err("raw synthetic UDP target must fail");
    assert_eq!(raw_error.code(), "fake_ip_reverse_missing");

    let original = Address::Ipv4([198, 18, 0, 10]);
    let mut quic = input(
        Address::Domain("sniffed-quic.example".to_owned()),
        443,
        b"quic",
        &auth,
    );
    quic.transparent_target = true;
    quic.transparent_original_target = Some(original);
    quic.transparent_host_source = Some(zero_core::TargetHostSource::QuicSni);
    let quic_error = dispatch
        .dispatch(quic)
        .await
        .expect_err("QUIC SNI must not bypass missing Fake-IP ownership");
    assert_eq!(quic_error.code(), "fake_ip_reverse_missing");

    let completed = engine.completed_sessions();
    assert_eq!(completed.len(), 2);
    for record in completed {
        assert_eq!(record.close_reason.as_deref(), Some("target_error"));
        assert_eq!(
            record.fake_ip_reverse_status,
            Some(zero_core::FakeIpReverseStatus::Missing)
        );
        assert!(record.route.is_none());
        assert!(record.path.network.is_none());
        assert_eq!(record.outbound_tx_bytes, 0);
        assert_eq!(record.outbound_rx_bytes, 0);
        let failure = record.failure.expect("target failure observation");
        assert_eq!(failure.stage, "target_recovery");
        assert_eq!(failure.code.as_deref(), Some("fake_ip_reverse_missing"));
        assert!(failure.remote.is_none());
    }
}
