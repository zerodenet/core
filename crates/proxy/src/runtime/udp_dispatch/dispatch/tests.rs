use std::time::Duration;

use zero_config::RuntimeConfig;
use zero_core::{Address, ProtocolType, SessionAuth};

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
