use super::*;
use zero_core::Address;
use zero_engine::RouteDecision;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

fn auto_services() -> TcpExecutionServices {
    let config = serde_json::from_value(serde_json::json!({
        "outbounds": [
            {"tag": HEALTH_TAG, "protocol": {
                "type": "shadowsocks", "server": "127.0.0.1", "port": 443,
                "password": "password", "cipher": "aes-256-gcm"
            }},
            {"tag": "alternate", "protocol": {
                "type": "shadowsocks", "server": "127.0.0.1", "port": 444,
                "password": "password", "cipher": "aes-256-gcm"
            }}
        ],
        "outbound_groups": [{
            "tag": "auto", "type": "url_test", "outbounds": [HEALTH_TAG, "alternate"],
            "url": "http://probe.example/", "interval_seconds": 300
        }],
        "route": {"final": {"type": "route", "outbound": "auto"}}
    }))
    .unwrap();
    crate::Proxy::new(config)
        .unwrap()
        .tcp_runtime_services()
        .execution()
}

fn selected_tag(services: &TcpExecutionServices) -> String {
    let (_, _, selections) = services
        .engine()
        .resolve_route_decision_for_flow(
            RouteDecision::Route("auto".into()),
            &Address::Domain("www.google.com".into()),
            443,
        )
        .unwrap();
    // Observe the actual per-flow policy decision without projecting a kernel
    // outbound leaf into runtime dispatch, including in this test fixture.
    assert_eq!(selections.len(), 1, "URLTest makes one member selection");
    let selection = &selections[0];
    assert_eq!(selection.policy_tag, "auto");
    selection.member_tag.clone()
}

#[tokio::test]
async fn thirty_local_capacity_rejections_preserve_selection_and_immediate_recovery() {
    for message in [
        "actual taskless original or caller ACK still occupies capacity",
        "previous actual taskless carrier cleanup is unfinished",
    ] {
        let services = auto_services();
        assert_eq!(selected_tag(&services), HEALTH_TAG);
        for _ in 0..30 {
            let error = ChannelError::new(Stage::Dial, Reason::Capacity, Impact::Session)
                .with_source(std::io::Error::other(message));
            let failure = complete_error(
                &services,
                TcpDispatchIntent::Traffic,
                std::io::Error::other(error).into(),
            );
            assert_eq!(
                crate::transport::channel_failure(&failure.error)
                    .unwrap()
                    .reason,
                Reason::Capacity
            );
            services
                .engine()
                .check_outbound_health(HEALTH_TAG)
                .expect("local admission exhaustion is not evidence against the selected node");
            assert_eq!(selected_tag(&services), HEALTH_TAG);
        }
        // Exercise the production completion hook with a real loopback TCP
        // stream after admission recovers, not a fabricated successful probe.
        assert!(complete_wire(&services, TcpDispatchIntent::Traffic)
            .await
            .is_ok());
        assert_eq!(selected_tag(&services), HEALTH_TAG);
    }
}

#[tokio::test]
async fn actual_remote_failures_still_reselect_before_the_next_urltest_probe() {
    let services = auto_services();
    assert_eq!(selected_tag(&services), HEALTH_TAG);
    for _ in 0..5 {
        let error = ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
            .with_source(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        complete_error(
            &services,
            TcpDispatchIntent::Traffic,
            std::io::Error::other(error).into(),
        );
    }
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect_err("actual remote failures retain their circuit-breaker semantics");
    assert_eq!(selected_tag(&services), "alternate");
}
