#![cfg(feature = "udp")]

use std::collections::BTreeMap;
use std::time::Duration;

use zero_config::{
    DnsAnswerConfig, DnsConfig, DnsDispatchRuleConfig, DnsServerConfig, RuleConditionConfig,
};
use zero_traits::IpAddress;

#[tokio::test]
async fn dispatch_queries_only_the_selected_backend() {
    let selected = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind selected DNS backend");
    let selected_port = selected.local_addr().unwrap().port();
    let selected_task = tokio::spawn(async move {
        for _ in 0..2 {
            let mut query = [0_u8; 512];
            let (size, peer) = selected.recv_from(&mut query).await.expect("receive query");
            let response = zero_dns::udp::build_dns_response(
                &query[..size],
                &[IpAddress::V4([203, 0, 113, 33])],
            );
            selected
                .send_to(&response, peer)
                .await
                .expect("send selected response");
        }
    });

    let unrelated = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind unrelated DNS backend");
    let unrelated_port = unrelated.local_addr().unwrap().port();
    let unrelated_task = tokio::spawn(async move {
        let mut query = [0_u8; 512];
        tokio::time::timeout(Duration::from_millis(500), unrelated.recv_from(&mut query))
            .await
            .is_ok()
    });

    let config = DnsConfig {
        servers: BTreeMap::from([
            (
                "private".to_owned(),
                DnsServerConfig::Udp {
                    host: "127.0.0.1".to_owned(),
                    port: selected_port,
                    bootstrap: Vec::new(),
                },
            ),
            (
                "public".to_owned(),
                DnsServerConfig::Udp {
                    host: "127.0.0.1".to_owned(),
                    port: unrelated_port,
                    bootstrap: Vec::new(),
                },
            ),
        ]),
        default_server: "public".to_owned(),
        dispatch: vec![DnsDispatchRuleConfig {
            condition: RuleConditionConfig::Domain {
                values: vec!["internal.example".to_owned()],
            },
            server: "private".to_owned(),
        }],
        cache: None,
        answer: DnsAnswerConfig::Real,
    };
    let dns = zero_dns::DnsSystem::build(Some(&config)).expect("build DNS system");

    let addresses = dns
        .resolve_real("api.internal.example")
        .await
        .expect("resolve through selected backend");
    assert_eq!(addresses, vec![IpAddress::V4([203, 0, 113, 33])]);
    selected_task.await.expect("selected backend task");
    assert!(
        !unrelated_task.await.expect("unrelated backend task"),
        "DNS query leaked to an unrelated backend"
    );
}
