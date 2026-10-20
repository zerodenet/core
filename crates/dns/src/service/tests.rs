use super::*;
use std::time::Duration;
#[cfg(feature = "udp")]
use std::{collections::BTreeMap, sync::Arc};
#[cfg(feature = "udp")]
use zero_config::{DnsAnswerConfig, DnsCacheConfig, DnsConfig, DnsPolicyConfig, DnsServerConfig};
use zero_platform_tokio::contract::dns::DnsQueryRole;
#[cfg(feature = "udp")]
use zero_platform_tokio::contract::CancellationToken;
use zero_traits::AddressFamily;
#[cfg(feature = "udp")]
use zero_traits::IpAddress;

#[cfg(feature = "udp")]
fn config(port: u16) -> DnsConfig {
    DnsConfig {
        servers: BTreeMap::from([(
            "local".into(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".into(),
                port,
                bootstrap: vec![],
                detour: None,
            },
        )]),
        default_server: "local".into(),
        dispatch: vec![],
        cache: Some(DnsCacheConfig {
            max_entries: 16,
            max_ttl_seconds: None,
        }),
        reverse_mapping: None,
        answer: DnsAnswerConfig::Real,
        policy: DnsPolicyConfig {
            node_server: Some("local".into()),
            ..Default::default()
        },
    }
}
fn request(name: &str, role: DnsQueryRole) -> DnsRequest {
    DnsRequest {
        name: name.into(),
        role,
        family: AddressFamily::OnlyIpv4,
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[cfg(feature = "udp")]
#[tokio::test]
async fn public_dns_service_uses_real_role_isolated_queries_and_reports_actual_generations() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dns = DnsSystem::build(Some(&config(socket.local_addr().unwrap().port()))).unwrap();
    let generation = dns.generation();
    let server = tokio::spawn(async move {
        let mut buffer = [0u8; 4096];
        for _ in 0..2 {
            let (len, peer) = socket.recv_from(&mut buffer).await.unwrap();
            assert_eq!(
                message::parse_question(&buffer[..len]).unwrap().query_type,
                message::TYPE_A
            );
            let response =
                crate::udp::build_dns_response(&buffer[..len], &[IpAddress::V4([192, 0, 2, 11])]);
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    for role in [DnsQueryRole::Node, DnsQueryRole::Bootstrap] {
        let answer = DnsService::resolve(
            &dns,
            request("service.test", role),
            DnsQueryContext {
                deadline: Some(deadline()),
                expected_generation: Some(generation),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(answer.addresses, vec![IpAddress::V4([192, 0, 2, 11])]);
        assert_eq!(answer.generation, generation);
    }
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    dns.shutdown_queries(deadline()).await.unwrap();
    assert_eq!(
        DnsService::resolve(
            &dns,
            request("service.test", DnsQueryRole::Node),
            DnsQueryContext::default()
        )
        .await
        .unwrap_err()
        .reason,
        Reason::Revoked
    );
}

#[cfg(feature = "udp")]
#[tokio::test]
async fn cancelled_real_dns_query_cannot_commit_a_late_success_to_cache() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dns =
        Arc::new(DnsSystem::build(Some(&config(socket.local_addr().unwrap().port()))).unwrap());
    let (entered, received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut buffer = [0u8; 4096];
        let (len, peer) = socket.recv_from(&mut buffer).await.unwrap();
        entered.send(()).unwrap();
        released.await.unwrap();
        socket
            .send_to(
                &crate::udp::build_dns_response(&buffer[..len], &[IpAddress::V4([192, 0, 2, 12])]),
                peer,
            )
            .await
            .unwrap();
    });
    let cancel = CancellationToken::new();
    let pending = {
        let dns = dns.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            DnsService::resolve(
                dns.as_ref(),
                request("cancel.test", DnsQueryRole::Default),
                DnsQueryContext {
                    cancel,
                    deadline: Some(deadline()),
                    ..Default::default()
                },
            )
            .await
        })
    };
    received.await.unwrap();
    assert!(
        !pending.is_finished(),
        "cancellation must interrupt actual pending DNS I/O"
    );
    cancel.cancel();
    assert_eq!(
        pending.await.unwrap().unwrap_err().reason,
        Reason::Cancelled
    );
    release.send(()).unwrap();
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    dns.shutdown_queries(deadline()).await.unwrap();
    assert!(dns.inspect_cache("cancel.test").await.is_none());
}

#[tokio::test]
async fn public_dns_generation_and_expired_context_fail_before_any_query() {
    let dns = DnsSystem::build(None).unwrap();
    let old = dns.generation();
    dns.reload(None).unwrap();
    assert_ne!(old, dns.generation());
    let error = DnsService::resolve(
        &dns,
        request("unused.invalid", DnsQueryRole::Default),
        DnsQueryContext {
            expected_generation: Some(old),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    let error = DnsService::resolve(
        &dns,
        request("unused.invalid", DnsQueryRole::Default),
        DnsQueryContext {
            deadline: Some(Instant::now()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, Reason::Timeout);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    dns.shutdown_queries(deadline()).await.unwrap();
}
