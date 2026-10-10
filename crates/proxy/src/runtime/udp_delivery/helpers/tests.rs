use super::accounting::UdpInboundResponseAccounting;
use super::parts::UdpDirectResponseParts;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;
use crate::runtime::udp_socket::{DirectUdpPolicy, DirectUdpResponseGuard, DirectUdpSockets};

async fn read_reply() -> (
    crate::runtime::Proxy,
    DirectUdpSockets,
    DirectUdpResponseGuard,
) {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],
        "route":{"final":{"type":"direct"}}
    }"#,
    )
    .unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let services = UdpRuntimeServices::new(proxy.tcp_runtime_services());
    let mut sockets = DirectUdpSockets::new(services.network(), None);
    let (dial_policy, generation) = proxy.engine().direct_dial_policy(Some("direct")).unwrap();
    let policy = DirectUdpPolicy {
        tag: Some("direct".to_owned()),
        dial_policy,
        generation,
    };
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sockets
        .send_to_addr(b"request", peer.local_addr().unwrap(), 1, &policy)
        .await
        .unwrap();
    let mut buffer = [0; 32];
    let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
    peer.send_to(b"response", local).await.unwrap();
    let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
    (proxy, sockets, response.guard.unwrap())
}

async fn response<'a>(
    proxy: &crate::runtime::Proxy,
    guard: DirectUdpResponseGuard,
    payload: &'a [u8],
) -> UdpDirectResponseParts<'a> {
    let mut session = zero_core::Session::new(
        1,
        zero_core::Address::Ipv4([127, 0, 0, 1]),
        53,
        zero_core::Network::Udp,
        zero_core::ProtocolType::UNKNOWN,
    );
    session.down_bps = Some(65_536);
    let limiters =
        UdpFlowRateLimiters::new(PrincipalRateLimitRegistry::default().acquire(&session));
    assert!(limiters.throttle_download(16 * 1024).await);
    let services = UdpRuntimeServices::new(proxy.tcp_runtime_services());
    UdpDirectResponseParts {
        target: session.target,
        port: session.port,
        payload,
        accounting: UdpInboundResponseAccounting::record_received(
            &services,
            Some(1),
            payload.len(),
            limiters,
        ),
        guard: Some(guard),
    }
}

#[tokio::test]
async fn optional_direct_writer_rechecks_retirement_after_download_wait() {
    let (proxy, mut sockets, guard) = read_reply().await;
    let payload = [0; 16 * 1024];
    let response = response(&proxy, guard, &payload).await;
    let called = std::sync::atomic::AtomicBool::new(false);
    let write = crate::runtime::udp_delivery::write_optional_direct_response(&response, || async {
        called.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, std::io::Error>(Some(payload.len()))
    });
    tokio::pin!(write);
    assert!(matches!(
        futures_util::poll!(&mut write),
        std::task::Poll::Pending
    ));
    sockets.retire_session(1);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), write)
            .await
            .unwrap()
            .unwrap(),
        None
    );
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
}

#[cfg(feature = "udp-response-runtime")]
#[tokio::test]
async fn direct_writer_rechecks_policy_generation_after_download_wait() {
    let (proxy, _sockets, guard) = read_reply().await;
    let payload = [0; 16 * 1024];
    let response = response(&proxy, guard, &payload).await;
    let called = std::sync::atomic::AtomicBool::new(false);
    let write = crate::runtime::udp_delivery::write_direct_response(&response, || async {
        called.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, std::io::Error>(payload.len())
    });
    tokio::pin!(write);
    assert!(matches!(
        futures_util::poll!(&mut write),
        std::task::Poll::Pending
    ));
    let mut config = proxy.engine().config().as_ref().clone();
    config.outbounds[0].dial.address_family = zero_config::OutboundAddressFamily::OnlyIpv4;
    proxy.engine().reload_runtime_config(config).unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), write)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
}
