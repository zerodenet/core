use super::*;
use crate::contract::CancellationToken;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{AddressFamily, DialPolicy};

fn context() -> CarrierContext {
    CarrierContext {
        session: None,
        deadline: Some(tokio::time::Instant::now() + Duration::from_secs(2)),
        cancel: CancellationToken::new(),
    }
}

#[tokio::test]
async fn root_service_uses_real_socket_and_preserves_source_binding() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let egress = EgressInterfaceControl::default();
    let policy = DialPolicy {
        source_ip: Some(std::net::Ipv4Addr::LOCALHOST.into()),
        ..Default::default()
    };
    let request = TcpNetworkRequest {
        peer: listener.local_addr().unwrap(),
        dial: policy,
        generation: NetworkService::generation(&egress),
    };
    let mut opened = egress.open_tcp(request.clone(), context()).await.unwrap();
    let (mut peer, observed) = listener.accept().await.unwrap();
    assert_eq!(observed, opened.local);
    assert_eq!(opened.remote, request.peer);
    assert_eq!(opened.generation, NetworkService::generation(&egress));
    opened.io.write_all(b"root network").await.unwrap();
    let mut bytes = [0; 12];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"root network");
}

#[tokio::test]
async fn root_service_rejects_hard_family_and_cancelled_context_without_connecting() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let egress = EgressInterfaceControl::default();
    let mut request = TcpNetworkRequest {
        peer: listener.local_addr().unwrap(),
        dial: DialPolicy::default(),
        generation: NetworkService::generation(&egress),
    };
    request.dial.address_family = AddressFamily::OnlyIpv6;
    assert!(egress.open_tcp(request.clone(), context()).await.is_err());
    request.dial = DialPolicy::default();
    let context = context();
    context.cancel.cancel();
    let failure = egress.open_tcp(request, context).await.err().unwrap();
    assert_eq!(failure.reason, Reason::Cancelled);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn root_service_keeps_native_connect_error_in_standard_source() {
    let egress = EgressInterfaceControl::default();
    // TCP cannot open a broadcast destination. This produces a real kernel
    // route/connect error without releasing a reserved listener port.
    let request = TcpNetworkRequest {
        peer: "255.255.255.255:9".parse().unwrap(),
        dial: DialPolicy::default(),
        generation: NetworkService::generation(&egress),
    };
    let failure = egress.open_tcp(request, context()).await.err().unwrap();
    let code = failure
        .source
        .raw_os_error()
        .expect("native route/connect failure");
    let error = failure.into_channel_error();
    let source = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(source.raw_os_error(), Some(code));
}

#[tokio::test]
async fn real_network_invalidation_rejects_a_previously_captured_binding() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let egress = EgressInterfaceControl::default();
    let request = TcpNetworkRequest {
        peer: listener.local_addr().unwrap(),
        dial: DialPolicy::default(),
        generation: NetworkService::generation(&egress),
    };
    egress.invalidate_network();
    assert_ne!(request.generation, NetworkService::generation(&egress));
    let failure = egress.open_tcp(request, context()).await.err().unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
}
