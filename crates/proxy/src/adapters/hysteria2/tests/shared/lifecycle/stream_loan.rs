//! A service loan holds one actual logical stream of the ordinary HY2 pool.
use super::*;
use tokio::io::AsyncRead;
use zero_core::request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::controlled_carrier::{loan::StreamLoan, CarrierOriginOwner};

async fn loan(proxy: &Proxy) -> StreamLoan {
    let services = proxy.tcp_runtime_services().execution();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("hy")
        .unwrap();
    let role = slot.stream(end()).await.unwrap();
    proxy
        .protocols
        .drive_component_supervision(role.connect_carrier(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
            },
            OpenContext {
                identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::DnsQuery,
                })),
                deadline: Some(end()),
                cancel: CancellationToken::new(),
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: slot.dial.clone(),
                        network_generation: zero_core::component::NetworkGeneration(
                            proxy.egress_interface.generation(),
                        ),
                    },
                    Arc::new(Denied),
                ),
                observer: None,
            },
        ))
        .await
        .unwrap()
        .io
}

#[tokio::test]
async fn hysteria2_actual_stream_loan_returns_logical_control_and_reuses_the_same_native_pool() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let proxy = server.proxy();
        start(&proxy).await;
        let mut first = loan(&proxy).await;
        let root = first.owner.original().clone();
        let original = match root.owner() {
            CarrierOriginOwner::Metadata(owner) => owner.current_original().unwrap(),
            _ => panic!("actual HY2 native Metadata owner"),
        };
        first.io.write_all(b"ping").await.unwrap();
        first.io.read_exact(&mut [0; 4]).await.unwrap();
        std::future::poll_fn(|cx| {
            let mut byte = [0; 1];
            let mut bytes = tokio::io::ReadBuf::new(&mut byte);
            assert!(
                std::pin::Pin::new(&mut first.io)
                    .poll_read(cx, &mut bytes)
                    .is_pending(),
                "the actual QUIC receive must be pending before cancellation"
            );
            std::task::Poll::Ready(())
        })
        .await;
        assert!(!first
            .owner
            .close(tokio::time::Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_complete());
        assert!(root.closed().is_none());
        assert!(original.closed().is_none());
        drop(first.io);
        assert!(first.owner.close(end()).await.unwrap().is_complete());
        assert!(first.owner.closed().unwrap().matches(&first.owner));
        assert!(
            original.closed().is_none(),
            "logical return never proves native close"
        );
        let mut next = loan(&proxy).await;
        assert!(next.owner.original().same_original(&root));
        next.io.write_all(b"ping").await.unwrap();
        let mut response = [0; 4];
        next.io.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"ping");
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        assert!(proxy.engine().active_sessions().is_empty());
        drop(next.io);
        assert!(next.owner.close(end()).await.unwrap().is_complete());
        proxy.protocols.shutdown_components(end()).await.unwrap();
        assert!(original.closed().unwrap().had_native_acquisition());
        assert!(root.owner().closed().unwrap().matches(root.owner()));
        server.close().await;
    })
    .await
    .unwrap();
}
