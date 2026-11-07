//! Fixed-target borrowing retains the existing protocol packet control and pool.
use crate::{runtime::MuxPrefix, Proxy};
use std::{sync::Arc, time::Duration};
use tokio::{net::UdpSocket, time::Instant};
use zero_core::{
    component::AssociationId,
    request::{DatagramTargets, Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        datagram::DatagramRequest,
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError,
    },
    controlled_carrier::CarrierOrigin,
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
struct OriginalOnly;
impl UpstreamOpener for OriginalOnly {
    fn observe_carrier_origin(&self, origin: &CarrierOrigin) -> bool {
        origin.driver().is_some() && !origin.is_retired()
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("the actual MUX pool must use its owning background provider") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("borrowing cannot create another datagram pool") })
    }
}

#[tokio::test]
async fn actual_mux_datagram_carrier_loans_retain_original_control_and_share_the_physical_driver() {
    tokio::time::timeout(Duration::from_secs(35), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let mut server = MuxPrefix::start(protocol, "pool").await;
            if let zero_config::OutboundProtocolConfig::Vless {
                xudp_concurrency, ..
            } = &mut server.node.protocol
            {
                *xudp_concurrency = Some(8);
            }
            let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let endpoint = Destination::new(
                Address::Ipv4([127, 0, 0, 1]),
                echo.local_addr().unwrap().port(),
            );
            let mut config =
                zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#)
                    .unwrap();
            config.outbounds = vec![server.node.clone()];
            // This is the existing controlled assembly used to qualify the
            // original provider. Ordinary registration is checked separately.
            let proxy =
                Proxy::from_engine_with_background(zero_engine::Engine::new(config).unwrap(), true)
                    .unwrap();
            let config = proxy.engine().runtime_snapshot().config().clone();
            proxy
                .protocols
                .start_components(config.clone(), end())
                .await
                .unwrap();
            proxy
                .protocols
                .commit_components(&config, || Ok(()))
                .unwrap();
            let plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let slot = plan.configured("pool").unwrap();
            let role = slot.datagram(end()).await.unwrap();
            proxy
                .protocols
                .drive_component_supervision(async {
                    let context = || OpenContext {
                        identity: RequestIdentity::Service(
                            Arc::new(ServiceRequestFacts {
                                origin: ServiceOrigin::Runtime,
                                purpose: ServicePurpose::BackgroundControl,
                            })
                            .into(),
                        ),
                        deadline: Some(end()),
                        cancel: CancellationToken::new(),
                        observer: None,
                        upstream: UpstreamTransport::new(
                            UpstreamBinding {
                                prefix: vec![],
                                dial: slot.dial.clone(),
                                network_generation: zero_core::component::NetworkGeneration(
                                    proxy.egress_interface.generation(),
                                ),
                            },
                            Arc::new(OriginalOnly),
                        ),
                    };
                    let open = |id| {
                        role.open_carrier(
                            DatagramRequest {
                                association: AssociationId(id),
                                targets: DatagramTargets::Fixed(endpoint.clone()),
                            },
                            context(),
                        )
                    };
                    let mut first = open(901).await.unwrap();
                    let root = first.owner.original().clone();
                    let driver = root.driver().unwrap().clone();
                    first.io.send(b"loan").await.unwrap();
                    let mut bytes = [0; 32];
                    let (count, peer) = echo.recv_from(&mut bytes).await.unwrap();
                    assert_eq!(&bytes[..count], b"loan");
                    echo.send_to(&bytes[..count], peer).await.unwrap();
                    assert_eq!(first.io.receive(&mut bytes).await.unwrap(), 4);
                    let mut receive = Box::pin(first.io.receive(&mut bytes));
                    std::future::poll_fn(|cx| {
                        use std::future::Future;
                        assert!(
                            receive.as_mut().poll(cx).is_pending(),
                            "actual packet receive pending"
                        );
                        std::task::Poll::Ready(())
                    })
                    .await;
                    drop(receive);
                    assert!(!first
                        .owner
                        .close(Instant::now() + Duration::from_millis(10))
                        .await
                        .unwrap()
                        .is_complete());
                    let mut second = open(902).await.unwrap();
                    assert!(root.same_original(second.owner.original()));
                    drop(first.io);
                    assert!(first.owner.close(end()).await.unwrap().is_complete());
                    assert!(first.owner.closed().unwrap().matches(&first.owner));
                    assert!(!driver.is_joined() && root.closed().is_none());
                    second.io.send(b"same").await.unwrap();
                    let (count, peer) = echo.recv_from(&mut bytes).await.unwrap();
                    assert_eq!(&bytes[..count], b"same");
                    echo.send_to(&bytes[..count], peer).await.unwrap();
                    assert_eq!(second.io.receive(&mut bytes).await.unwrap(), 4);
                    drop(second.io);
                    assert!(second.owner.close(end()).await.unwrap().is_complete());
                    proxy.protocols.shutdown_components(end()).await.unwrap();
                    assert!(driver.matches_joined(&driver.try_joined().unwrap()));
                    assert!(root.closed().unwrap().matches(&root));
                    assert!(root.owner().closed().unwrap().matches(root.owner()));
                })
                .await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}
