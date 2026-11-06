//! No-deadline Service loans retain their real one-slot taskless controller.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{UpstreamBinding, UpstreamTransport},
    stream::ConnectRequest,
    CancellationToken,
};
async fn open(
    proxy: &Proxy,
    target: std::net::SocketAddr,
    cancel: CancellationToken,
) -> Result<
    zero_platform_tokio::controlled_carrier::loan::StreamLoan,
    zero_platform_tokio::contract::ChannelError,
> {
    let services = proxy.tcp_runtime_services();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("a")
        .unwrap();
    let role = slot.stream(deadline()).await?;
    let endpoint=slot.recipe.spec().resources.iter().find_map(|resource|match resource {zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{endpoint,..}=>Some(endpoint.clone()),_=>None}).unwrap();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: zero_core::component::NetworkGeneration(
            services.upstream().network_generation(),
        ),
    };
    let peer = std::net::SocketAddr::new(
        match endpoint.address {
            zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(ip).into(),
            _ => panic!("fixture literal IPv4"),
        },
        endpoint.port,
    );
    let root = zero_platform_tokio::controlled_carrier::NativeCarrierOpener::new(
        slot.instance_id(),
        binding.clone(),
        peer,
        Arc::new(proxy.egress_interface_control()),
        None,
    )?;
    role.connect_carrier(
        ConnectRequest {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                target.port(),
            ),
        },
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {
                origin: zero_core::request::ServiceOrigin::Runtime,
                purpose: zero_core::request::ServicePurpose::BackgroundControl,
            })),
            deadline: None,
            cancel,
            observer: None,
            upstream: UpstreamTransport::new(binding, Arc::new(root)),
        },
    )
    .await
    .map(|connected| connected.io)
}
#[tokio::test]
async fn actual_taskless_none_deadline_keeps_n1_and_cancels_the_same_pending_view() {
    tokio::time::timeout(Duration::from_secs(35), async {
        for protocol in [
            "vless",
            "vmess",
            "trojan",
            #[cfg(feature = "shadowsocks")]
            "shadowsocks",
        ] {
            let mut prefix = if protocol == "shadowsocks" {
                crate::runtime::MuxPrefix::shadowsocks("aes-128-gcm", "a").await
            } else {
                crate::runtime::MuxPrefix::start(protocol, "a").await
            };
            match &mut prefix.node.protocol {
                zero_config::OutboundProtocolConfig::Vless {
                    mux_concurrency,
                    xudp_concurrency,
                    ..
                } => {
                    *mux_concurrency = None;
                    *xudp_concurrency = None;
                }
                zero_config::OutboundProtocolConfig::Vmess {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Trojan {
                    mux_concurrency, ..
                } => *mux_concurrency = None,
                _ => {}
            }
            let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let target = echo.local_addr().unwrap();
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let worker = tokio::spawn(async move {
                let (mut io, _) = echo.accept().await.unwrap();
                let mut bytes = [0; 3];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, *b"one");
                io.write_all(&bytes).await.unwrap();
                io.flush().await.unwrap();
                stopped.await.unwrap();
            });
            let mut settings = config([target; 3]);
            settings.outbounds = vec![prefix.node.clone()];
            settings.outbound_groups.clear();
            settings.runtime.native_stream_max_originals = Some(1);
            let proxy = Proxy::new(settings.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(settings, Duration::from_secs(5))
                .await
                .unwrap();
            let cancel = CancellationToken::new();
            let mut loan = open(&proxy, target, cancel.clone()).await.unwrap();
            let original = loan.owner.original().clone();
            assert!(original.driver().is_none());
            assert_eq!(original.owner().limit().get(), 1);
            loan.io.write_all(b"one").await.unwrap();
            loan.io.flush().await.unwrap();
            let mut bytes = [0; 3];
            loan.io.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, *b"one");
            assert_eq!(
                open(&proxy, target, CancellationToken::new())
                    .await
                    .err()
                    .unwrap()
                    .reason,
                zero_platform_tokio::contract::Reason::Capacity
            );
            {
                let mut byte = [0; 1];
                let pending = loan.io.read(&mut byte);
                tokio::pin!(pending);
                assert!(
                    futures_util::poll!(&mut pending).is_pending(),
                    "actual target is holding its next write"
                );
                cancel.cancel();
                assert!(pending.await.is_err());
            }
            assert!(
                !loan.owner.try_close().unwrap().is_complete(),
                "cancellation does not return an actual held view"
            );
            let zero_platform_tokio::controlled_carrier::loan::StreamLoan { io, mut owner } = loan;
            drop(io);
            tokio::time::timeout_at(deadline(), async {
                loop {
                    if owner.try_close().unwrap().is_complete() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(owner.closed().unwrap().matches(&owner));
            let closed = original.closed().unwrap();
            assert!(closed.matches(&original) && closed.had_native_acquisition());
            stop.send(()).unwrap();
            worker.await.unwrap();
            running.shutdown().await.unwrap();
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            assert!(original
                .owner()
                .users_closed()
                .unwrap()
                .matches(original.owner()));
            prefix.close().await;
        }
    })
    .await
    .unwrap();
}
