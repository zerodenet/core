use super::*;
#[tokio::test]
async fn controlled_vmess_grpc_keeps_the_host_minted_business_identity_and_service_only_provider_refuses_before_io(
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let runtime = vmess::transport::VmessTransportRuntime::default();
    let mut fixture = Fixture::cold(peer, &runtime, None, &profile(false), None, (0, 1), None);
    fixture.start().await;
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let identity = proxy.engine().mint_session_identity().unwrap();
    let mut business = zero_core::Session::new(
        identity.id().0,
        destination().address,
        destination().port,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("actual-business"),
    );
    proxy
        .engine()
        .prepare_session_with_identity(&identity, &mut business, "incoming")
        .unwrap();
    let invocation = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&business)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    let mut context = fixture.context();
    context.identity = invocation.identity;
    let failure = fixture
        .owner
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            context,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(failure.session, Some(identity.id()));
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(fixture.observations.outer.load(Ordering::Relaxed), 0);
    assert!(fixture.observations.facts.lock().unwrap().is_empty());
    assert!(timeout(Duration::from_millis(20), listener.accept())
        .await
        .is_err());
    let (report, tasks) = fixture.close().await;
    assert!(report.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}
#[tokio::test]
async fn complete_vmess_grpc_recipe_conflicts_and_legacy_real_tcp_udp_relay_entries_cannot_be_adopted(
) {
    use zero_traits::ProtocolUdpFlowLeaf;
    for kind in ["tcp", "udp", "relay"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let runtime = vmess::transport::VmessTransportRuntime::default();
        let grpc = profile(false);
        let mut fixture = Fixture::cold(peer, &runtime, None, &grpc, None, (0, 1), None);
        assert!(vmess::component::grpc::Options::from_options_refs_at(
            input(peer, None, &profile(true)),
            Default::default(),
            None,
            None,
            NonZeroUsize::new(1).unwrap(),
            &runtime
        )
        .is_err());
        let leaf = vmess::transport::VmessOutboundLeaf::from_options_refs(
            None,
            input(peer, None, &grpc),
            &runtime,
        )
        .unwrap();
        let target = zero_core::Session::new(
            901,
            destination().address,
            destination().port,
            if kind == "udp" {
                zero_core::Network::Udp
            } else {
                zero_core::Network::Tcp
            },
            zero_core::ProtocolType::new("client-wire"),
        );
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            drop(io);
        });
        if kind == "relay" {
            let socket = tokio::net::TcpStream::connect(peer).await.unwrap();
            let original = leaf
                .open_tcp_relay_hop(zero_platform_tokio::TcpRelayStream::new(socket), &target)
                .await;
            // gRPC may return its deferred stream before the peer close is
            // observed. Either outcome retains the same legacy acquisition.
            drop(original);
        } else if kind == "tcp" {
            let original = leaf
                .open_tcp_stream(&target, move |_, _| async move {
                    tokio::net::TcpStream::connect(peer)
                        .await
                        .map(zero_platform_tokio::TokioSocket::new)
                        .map_err(zero_transport::RuntimeError::from)
                })
                .await;
            // gRPC may return its deferred stream before the peer close is
            // observed. Either outcome retains the same legacy acquisition.
            drop(original);
        } else {
            let resume = ProtocolUdpFlowLeaf::direct_udp_resume(&leaf);
            let original = resume
                .open_direct_connection(&target, move |_, _| async move {
                    tokio::net::TcpStream::connect(peer)
                        .await
                        .map(zero_platform_tokio::TokioSocket::new)
                        .map_err(zero_transport::RuntimeError::from)
                })
                .await;
            // gRPC may return its deferred stream before the peer close is
            // observed. Either outcome retains the same legacy acquisition.
            drop(original);
        }
        timeout(Duration::from_secs(3), server)
            .await
            .unwrap_or_else(|_| {
                panic!("{kind}: actual legacy carrier must reach its retained listener")
            })
            .unwrap();
        // Even failure and actual app/leaf Drop provide no old native-control
        // or original driver join receipt, so the negative fence stays sticky.
        drop(leaf);
        let access = fixture.grants.activate(deadline()).unwrap();
        let failure = fixture.owner.lifecycle.start(deadline()).await.unwrap_err();
        drop(access);
        assert_eq!(failure.reason, Reason::StalePlan, "{kind}");
        let (_, tasks) = fixture.close().await;
        assert!(tasks.exited_tasks.is_empty());
    }
}
#[derive(Debug)]
struct PanicAfterSocket;
impl zero_traits::IoObserver for PanicAfterSocket {
    fn stream_boundary(&self) {
        panic!("real native VMess gRPC socket outcome was not observed")
    }
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn controlled_vmess_grpc_unknown_actual_socket_acquisition_keeps_finite_slot_and_original_source(
) {
    use futures_util::FutureExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let runtime = vmess::transport::VmessTransportRuntime::default();
    let mut fixture = Fixture::cold(
        peer,
        &runtime,
        None,
        &profile(false),
        None,
        (0, 1),
        Some(Arc::new(PanicAfterSocket)),
    );
    fixture.start().await;
    let attempted =
        std::panic::AssertUnwindSafe(fixture.owner.roles.stream.as_ref().unwrap().connect(
            ConnectRequest {
                destination: destination(),
            },
            fixture.context(),
        ))
        .catch_unwind()
        .await;
    assert!(attempted.is_err());
    let first = fixture
        .owner
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            fixture.context(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(first.delivery, DeliveryState::Unknown);
    let original = std::error::Error::source(&first)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap()
        .clone();
    let second = fixture
        .owner
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            fixture.context(),
        )
        .await
        .err()
        .unwrap();
    assert!(Arc::ptr_eq(
        &original,
        std::error::Error::source(&second)
            .unwrap()
            .downcast_ref::<Arc<ChannelError>>()
            .unwrap()
    ));
    let (owner, tasks) = tokio::join!(
        fixture.owner.lifecycle.shutdown(deadline()),
        fixture.scope.shutdown(deadline())
    );
    assert_eq!(owner.err().unwrap().delivery, DeliveryState::Unknown);
    assert!(tasks.exited_tasks.is_empty());
    assert!(tasks.is_complete());
}
