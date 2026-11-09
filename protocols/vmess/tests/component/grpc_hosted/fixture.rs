use super::*;
pub(super) async fn actual_failure(user_receives: bool) {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let grpc = OwnedGrpcProfile {
            service_names: vec!["actual-vmess".into()],
            multi_mode: true,
            ..Default::default()
        };
        let server_profile = grpc.clone();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming =
                zero_transport::grpc::accept_grpc_connection(io, &server_profile).unwrap();
            let carrier = incoming.accept().await.unwrap();
            let profile = crate::inbound::VmessInboundProfile::from_users(vec![
                crate::inbound::VmessUser::from_config(
                    ID,
                    "aes-128-gcm",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ]);
            let (facts, mut io) = profile
                .accept_request_stream(
                    crate::inbound::VmessInbound,
                    zero_platform_tokio::TcpRelayStream::new(carrier),
                )
                .await
                .unwrap();
            assert_eq!(facts.destination, destination());
            for expected in [b"live", b"fail"] {
                let mut bytes = [0; 4];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, expected);
                io.write_all(&bytes).await.unwrap();
                io.flush().await.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let runtime = crate::transport::VmessTransportRuntime::default();
        let cap = NonZeroUsize::new(1).unwrap();
        let options = Options::from_options_refs_at(
            crate::transport::VmessOutboundBuildOptionsRef::<
                OwnedClientTlsProfile,
                OwnedWebSocketProfile,
                OwnedGrpcProfile,
            > {
                tag: "actual",
                server: "127.0.0.1",
                port: peer.port(),
                protocol: crate::transport::VmessOutboundOptionsRef {
                    id: ID,
                    cipher: "aes-128-gcm",
                    mux_concurrency: None,
                    mux_idle_timeout_secs: None,
                    mux_response_backlog_frames: None,
                    mux_response_backlog_bytes: None,
                },
                tls: None,
                ws: None,
                grpc: Some(&grpc),
            },
            Default::default(),
            None,
            None,
            cap,
            &runtime,
        )
        .unwrap();
        let actual = ComponentInstanceId {
            component: ComponentId(912),
            generation: ResourceGeneration(4),
        };
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let observer = Arc::new(FailActualRead::default());
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(actual, binding, peer, egress, Some(observer.clone()))
                    .unwrap(),
            ),
        );
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(cap));
        scope.enable_registration().unwrap();
        let mut owner = Factory
            .build(
                Arc::new(options),
                BuildContext {
                    component: actual.component,
                    generation: actual.generation,
                    dependencies: Arc::from([]),
                    roles: ComponentRoleIds {
                        outbound: Some(OutboundId(913)),
                        ..Default::default()
                    },
                    resources: Arc::new(Denied),
                    tasks: scope.registrar(),
                    cancel: CancellationToken::new(),
                    background_carrier: Some(upstream.clone()),
                },
            )
            .unwrap();
        owner.lifecycle.start(end()).await.unwrap();
        let mut connected = owner
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                OpenContext {
                    identity: RequestIdentity::Service(
                        Arc::new(ServiceRequestFacts {
                            origin: ServiceOrigin::Runtime,
                            purpose: ServicePurpose::DiagnosticProbe,
                        })
                        .into(),
                    ),
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                    upstream,
                    observer: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
        connected.io.write_all(b"live").await.unwrap();
        connected.io.flush().await.unwrap();
        let mut bytes = [0; 4];
        connected.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"live");
        observer.0.store(true, Ordering::SeqCst);
        connected.io.write_all(b"fail").await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let original = receipt.result().unwrap_err();
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        assert!(!receipt.local_failure_accepted());
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        if user_receives {
            let failure = connected.io.read_exact(&mut bytes).await.unwrap_err();
            // VMess's native wrapper forwards the same actual aggregate Arc,
            // retaining the original panic payload/source and decision fields.
            fn original_scope<'a>(
                source: &'a (dyn std::error::Error + 'static),
            ) -> Option<&'a Arc<ChannelError>> {
                if let Some(failure) = source.downcast_ref::<super::wire::SharedFailure>() {
                    return Some(&failure.0);
                }
                if let Some(io) = source.downcast_ref::<std::io::Error>() {
                    if let Some(found) = io.get_ref().and_then(|source| original_scope(source)) {
                        return Some(found);
                    }
                }
                source.source().and_then(original_scope)
            }
            assert!(Arc::ptr_eq(&original, original_scope(&failure).unwrap()));
            assert!(receipt.local_failure_accepted());
        }
        drop(connected);
        let deadline = end();
        let shutdown = owner.lifecycle.shutdown(deadline);
        tokio::pin!(shutdown);
        std::future::poll_fn(|cx| match shutdown.as_mut().poll(cx) {
            std::task::Poll::Ready(result) => {
                panic!("physical cleanup finished before original Host ACK: {result:?}")
            }
            std::task::Poll::Pending => {
                if receipt.local_failure_accepted() {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }
        })
        .await;
        assert!(!receipt.registration_released());
        assert!(Arc::ptr_eq(&original, &receipt.result().unwrap_err()));
        assert!(
            receipt.local_failure_accepted(),
            "no-user cleanup persists the same original record before accepting"
        );
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        scope.acknowledge(&receipt).unwrap();
        let report = shutdown.await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 1);
        assert!(receipt.registration_released());
        let healthy = scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let success = scope.next_receipt().await.unwrap();
        assert_eq!(success.id(), healthy);
        assert!(success.result().is_ok());
        scope.acknowledge(&success).unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
