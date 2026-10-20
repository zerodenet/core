//! The actual registry owner, canonical cap and original handler Scope.
use super::*;
use zero_platform_tokio::contract::{component::AcceptedTcpLimits, lifecycle::TaskClass};

#[tokio::test]
async fn rejected_plain_client_retains_actual_source_without_stopping_same_owned_listener() {
    timeout(Duration::from_secs(5), async {
        let proxy = proxy();
        let config = proxy.engine().config();
        let operation = proxy
            .protocols
            .prepare_inbound_listener(config.inbounds[0].clone(), None)
            .unwrap();
        let prepared = PreparedInboundBind::new(
            operation,
            Arc::from([ResourceDeclaration::TcpListener {
                destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
                max_bind_attempts: NonZeroUsize::new(1).unwrap(),
                accepted: Some(AcceptedTcpLimits {
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                    max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
                }),
            }]),
        )
        .unwrap();
        let (shutdown, _) = watch::channel(false);
        let mut lease = proxy
            .inbound_owners
            .reserve_listener(
                "first",
                &config,
                &proxy.protocols,
                prepared,
                shutdown.clone(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        lease.bind_tcp().unwrap();
        let (operation, listener, installed) = lease.finish().unwrap().promote().unwrap();
        drop(operation); // The fixture below has an audited single handler body.
        let owner = installed.owner();
        let address = listener.local_addr().unwrap();
        let listener = zero_platform_tokio::TokioListener::from_controlled(listener).unwrap();
        let (source_send, source_receive) = tokio::sync::oneshot::channel();
        let source_send = Arc::new(std::sync::Mutex::new(Some(source_send)));
        let request = crate::runtime::listener_loop::TcpListenerLoopRequest {
            runtime_factory: crate::runtime::route_runtime::InboundRouteRuntimeFactory::new(
                crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                    proxy.tcp_runtime_services(),
                ),
                "first".into(),
            ),
            protocol_name: "audited-plain-test",
            listener,
            shutdown: shutdown.subscribe(),
            handler: move |runtime: crate::runtime::route_runtime::InboundRouteRuntime,
                           mut io: zero_platform_tokio::TokioSocket| {
                let source_send = source_send.clone();
                async move {
                    let caller = runtime.accepted_caller().unwrap();
                    let (_, actual_witness) =
                        runtime.accepted_tasks().unwrap().current_task().unwrap();
                    assert!(caller
                        .task_join_witness()
                        .unwrap()
                        .same_registration(&actual_witness));
                    caller.check_open().map_err(std::io::Error::other)?;
                    if let Some(sender) = source_send.lock().unwrap().take() {
                        sender.send(caller).ok().unwrap();
                    }
                    let mut request = [0];
                    io.read_exact(&mut request).await?;
                    if request == [b'!'] {
                        return Err(EngineError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "original rejected client",
                        )));
                    }
                    assert_eq!(request, [b'q']);
                    io.write_all(&request).await?;
                    Ok(())
                }
            },
        };
        owner
            .spawn_driver(crate::runtime::listener_loop::run_owned_tcp_listener_loop(
                crate::runtime::listener_loop::OwnedTcpLoop {
                    request,
                    users: crate::runtime::listener_loop::AcceptedUsersPlan::PrimaryOnly,
                },
            ))
            .unwrap();
        owner.seal_listener_installation();
        let exercise = async {
            let mut rejected = TcpStream::connect(address).await.unwrap();
            rejected.write_all(b"!").await.unwrap();
            assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
            let source = source_receive.await.unwrap();
            let original = source
                .task_join_witness()
                .unwrap()
                .try_joined()
                .unwrap()
                .result()
                .unwrap_err();
            assert!(
                owner.is_running(),
                "ordinary session rejection must not stop this listener"
            );
            let mut healthy = TcpStream::connect(address).await.unwrap();
            healthy.write_all(b"q").await.unwrap();
            let mut byte = [0];
            healthy.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [b'q']);
            assert_eq!(healthy.read(&mut byte).await.unwrap(), 0);
            shutdown.send_replace(true);
            original
        };
        let (observed, original) = tokio::join!(owner.observe_driver(), exercise);
        observed.unwrap();
        let receipt = proxy
            .inbound_owners
            .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        let terminal = receipt.terminal();
        assert!(terminal.failure().is_none());
        assert!(terminal.task_failures().is_empty());
        let saved = terminal.client_failure().unwrap();
        assert_eq!(saved.class(), TaskClass::Auxiliary);
        assert!(Arc::ptr_eq(&original, &saved.result().unwrap_err()));
        assert_eq!(terminal.joined_tasks(), 3); // Two actual handlers and driver.
        assert_eq!(terminal.closed_io_receipts(), 2); // Listener + child ledger.
        let _replacement = TcpListener::bind(address).await.unwrap();
    })
    .await
    .unwrap();
}
