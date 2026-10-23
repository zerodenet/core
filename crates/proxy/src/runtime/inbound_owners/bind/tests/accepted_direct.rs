//! Actual Direct operation: original accepted native/view/handler/host receipts.
use super::*;
use zero_platform_tokio::contract::{component::AcceptedTcpLimits, lifecycle::TaskClass};

#[tokio::test]
async fn actual_fixed_direct_handler_keeps_source_join_and_native_close_separate() {
    timeout(Duration::from_secs(8), async {
        // Keep the real endpoint reserved where a non-listener refuses connects.
        // Darwin instead drops SYNs to a bound non-listener, so release that
        // external fixture there to exercise an actual native refusal.
        let target = tokio::net::TcpSocket::new_v4().unwrap();
        target.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let target_address = target.local_addr().unwrap();
        #[cfg(target_os = "macos")]
        drop(target);
        let config = RuntimeConfig::parse(&format!(
            r#"{{
            "inbounds":[{{"tag":"first","listen":{{"address":"127.0.0.1","port":0}},
                "udp":{{"enabled":false}},"protocol":{{"type":"direct",
                "target":"127.0.0.1","port":{}}}}}],
            "route":{{"final":{{"type":"direct"}}}}
        }}"#,
            target_address.port()
        ))
        .unwrap();
        let proxy = Proxy::new(config).unwrap();
        let config = proxy.engine().config();
        proxy
            .protocols
            .start_components(config.clone(), Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .unwrap();
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
        let owner = installed.owner();
        let address = listener.local_addr().unwrap();
        let runtime = crate::runtime::route_runtime::InboundListenerRuntime::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                proxy.tcp_runtime_services(),
            ),
            "first".into(),
            Some(owner.task_access()),
        );
        let bound = crate::protocol_registry::BoundInbound::Tcp(
            zero_platform_tokio::TokioListener::from_controlled(listener).unwrap(),
        );
        owner
            .spawn_driver(operation.execute(runtime, bound, shutdown.subscribe()))
            .unwrap();
        owner.seal_listener_installation();
        let exercise = async {
            let mut rejected = TcpStream::connect(address).await.unwrap();
            assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
            assert!(
                owner.is_running(),
                "ordinary upstream refusal must not stop listener"
            );
            #[cfg(not(target_os = "macos"))]
            let target = target.listen(1).unwrap();
            #[cfg(target_os = "macos")]
            let target = TcpListener::bind(target_address).await.unwrap();
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(b"same-original").await.unwrap();
            let (mut peer, _) = target.accept().await.unwrap();
            let mut wire = [0; 13];
            peer.read_exact(&mut wire).await.unwrap();
            assert_eq!(&wire, b"same-original");
            peer.write_all(&wire).await.unwrap();
            let mut reply = [0; 13];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, wire);
            // Observe real pending original byte I/O before cancellation.
            {
                let mut bytes = [0];
                let read = client.read(&mut bytes);
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            assert!(owner.terminal().is_none());
            shutdown.send_replace(true);
            (client, peer, target)
        };
        let (observed, (mut client, peer, target)) = tokio::join!(owner.observe_driver(), exercise);
        observed.unwrap();
        assert!(owner.terminal().is_none());
        // Driver retirement explicitly closed native IO while the original
        // external observer consumed its children. That is not yet whole-owner
        // terminal confirmation (Scope admission is closed separately below).
        let rebound = TcpListener::bind(address).await.unwrap();
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        // Actual persistent cleanup concurrently consumes the remaining handler
        // and closes the same accepted original only after its true view/join.
        let receipt = proxy
            .inbound_owners
            .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        let terminal = receipt.terminal();
        assert!(terminal.failure().is_none());
        assert!(terminal.task_failures().is_empty());
        assert_eq!(terminal.joined_tasks(), 3);
        assert_eq!(terminal.closed_io_receipts(), 2);
        let failed = terminal.client_failure().unwrap();
        assert_eq!(failed.class(), TaskClass::Auxiliary);
        let original = failed.result().unwrap_err();
        let repeated = terminal.client_failure().unwrap().result().unwrap_err();
        assert!(Arc::ptr_eq(&original, &repeated));
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(original.as_ref());
        let mut native_refused = false;
        while let Some(source) = cause {
            if let Some(io) = source.downcast_ref::<std::io::Error>() {
                native_refused |= io.kind() == std::io::ErrorKind::ConnectionRefused
                    && io.raw_os_error().is_some();
            }
            cause = source.source();
        }
        assert!(
            native_refused,
            "the original native OS source survives the actual handler receipt"
        );
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        drop((client, peer, target));
        assert_eq!(rebound.local_addr().unwrap(), address);
    })
    .await
    .unwrap();
}
