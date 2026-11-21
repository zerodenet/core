//! Actual Direct operation: original accepted native/view/handler/host receipts.
use super::*;
use zero_platform_tokio::contract::lifecycle::TaskClass;

#[tokio::test]
async fn actual_fixed_direct_handler_keeps_source_join_and_native_close_separate() {
    let phase = std::sync::atomic::AtomicUsize::new(0);
    let stage = |value| phase.store(value, std::sync::atomic::Ordering::Relaxed);
    let result = timeout(Duration::from_secs(8), async {
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
        let direct = proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .direct(None)
            .unwrap()
            .original_carrier_owner(0)
            .unwrap();
        let prepared = proxy
            .protocols
            .prepare_inbound_bind(config.inbounds[0].clone(), None)
            .unwrap()
            .expect("actual standard Direct recipe");
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
        let (definition, listener, installed) = promote_tcp(lease.finish().unwrap());
        let owner = installed.owner();
        let scope = owner
            .accepted_registrar()
            .downgrade()
            .retirement_observation()
            .unwrap();
        let original_client_receiver = owner.client_failure_receiver().unwrap().clone();
        assert!(original_client_receiver.is_empty());
        let address = listener.local_addr().unwrap();
        let recipe = definition;
        let limit = match recipe.spec().resources[0] {
            ResourceDeclaration::TcpListener {
                accepted: Some(limits),
                ..
            } => limits.max_sockets,
            _ => panic!("actual accepted declaration"),
        };
        let (source, _) = installed
            .build_inbound(recipe, NativeListenerView::Tcp(listener), None)
            .unwrap();
        let runtime = crate::runtime::route_runtime::InboundListenerRuntime::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                proxy.tcp_runtime_services(),
            ),
            "first".into(),
            Some(owner.task_access()),
        )
        .with_inbound_instance(owner.identity());
        owner
            .spawn_driver(crate::runtime::inbound_operation::source::run(
                source,
                runtime,
                shutdown.subscribe(),
                limit,
                "direct",
            ))
            .unwrap();
        owner.seal_listener_installation();
        let exercise = async {
            stage(1); // first connection / actual native refusal
            let mut rejected = TcpStream::connect(address).await.unwrap();
            stage(2); // wait for first accepted original EOF
            assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
            stage(3); // first native EOF has actually arrived
            if !owner.is_running() {
                owner.diagnose_datagram_failures_for_test();
            }
            assert!(
                owner.is_running(),
                "ordinary upstream refusal must not stop listener"
            );
            let (claim, closed) = loop {
                if let Some(receipt) = owner.accepted_closed_for_test() {
                    break receipt;
                }
                tokio::task::yield_now().await;
            };
            assert!(closed.matches(&claim));
            #[cfg(not(target_os = "macos"))]
            let target = target.listen(1).unwrap();
            #[cfg(target_os = "macos")]
            let target = TcpListener::bind(target_address).await.unwrap();
            stage(4); // second TCP connect
            let mut client = TcpStream::connect(address).await.unwrap();
            stage(5); // second TCP established / application write
            client.write_all(b"same-original").await.unwrap();
            stage(6); // second actual upstream accept
            let (mut peer, _) = target.accept().await.unwrap();
            stage(7); // original forward bytes
            let mut wire = [0; 13];
            peer.read_exact(&mut wire).await.unwrap();
            assert_eq!(&wire, b"same-original");
            peer.write_all(&wire).await.unwrap();
            let mut reply = [0; 13];
            stage(8); // original reverse bytes
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
            let active = direct.observe().unwrap();
            let outbound = active
                .slots()
                .iter()
                .find(|original| original.physical_view_live())
                .expect("the same Direct original still owns the live connection")
                .original()
                .clone();
            assert!(outbound.closed().is_none());
            assert!(!scope.is_drained());
            stage(9); // exact pending read observed; driver shutdown/join
            owner.request_stop();
            (client, peer, target, outbound)
        };
        // Production drives both the Source Scope and the Host's retained
        // carrier close continuation while an accepted caller retires.
        let (observed, (mut client, peer, target, outbound)) = proxy
            .protocols
            .drive_component_supervision(async { tokio::join!(owner.observe_driver(), exercise) })
            .await;
        observed.unwrap();
        stage(10); // actual original Source driver result received
        assert!(owner.terminal().is_none());
        // Actual persistent cleanup concurrently consumes the remaining handler
        // and closes the same accepted original only after its true view/join.
        stage(11); // full original Scope / native terminal confirmation
        let receipt = proxy
            .protocols
            .drive_component_supervision(
                proxy
                    .inbound_owners
                    .confirm_shutdown("first", Instant::now() + Duration::from_secs(2)),
            )
            .await
            .unwrap()
            .unwrap();
        let terminal = receipt.terminal();
        assert_eq!(terminal.identity(), owner.identity());
        assert!(terminal.failure().is_none());
        assert!(terminal.task_failures().is_empty());
        assert!(scope.is_drained());
        let closed = outbound
            .closed()
            .expect("actual Direct original close receipt");
        assert!(outbound.matches_closed(&closed) && closed.had_native_acquisition());
        let failed = terminal.client_failure().unwrap();
        assert_eq!(failed.class(), TaskClass::Auxiliary);
        let original = failed.result().unwrap_err();
        let retained_clients = terminal.client_failure_receiver().unwrap();
        assert!(retained_clients.same_receiver(&original_client_receiver));
        let mut all_clients = terminal.client_failures();
        let received = all_clients
            .next()
            .expect("actual paid original client fault");
        assert_eq!(received.id(), failed.id());
        assert!(received.matches_joined(&failed));
        // The live second connection can also report its own stop through
        // the same receiver. It must be distinct from the retained refusal.
        let cancelled = all_clients.next();
        if let Some(cancelled) = &cancelled {
            assert_ne!(cancelled.id(), received.id());
        }
        assert!(all_clients.next().is_none());
        assert_eq!(
            original_client_receiver.len(),
            1 + usize::from(cancelled.is_some())
        );
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
        stage(12);
    })
    .await;
    if result.is_err() {
        eprintln!(
            "actual paid Direct Source timed out at stage {}",
            phase.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
    result.unwrap();
}
