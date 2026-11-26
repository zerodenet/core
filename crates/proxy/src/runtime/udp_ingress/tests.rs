use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;

use super::UdpIngressRuntime;

#[tokio::test]
async fn unmatched_domain_is_rechecked_against_resolved_ip_rules() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "ip",
                            "values": ["127.0.0.0/8", "::1/128"]
                        },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse routing config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let session = Session::new(
        1,
        Address::Domain("localhost".to_owned()),
        53,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );

    assert_eq!(
        runtime.route_decision(&session).await,
        RouteDecision::Direct
    );
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn back_to_back_udp_targets_on_same_accepted_caller_reuse_source_process_before_scope_ack() {
    use std::num::NonZeroUsize;
    use tokio::{io::AsyncReadExt, net::TcpStream, time::Instant};
    use zero_core::request::Destination;
    use zero_platform_tokio::{
        contract::{
            component::{AcceptedTcpLimits, ResourceDeclaration},
            lifecycle::TaskClass,
            scope::TaskScope,
            CancellationToken, ChannelError, Impact, Reason, Stage,
        },
        AcceptedTaskFailures, ControlledTcpListenerOwner,
    };

    let config = RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, std::time::Duration::from_secs(5))
        .await
        .unwrap();

    let one = NonZeroUsize::MIN;
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
    let declaration = ResourceDeclaration::TcpListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: one,
        accepted: Some(AcceptedTcpLimits {
            max_sockets: one,
            max_tasks_per_socket: one,
            max_resources_per_socket: 0,
        }),
    };
    let mut owner = ControlledTcpListenerOwner::new_with_accepted_graph(
        &declaration,
        one,
        CancellationToken::new(),
        one,
        one,
        &scope.registrar(),
    )
    .unwrap();
    let instance = proxy.protocols.allocate_resource_identity().unwrap();
    let roots = proxy.protocols.runtime_host().original_budgets();
    let failures = AcceptedTaskFailures::prepare(
        instance,
        &scope.registrar(),
        &roots.packet.driver_metadata_view(),
    )
    .unwrap();
    owner.bind_task_failure_receiver(&failures).unwrap();
    owner
        .bind_original_totals(instance, &scope.registrar(), &roots.carrier)
        .unwrap();
    let mut listener = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let receiver = owner.accepted_task_receiver().unwrap();
    scope.enable_registration().unwrap();

    let client_udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_source = client_udp.local_addr().unwrap();
    let (peer, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept_owned()
    );
    let peer = peer.unwrap();
    let (accepted, _) = accepted.unwrap();
    let claim = accepted.claim();
    let runtime_base = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let (handler_id, _) = accepted
        .spawn_handler_bound(TaskClass::Auxiliary, move |mut io, tasks| async move {
            let caller = tasks.caller()?;
            let runtime = runtime_base
                .with_accepted_caller(caller)
                .with_source_addr(Some(udp_source));
            let mut cached = None;
            let mut first = Session::new(
                0,
                Address::Ipv4([203, 0, 113, 1]),
                53,
                Network::Udp,
                ProtocolType::UNKNOWN,
            );
            let mut second = Session::new(
                0,
                Address::Ipv4([203, 0, 113, 2]),
                53,
                Network::Udp,
                ProtocolType::UNKNOWN,
            );
            let d1 = runtime
                .prepare_udp_session_with_cached_process(&mut first, "entry", &mut cached)
                .await
                .map_err(|e| {
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session).with_source(e)
                })?;
            // Without acknowledging helper #1's TaskScope receipt yet, target #2 on
            // the same source_addr must reuse the cached lookup instead of failing
            // with Reason::Capacity ("actual accepted metadata worker budget is full").
            let d2 = runtime
                .prepare_udp_session_with_cached_process(&mut second, "entry", &mut cached)
                .await
                .map_err(|e| {
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session).with_source(e)
                })?;
            assert_eq!(first.process_id, Some(std::process::id()));
            assert_eq!(second.process_id, Some(std::process::id()));
            drop((d1, d2));
            done_tx.send(()).unwrap();
            let _ = io.read_u8().await;
            Ok(())
        })
        .unwrap();

    done_rx.await.unwrap();
    let lookup_receipt = scope.next_receipt().await.unwrap();
    assert_ne!(lookup_receipt.id(), handler_id);
    assert!(receiver
        .observe_joined(lookup_receipt.id())
        .unwrap()
        .result()
        .is_ok());
    scope.acknowledge(&lookup_receipt).unwrap();

    drop(peer);
    let handler_receipt = scope.next_receipt().await.unwrap();
    assert_eq!(handler_receipt.id(), handler_id);
    assert!(receiver
        .observe_joined(handler_id)
        .unwrap()
        .result()
        .is_ok());
    scope.acknowledge(&handler_receipt).unwrap();

    let end = Instant::now() + std::time::Duration::from_secs(2);
    assert!(owner
        .close_accepted_graph(&claim, end)
        .await
        .unwrap()
        .unwrap()
        .matches(&claim));
    assert!(owner.wait_closed(end).await.unwrap().is_complete());
    scope.begin_shutdown();
    assert!(scope.is_drained());
    running.shutdown().await.unwrap();
}
