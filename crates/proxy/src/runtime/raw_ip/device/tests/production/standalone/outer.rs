//! Real normal SOCKS underlay, encrypted WG peer and the Source's exact Scope.
use super::*;
#[tokio::test]
async fn ordinary_outer_wireguard_uses_actual_software_scope_and_keeps_physical_callee_separate() {
    tokio::time::timeout(Duration::from_secs(25), exercise_outer())
        .await
        .unwrap();
}
async fn exercise_outer() {
    let outer_config=RuntimeConfig::parse(r#"{"inbounds":[{"tag":"outer-in","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let outer = Proxy::new(outer_config.clone()).unwrap();
    let outer_handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(outer.engine().clone()),
        outer.clone(),
    );
    let outer_running = outer.spawn();
    outer_handle
        .apply_runtime_config_and_wait(outer_config, Duration::from_secs(5))
        .await
        .unwrap();
    let port = outer
        .inbound_owners
        .owner("outer-in")
        .unwrap()
        .original_listener_address
        .get()
        .unwrap()
        .port();
    let (endpoint, peer_task) = peer::echo_peer().await;
    let mut config = serde_json::to_value(configuration(endpoint, false)).unwrap();
    let second_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    config["outbounds"][0]["protocol"]["peers"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "public_key":peer::public(123),"endpoint":second_peer.local_addr().unwrap().to_string(),
            "allowed_ips":["172.16.0.0/12"]
        }));
    config["outbounds"][0]["protocol"]["outer_udp_proxy"] = serde_json::json!("outer");
    config["outbounds"].as_array_mut().unwrap().push(serde_json::json!({"tag":"outer","protocol":{"type":"socks5","server":"127.0.0.1","port":port}}));
    let config = RuntimeConfig::parse(&config.to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let memory = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    assert_eq!(memory.native().occupied.frames, 0);
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    proxy
        .inbound_owners
        .owner("socks")
        .unwrap()
        .wait_ready()
        .await
        .unwrap();
    let origins = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref());
    assert_eq!(origins.len(), 1);
    let origin = &origins[0];
    let logical = origin
        .logical_packet()
        .expect("real software Source, no fake bound UDP");
    assert_eq!(
        logical.drivers().len(),
        2,
        "complete actual peer set, never just the first witness"
    );
    let driver = &logical.drivers()[0];
    assert!(!driver.is_joined());
    assert!(
        origin.closed().is_none(),
        "logical registration never issues a native-close receipt"
    );
    assert_eq!(
        memory.native().occupied.frames,
        4,
        "two original wire/decode pairs, not duplicated proxy readers"
    );
    let execution = proxy.tcp_runtime_services().execution();
    let target =
        DatagramAssociationTarget::resolve_endpoint(&execution, "outer", Some(("127.0.0.1", port)))
            .unwrap();
    let slot = target.current_slot(&execution).unwrap();
    let physical = execution
        .components
        .as_ref()
        .unwrap()
        .original_carrier_owner(slot.instance_id(), 0)
        .expect("same actual SOCKS provider owns its originals");
    assert_ne!(physical.instance(), origin.instance());
    assert!(physical
        .observe()
        .unwrap()
        .slots()
        .iter()
        .any(|slot| slot.native().is_some_and(|native| !native.is_closed())));
    let mut client = tokio::time::timeout_at(end(), connect(socks_port(&proxy), false))
        .await
        .unwrap();
    client.write_all(b"logical-wire").await.unwrap();
    let mut echoed = [0; 12];
    tokio::time::timeout_at(end(), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"logical-wire");
    let control = datagrams(socks_port(&proxy)).await;
    drop(client);
    drop(control);
    assert!(
        physical.closed().is_none(),
        "return of logical users does not close the outer provider"
    );
    assert!(origin.closed().is_none());
    let devices = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(devices.len(), 2);
    let joins = devices[0].task.test_original_witnesses();
    assert!(joins.len() >= 3);
    assert!(joins.iter().all(|witness| witness
        .scope_identity()
        .same_scope(&driver.scope_identity())));
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(logical.drivers().iter().all(|driver| driver
        .try_joined()
        .is_some_and(|joined| driver.matches_joined(&joined) && joined.registration_released())));
    assert!(joins.iter().all(|witness| witness
        .try_joined()
        .is_some_and(|joined| witness.matches_joined(&joined) && joined.registration_released())));
    assert!(
        origin.closed().is_none(),
        "physical TEMP debt is not replaced by logical Scope completion"
    );
    assert!(
        physical.closed().unwrap().matches(&physical),
        "only that actual callee publishes its own native close"
    );
    assert_eq!(memory.native().occupied.frames, 0);
    assert_eq!(memory.native().occupied.bytes, 0);
    outer_running.shutdown().await.unwrap();
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|error| error.is_cancelled()));
}
