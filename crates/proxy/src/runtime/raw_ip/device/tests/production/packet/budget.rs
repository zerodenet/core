//! A policy update cannot evict an admitted route or strand its idle own view.
use super::*;
use zero_api::CommandService;

#[tokio::test]
async fn ordinary_packet_budget_waits_for_real_route_close_then_replaces_native_source() {
    let (remote, remote_task) = peer::echo_peer().await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = config(remote, socket.local_addr().unwrap());
    let input = config.endpoints[0].inbound().unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let old = proxy.inbound_owners.owner(&input.tag).unwrap();
    old.wait_ready().await.unwrap();
    let old_main = old.task_access().source_main_witness().await.unwrap();
    let original = proxy
        .inbound_owners
        .packet_source_origin(&input)
        .unwrap()
        .unwrap();
    let mut client =
        client::Client::from_original(socket, *old.original_listener_address.get().unwrap());
    client.echo(b"active-route-blocks-policy-eviction").await;
    let mut changed = config.clone();
    changed.runtime.packet_return_max_frames = Some(128);
    assert!(handle
        .apply_runtime_config_and_wait(changed.clone(), Duration::from_secs(5))
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        &old,
        &proxy.inbound_owners.owner(&input.tag).unwrap()
    ));
    assert!(!old_main.is_joined());
    assert!(!original.driver().is_joined());
    assert!(original.closed().is_none());
    client.echo(b"original-still-usable-after-busy").await;
    let routes = proxy
        .engine()
        .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default());
    assert_eq!(routes.total, 1);
    let closed = handle
        .execute_acknowledged(zero_api::CommandRequest::PacketRouteClose(
            zero_api::PacketRouteCloseCommand {
                route_id: routes.routes[0].route_id.clone(),
                expected_core_instance_id: proxy.engine().core_instance_id().into(),
                expected_config_revision: None,
            },
        ))
        .await
        .unwrap();
    assert!(closed.accepted);
    assert_eq!(
        proxy
            .engine()
            .packet_routes_snapshot(&Default::default())
            .total,
        0
    );
    handle
        .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
        .await
        .unwrap();
    let current = proxy.inbound_owners.owner(&input.tag).unwrap();
    current.wait_ready().await.unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    assert!(old_main.is_joined());
    assert!(old_main.try_joined().unwrap().registration_released());
    assert!(original.driver().is_joined());
    assert!(original
        .driver()
        .try_joined()
        .unwrap()
        .registration_released());
    assert!(original.closed().unwrap().matches(original.owner()));
    let socket = client.into_socket();
    let mut client =
        client::Client::from_original(socket, *current.original_listener_address.get().unwrap());
    client.echo(b"new-native-source-after-matching-close").await;
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    remote_task.abort();
    assert!(remote_task.await.is_err_and(|error| error.is_cancelled()));
}
